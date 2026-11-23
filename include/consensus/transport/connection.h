#pragma once
#include "deadline.h"
#include "node_hello.h"
#include "node_pins.h"
#include <asio/ip/tcp.hpp>
#include <asio/ssl/context.hpp>
#include <climits>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <netinet/tcp.h>
#include <optional>
#include <string>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <task_asio.hh>
#ifdef __linux__
#include <linux/sockios.h>
#endif

// One host connection (docs/mechanisms/host-connection.md): OpenSSL driven on the non-blocking
// socket (readiness through await_asio on async_wait), with SSL_MODE_RELEASE_BUFFERS so an idle
// end holds no record buffers. Frames are a 4-byte little-endian length and the bytes; a length
// of 0 is a keepalive. Everything runs on the connection's loop. No write raises SIGPIPE.
namespace kronuz::consensus::transport {
namespace connection_detail {
// The socket's write side for OpenSSL: send() with MSG_NOSIGNAL, so a write to a link the peer
// closed fails with EPIPE instead of raising SIGPIPE (OpenSSL's socket BIO uses write()).
inline BIO *socket_writes(int fd) {
	static BIO_METHOD *const method = [] {
		BIO_METHOD *m = BIO_meth_new(BIO_get_new_index() | BIO_TYPE_SOURCE_SINK, "consensus socket writes");
		if (!m ||
			BIO_meth_set_write(m, [](BIO *b, const char *data, int n) -> int {
				const auto r = ::send(int(reinterpret_cast<std::intptr_t>(BIO_get_data(b))), data, std::size_t(n), MSG_NOSIGNAL);
				BIO_clear_retry_flags(b);
				if (r <= 0 && BIO_sock_should_retry(int(r)))
					BIO_set_retry_write(b);
				return int(r);
			}) != 1 ||
			BIO_meth_set_ctrl(m, [](BIO *, int command, long, void *) -> long { return command == BIO_CTRL_FLUSH; }) != 1)
			throw std::runtime_error("connection: BIO_meth_new");
		return m;
	}();
	BIO *bio = BIO_new(method);
	if (bio) {
		BIO_set_data(bio, reinterpret_cast<void *>(std::intptr_t(fd)));
		BIO_set_init(bio, 1);
	}
	return bio;
}
} // namespace connection_detail

struct ConnectionOptions {
	std::chrono::milliseconds handshake{5000};  // TLS and the hellos
	std::chrono::milliseconds keepalive{1000};  // a connection that sent nothing for this long sends a keepalive
	std::chrono::milliseconds dead_after{5000}; // nothing received, or a write wait with no drain, this long ends it
	std::size_t maximum_frame = 4 * 1024 * 1024;
};
// How run() ended: both ends sent close_notify, a socket, TLS or framing failure, or a peer
// silent for dead_after.
enum class ConnectionEnd : std::uint8_t { open, closed, failed, dead };

class Connection {
  public:
	// Each frame, synchronously on the loop, viewing the connection's own buffer until it returns.
	using Handler = std::function<void(std::string_view)>;

	Connection(task::scheduler_ref loop, asio::ip::tcp::socket socket, std::shared_ptr<asio::ssl::context> tls,
			   bool dialer, ConnectionOptions options = {})
		: loop_(loop), socket_(std::move(socket)), tls_(std::move(tls)), options_(options), dialer_(dialer) {
		if (!tls_ || options_.maximum_frame > UINT32_MAX || options_.keepalive <= options_.keepalive.zero() ||
			options_.dead_after <= options_.dead_after.zero() || options_.handshake <= options_.handshake.zero())
			throw std::invalid_argument("connection options");
		socket_.non_blocking(true);
		const int one = 1;
		::setsockopt(socket_.native_handle(), IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
		const int fd = int(socket_.native_handle());
		ssl_.reset(SSL_new(tls_->native_handle()));
		BIO *writes = ssl_ ? connection_detail::socket_writes(fd) : nullptr;
		if (!writes)
			throw std::runtime_error("connection: SSL_new");
		SSL_set0_wbio(ssl_.get(), writes);  // reads stay OpenSSL's socket BIO
		if (SSL_set_rfd(ssl_.get(), fd) != 1)
			throw std::runtime_error("connection: SSL_set_rfd");
		SSL_set_mode(ssl_.get(),
					 SSL_MODE_RELEASE_BUFFERS | SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
		dialer_ ? SSL_set_connect_state(ssl_.get()) : SSL_set_accept_state(ssl_.get());
	}
	Connection(const Connection &) = delete;
	Connection &operator=(const Connection &) = delete;

	bool dialer() const noexcept { return dialer_; }
	bool draining() const noexcept { return closing_; }
	ConnectionEnd end() const noexcept { return end_; }
	std::size_t queued() const noexcept { return out_.size() + sending_.size() - sent_; }
	std::uint64_t keepalives_sent() const noexcept { return keepalives_sent_; }
	std::uint64_t keepalives_received() const noexcept { return keepalives_received_; }
	// Write waits that reached dead_after with the send queue drained: a slow reader, kept.
	std::uint64_t drained_expiries() const noexcept { return drained_expiries_; }
	const asio::ip::tcp::socket &socket() const noexcept { return socket_; }

	// After each read's frames went to the handler, also a read that fails after handing some
	// over; when the queue is empty and the writer would wait, so a sender keeps its own queues
	// and hands over a little at a time. Set before run().
	std::function<void()> on_read, on_drained;

	// Moves the connection to another loop between its handshake and its hellos; nothing waits.
	void rehome(task::scheduler_ref loop, const asio::any_io_executor &executor) {
		const auto protocol = socket_.local_endpoint().protocol();
		socket_ = asio::ip::tcp::socket(executor, protocol, socket_.release());
		loop_ = loop;
	}

	// The TLS handshake, then the peer's node by its pin (`expected` when nonzero). Throws, and
	// the connection is unusable, when either fails or `deadline` is requested.
	task::task<kronuz::consensus::NodeId> handshake(const NodePins &pins, kronuz::consensus::NodeId expected,
													task::cancel_token deadline) {
		for (;;) {
			open_or_throw("TLS handshake");
			const int r = SSL_do_handshake(ssl_.get());
			if (r == 1)
				break;
			co_await want(SSL_get_error(ssl_.get(), r), deadline, "TLS handshake");
		}
		co_return pins.authenticate(ssl_.get(), expected);
	}
	task::task<void> write_hello(const NodeHello &hello, task::cancel_token deadline) {
		sending_ = encode_node_hello(hello);
		co_await flush(std::move(deadline));
	}
	task::task<NodeHello> read_hello(task::cancel_token deadline) {
		while (in_.size() < node_hello_size)
			if (!co_await fill(deadline))
				throw std::runtime_error("connection: closed before its hello");
		auto hello = decode_node_hello(std::string_view(in_).substr(0, node_hello_size));
		in_.erase(0, node_hello_size);
		co_return hello;
	}

	// Runs the connection once its hellos are exchanged: frames to `handler`, queued frames out,
	// a keepalive while idle, until it ends; destroy it after this returns.
	task::task<ConnectionEnd> run(Handler handler) {
		handler_ = std::move(handler);
		co_await task::when_all(reader(), writer());
		asio::error_code ignored;
		socket_.close(ignored);
		co_return end_;
	}

	// Queues a frame, sent in order after every frame queued before it.
	void send(std::string_view frame) {
		if (closing_ || end_ != ConnectionEnd::open)
			throw std::logic_error("connection: send after close");
		if (frame.empty() || frame.size() > options_.maximum_frame)
			throw std::invalid_argument("connection: frame size");
		put_length(frame.size());
		out_.append(frame);
		wake_writer();
	}
	// Graceful: the frames queued so far, then close_notify; run() ends when the peer's arrives.
	void close() noexcept {
		if (closing_)
			return;
		closing_ = true;
		wake_writer();
	}
	// Now: the socket shuts down and run() ends failed; the fd closes when run() returns.
	void abort() noexcept { fail(ConnectionEnd::failed); }

  private:
	struct ssl_free {
		void operator()(SSL *ssl) const noexcept { SSL_free(ssl); }
	};
	void put_length(std::size_t n) {
		for (int i = 0; i != 4; ++i)
			out_.push_back(char(n >> (8 * i)));
	}
	void wake_writer() noexcept {
		if (writer_wake_)
			writer_wake_->wake();
	}
	// Ends the connection once: the fd stays open until run() returns, so its number cannot be
	// reused while the reader or writer holds it, and nothing touches it after (open_or_throw).
	void fail(ConnectionEnd why) noexcept {
		closing_ = true;
		if (end_ == ConnectionEnd::open) {
			end_ = why;
			asio::error_code ignored;
			::shutdown(socket_.native_handle(), SHUT_RDWR);
			socket_.cancel(ignored);
		}
		wake_writer();
	}
	void open_or_throw(const char *what) const {
		if (end_ != ConnectionEnd::open)
			throw std::runtime_error(std::string("connection: ") + what + " after the end");
	}
	// Waits for what SSL_get_error asked for: readable or writable, or throws.
	task::task<void> want(int error, const task::cancel_token &deadline, const char *what) {
		using wait = asio::socket_base::wait_type;
		if (error != SSL_ERROR_WANT_READ && error != SSL_ERROR_WANT_WRITE) {
			ERR_clear_error();
			throw std::runtime_error(std::string("connection: ") + what + " failed");
		}
		const auto type = error == SSL_ERROR_WANT_READ ? wait::wait_read : wait::wait_write;
		const auto ec = co_await task::await_asio<void(asio::error_code)>(
			[this, type](auto h) { socket_.async_wait(type, std::move(h)); }, deadline);
		if (deadline.requested())
			throw std::runtime_error(std::string("connection: ") + what + " deadline");
		if (ec)
			throw std::system_error(ec);
		open_or_throw(what);
	}
	// One SSL_read into the buffer: false at the peer's close_notify. The buffer grows only by
	// what arrived, so a connection waiting to read holds none.
	task::task<bool> fill(task::cancel_token deadline) {
		constexpr std::size_t chunk = 16 * 1024;
		for (;;) {
			open_or_throw("read");
			const auto had = in_.size();
			in_.resize(had + chunk);
			const int r = SSL_read(ssl_.get(), in_.data() + had, int(chunk));
			in_.resize(had + std::size_t(std::max(r, 0)));
			if (r > 0)
				co_return true;
			if (in_.empty())
				std::string().swap(in_);
			const int error = SSL_get_error(ssl_.get(), r);
			if (error == SSL_ERROR_ZERO_RETURN)
				co_return false;
			co_await want(error, deadline, "read");
		}
	}
	// Writes `sending_` from `sent_` to its end, then releases it: each wait under `deadline` (the
	// hellos), or without one under the stuck rule (want_written).
	task::task<void> flush(std::optional<task::cancel_token> deadline = std::nullopt) {
		while (sent_ != sending_.size()) {
			open_or_throw("write");
			const auto rest = std::min<std::size_t>(sending_.size() - sent_, INT_MAX);
			const int r = SSL_write(ssl_.get(), sending_.data() + sent_, int(rest));
			if (r > 0) {
				sent_ += std::size_t(r);
				continue;
			}
			const int error = SSL_get_error(ssl_.get(), r);
			if (deadline)
				co_await want(error, *deadline, "write");
			else
				co_await want_written(error, "write");
		}
		std::string().swap(sending_);  // an idle connection holds no send buffer
		sent_ = 0;
	}
	// The stuck rule, for every wait of the writer: a wait that lasts dead_after ends the
	// connection unless the socket's send queue drained during it. The queue is sampled after
	// this end's own writes, so only the peer's acknowledgments shrink it, however coarsely the
	// kernel wakes the writer; after a drained expiry the caller retries its write.
	task::task<void> want_written(int error, const char *what) {
		const auto before = unacknowledged();
		late_deadline stuck(loop_, options_.dead_after);
		try {
			co_await want(error, stuck.token(), what);
		} catch (...) {
			if (stuck.state() != late_deadline::outcome::expired || end_ != ConnectionEnd::open || !drained(before))
				throw;
			++drained_expiries_;
		}
	}
	// The socket's send queue: bytes written and not yet acknowledged by the peer; none when the
	// platform cannot tell or the call fails.
	std::optional<std::size_t> unacknowledged() noexcept {
		int n = 0;
#if defined(SIOCOUTQ)
		if (::ioctl(socket_.native_handle(), SIOCOUTQ, &n) != 0)
			return std::nullopt;
#elif defined(SO_NWRITE)
		socklen_t size = sizeof n;
		if (::getsockopt(socket_.native_handle(), SOL_SOCKET, SO_NWRITE, &n, &size) != 0)
			return std::nullopt;
#elif defined(FIONWRITE)
		if (::ioctl(socket_.native_handle(), FIONWRITE, &n) != 0)
			return std::nullopt;
#else
		return std::nullopt;
#endif
		return std::size_t(std::max(n, 0));
	}
	// Whether the queue shrank since `before`; never when either reading is missing.
	bool drained(std::optional<std::size_t> before) noexcept {
		const auto now = unacknowledged();
		return before && now && *now < *before;
	}
	// Hands every whole frame in the buffer to the handler, none after the connection ends;
	// throws on an oversized one.
	void deliver() {
		std::size_t at = 0;
		while (end_ == ConnectionEnd::open && in_.size() - at >= 4) {
			std::size_t n = 0;
			for (int i = 0; i != 4; ++i)
				n |= std::size_t(static_cast<unsigned char>(in_[at + i])) << (8 * i);
			if (n > options_.maximum_frame)
				throw std::runtime_error("connection: frame over its bound");
			if (in_.size() - at - 4 < n)
				break;
			if (n == 0)
				++keepalives_received_;
			else if (handler_)
				handler_(std::string_view(in_).substr(at + 4, n));
			at += 4 + n;
		}
		in_.erase(0, at);
		if (in_.empty())
			std::string().swap(in_);  // an idle connection holds no frame buffer
	}
	task::task<void> reader() {
		try {
			for (bool more = true; more;) {
				std::exception_ptr failed;
				try {
					deliver();
				} catch (...) {
					failed = std::current_exception();
				}
				if (on_read)
					on_read();  // the frames this read handed over, even when it then failed
				if (failed)
					std::rethrow_exception(failed);
				late_deadline dead(loop_, options_.dead_after);
				try {
					more = co_await fill(dead.token());
				} catch (...) {
					dead_ = dead.state() == late_deadline::outcome::expired;
					throw;
				}
				co_await task::yield();  // one read per loop turn: a steady inflow starves nothing
			}
			close();  // the peer's close_notify: answer with ours once the queue is out
		} catch (...) {
			fail(dead_ ? ConnectionEnd::dead : ConnectionEnd::failed);
		}
		reading_done_ = true;
		if (end_ == ConnectionEnd::open && writing_done_)
			end_ = ConnectionEnd::closed;
	}
	task::task<void> writer() {
		try {
			for (;;) {
				if (sending_.empty()) {
					if (out_.empty() && on_drained && !closing_)
						on_drained();
					sending_.swap(out_);
				}
				if (!sending_.empty()) {
					co_await flush();
					continue;
				}
				if (closing_)
					break;
				late_deadline idle(loop_, options_.keepalive, false);  // a late keepalive goes at once
				writer_wake_ = &idle;
				const auto woke = co_await idle.wait();
				writer_wake_ = nullptr;
				if (woke == late_deadline::outcome::expired && out_.empty() && !closing_) {
					put_length(0);
					++keepalives_sent_;
				}
			}
			if (end_ == ConnectionEnd::open) {
				for (int r; (r = SSL_shutdown(ssl_.get())) < 0;)
					co_await want_written(SSL_get_error(ssl_.get(), r), "close_notify");
				::shutdown(socket_.native_handle(), SHUT_WR);
			}
		} catch (...) {
			writer_wake_ = nullptr;
			fail(ConnectionEnd::failed);
		}
		writing_done_ = true;
		if (end_ == ConnectionEnd::open && reading_done_)
			end_ = ConnectionEnd::closed;
	}
	task::scheduler_ref loop_;
	asio::ip::tcp::socket socket_;
	std::shared_ptr<asio::ssl::context> tls_;
	std::unique_ptr<SSL, ssl_free> ssl_;
	ConnectionOptions options_;
	Handler handler_;
	std::string in_, out_, sending_;
	std::size_t sent_ = 0;  // what of sending_ is written
	late_deadline *writer_wake_ = nullptr;
	std::uint64_t keepalives_sent_ = 0, keepalives_received_ = 0, drained_expiries_ = 0;
	ConnectionEnd end_ = ConnectionEnd::open;
	// dead_: the reader failed at its dead-peer deadline, nothing arrived within dead_after.
	bool dialer_, closing_ = false, reading_done_ = false, writing_done_ = false, dead_ = false;
};
} // namespace kronuz::consensus::transport
