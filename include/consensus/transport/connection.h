#pragma once
#include "deadline.h"
#include "node_hello.h"
#include "node_pins.h"
#include <asio/ip/tcp.hpp>
#include <asio/ssl/context.hpp>
#include <climits>
#include <functional>
#include <memory>
#include <netinet/tcp.h>
#include <string>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <task_asio.hh>
#ifdef __linux__
#include <linux/sockios.h>
#endif

// One host connection (docs/mechanisms/host-connection.md): OpenSSL driven on the non-blocking
// socket (SSL_set_fd, readiness through await_asio on async_wait), with SSL_MODE_RELEASE_BUFFERS
// so an idle end holds no record buffers. Frames are a 4-byte little-endian length and the
// bytes; a length of 0 is a keepalive. Everything runs on the connection's loop. OpenSSL writes
// with plain write(): where SO_NOSIGPIPE is missing (Linux), the host must ignore SIGPIPE.
namespace kronuz::consensus::transport {
struct ConnectionOptions {
	std::chrono::milliseconds handshake{5000};  // TLS and the hellos
	std::chrono::milliseconds keepalive{1000};  // a connection that sent nothing for this long sends a keepalive
	std::chrono::milliseconds dead_after{5000}; // nothing received, or no write progress, for this long ends it
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
#ifdef SO_NOSIGPIPE
		::setsockopt(socket_.native_handle(), SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof one);
#endif
		ssl_.reset(SSL_new(tls_->native_handle()));
		if (!ssl_ || SSL_set_fd(ssl_.get(), int(socket_.native_handle())) != 1)
			throw std::runtime_error("connection: SSL_new");
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
	const asio::ip::tcp::socket &socket() const noexcept { return socket_; }

	// After each read's frames went to the handler; when the queue is empty and the writer would
	// wait, so a sender keeps its own queues and hands over a little at a time. Set before run().
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
		co_await flush(deadline);
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
	// Writes `sending_` from `sent_` to its end, then releases it; with `progress`, returns early
	// when a write made progress and the next must wait, so the caller's stuck deadline restarts.
	task::task<void> flush(task::cancel_token deadline, bool progress = false) {
		for (bool wrote = false; sent_ != sending_.size();) {
			open_or_throw("write");
			const auto rest = std::min<std::size_t>(sending_.size() - sent_, INT_MAX);
			const int r = SSL_write(ssl_.get(), sending_.data() + sent_, int(rest));
			if (r > 0) {
				sent_ += std::size_t(r);
				wrote = true;
				continue;
			}
			const int error = SSL_get_error(ssl_.get(), r);
			if (progress && wrote && (error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE))
				co_return;
			co_await want(error, deadline, "write");
		}
		std::string().swap(sending_);  // an idle connection holds no send buffer
		sent_ = 0;
	}
	// The socket's send queue: bytes written and not yet acknowledged by the peer; 0 when the
	// platform cannot tell. It drains while the peer reads, however coarsely writes wake.
	std::size_t unacknowledged() noexcept {
		int n = 0;
#if defined(SIOCOUTQ)
		if (::ioctl(socket_.native_handle(), SIOCOUTQ, &n) != 0)
			return 0;
#elif defined(SO_NWRITE)
		socklen_t size = sizeof n;
		if (::getsockopt(socket_.native_handle(), SOL_SOCKET, SO_NWRITE, &n, &size) != 0)
			return 0;
#elif defined(FIONWRITE)
		if (::ioctl(socket_.native_handle(), FIONWRITE, &n) != 0)
			return 0;
#endif
		return n > 0 ? std::size_t(n) : 0;
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
				deliver();
				if (on_read)
					on_read();
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
				if (!sending_.empty()) {  // stuck: for dead_after, no write progress and the socket's queue never drained
					const auto queued = unacknowledged();
					late_deadline stuck(loop_, options_.dead_after);
					try {
						co_await flush(stuck.token(), true);
					} catch (...) {
						if (stuck.state() != late_deadline::outcome::expired || end_ != ConnectionEnd::open ||
							unacknowledged() >= queued)
							throw;
					}
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
				late_deadline stuck(loop_, options_.dead_after);
				for (int r; (r = SSL_shutdown(ssl_.get())) < 0;)
					co_await want(SSL_get_error(ssl_.get(), r), stuck.token(), "close_notify");
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
	std::uint64_t keepalives_sent_ = 0, keepalives_received_ = 0;
	ConnectionEnd end_ = ConnectionEnd::open;
	// dead_: the reader failed at its dead-peer deadline, nothing arrived within dead_after.
	bool dialer_, closing_ = false, reading_done_ = false, writing_done_ = false, dead_ = false;
};
} // namespace kronuz::consensus::transport
