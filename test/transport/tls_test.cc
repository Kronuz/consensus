#include "async_pipe.h"
#include "consensus/transport/session.h"
#include <cassert>
#include <csignal>
#include <cstdio>
#include <filesystem>
#include <future>
#include <iostream>
#include <openssl/pem.h>
#include <string>
#include <vector>

using namespace kronuz::consensus::transport;
using namespace std::chrono_literals;
using Tcp = asio::ip::tcp;

Fingerprint pin(const std::filesystem::path &path) {
	std::unique_ptr<BIO, decltype(&BIO_free)> input(BIO_new_file(path.c_str(), "r"), BIO_free);
	assert(input);
	std::unique_ptr<X509, decltype(&X509_free)> certificate(
		PEM_read_bio_X509(input.get(), nullptr, nullptr, nullptr), X509_free);
	assert(certificate);
	Fingerprint result{};
	unsigned size = 0;
	assert(X509_digest(certificate.get(), EVP_sha256(), result.data(), &size) == 1);
	assert(size == result.size());
	return result;
}

Hello hello(unsigned node) {
	Hello result;
	result.node = node;
	result.cluster[0] = 'c';
	result.group[0] = 'g';
	result.epoch[0] = 'e';
	result.connection = fresh_connection_identity();
	return result;
}

template <class Socket> void exchange(const std::filesystem::path &directory, unsigned failure) {
	asio::io_context io;
	auto server_context = std::make_shared<asio::ssl::context>(asio::ssl::context::tls);
	auto client_context = std::make_shared<asio::ssl::context>(asio::ssl::context::tls);
	reactor::tls::configure(
		*server_context,
		{(directory / "one.pem").string(), (directory / "one.key").string(), (directory / "ca.pem").string()},
		reactor::tls::Role::Peer);
	reactor::tls::configure(*client_context,
				  {failure == 2 ? "" : (directory / "two.pem").string(),
				   failure == 2 ? "" : (directory / "two.key").string(),
				   (directory / (failure == 1 ? "rogue.pem" : "ca.pem")).string()},
				  failure == 2 ? reactor::tls::Role::Client : reactor::tls::Role::Peer);
	auto sockets = [&]() -> std::pair<Socket, Socket> {
		if constexpr (std::is_same_v<Socket, AsyncPipe>)
			return AsyncPipe::pair(io);
		else {
			Tcp::acceptor acceptor(io, {asio::ip::address_v4::loopback(), 0});
			Tcp::socket client(io);
			client.connect(acceptor.local_endpoint());
			Tcp::socket server(io);
			acceptor.accept(server);
			for (auto *socket : {&server, &client}) {
				socket->set_option(asio::socket_base::send_buffer_size(4096));
				socket->set_option(asio::socket_base::receive_buffer_size(4096));
			}
			return {std::move(server), std::move(client)};
		}
	}();
	const auto capacity = failure >= 8 ? 1024 * 1024 : 64;
	const auto maximum_frame = failure >= 8 ? 512 * 1024 : 32;
	auto incoming = std::make_shared<ByteBudget>(capacity);
	auto outgoing = std::make_shared<ByteBudget>(capacity);
	BasicSession<Socket> server(std::move(sockets.first), server_context, incoming, maximum_frame, 200ms);
	BasicSession<Socket> client(std::move(sockets.second), client_context, incoming, maximum_frame, 200ms);
	asio::steady_timer closer(io);
	asio::cancellation_signal cancellation;
	std::map<kronuz::consensus::NodeId, Fingerprint> pins{{1, pin(directory / "one.pem")},
														   {2, pin(directory / "two.pem")}};
	if (failure == 3)
		pins.erase(2);
	if (failure == 4)
		pins.emplace(3, pins.at(2));
	auto first = hello(1), second = hello(2);
	if (failure == 5)
		second.profile[0] = 1;
	auto serve = [&]() -> asio::awaitable<void> {
		auto remote = co_await server.authenticate(true, first, pins);
		assert(remote.node == 2);
		if (failure >= 8 && failure != 10 && failure != 12) {
			asio::steady_timer delay(io, 300ms);
			co_await delay.async_wait(asio::use_awaitable);
			co_return;
		}
		if (failure == 10 || failure == 12) {
			closer.expires_after(50ms);
			closer.async_wait([&](asio::error_code error) {
				if (!error) {
					if (failure == 12)
						cancellation.emit(asio::cancellation_type::terminal);
					else
						server.close();
				}
			});
		}
		auto frame = co_await server.receive();
		assert(frame.bytes() == std::string("a\0b", 3));
		assert(incoming->used() == 3);
		Frame response(outgoing, 3);
		response.assign(frame.bytes());
		co_await server.send(std::move(response));
	};
	auto publish = [&]() -> asio::awaitable<void> {
		if (failure == 6) {
			asio::steady_timer delay(io, 300ms);
			co_await delay.async_wait(asio::use_awaitable);
			co_return;
		}
		auto remote = co_await client.authenticate(false, second, pins);
		assert(remote.node == 1);
		if (failure == 7 || failure == 10 || failure == 12) {
			asio::steady_timer delay(io, 300ms);
			co_await delay.async_wait(asio::use_awaitable);
			client.close();
			co_return;
		}
		if (failure == 8 || failure == 9 || failure == 11) {
			Frame frame(outgoing, 256 * 1024);
			frame.assign(std::string(frame.bytes().size(), 'x'));
			if (failure == 9 || failure == 11) {
				closer.expires_after(50ms);
				closer.async_wait([&](asio::error_code error) {
					if (!error) {
						if (failure == 11)
							cancellation.emit(asio::cancellation_type::terminal);
						else
							client.close();
					}
				});
			}
			co_await client.send(std::move(frame));
			co_return;
		}
		Frame frame(outgoing, 3);
		frame.assign(std::string("a\0b", 3));
		co_await client.send(std::move(frame));
		auto response = co_await client.receive();
		assert(response.bytes() == std::string("a\0b", 3));
	};
	auto a = asio::co_spawn(
		io, serve(),
		asio::bind_cancellation_slot(failure == 12 ? cancellation.slot() : asio::cancellation_slot{},
									 asio::use_future));
	auto b = asio::co_spawn(
		io, publish(),
		asio::bind_cancellation_slot(failure == 11 ? cancellation.slot() : asio::cancellation_slot{},
									 asio::use_future));
	io.run();
	unsigned errors = 0;
	bool canceled = false;
	std::vector<std::string> what;
	for (auto *future : {&a, &b}) {
		try {
			future->get();
		} catch (const asio::system_error &error) {
			canceled = canceled || error.code() == asio::error::operation_aborted;
			++errors;
			what.push_back(error.what());
		} catch (const std::exception &error) {
			++errors;
			what.push_back(error.what());
		}
	}
	if (failure ? errors == 0 : errors != 0) {
		for (const auto &w : what) { std::fprintf(stderr, "failure mode %d: %s\n", int(failure), w.c_str()); }
	}
	assert(failure ? errors > 0 : errors == 0);
	if (failure == 11 || failure == 12)
		assert(canceled);
	assert(incoming->used() == 0 && outgoing->used() == 0);
}

void malformed_frames(const std::filesystem::path &directory, unsigned mode) {
	asio::io_context io;
	auto server_context = std::make_shared<asio::ssl::context>(asio::ssl::context::tls);
	auto client_context = std::make_shared<asio::ssl::context>(asio::ssl::context::tls);
	reactor::tls::configure(
		*server_context,
		{(directory / "one.pem").string(), (directory / "one.key").string(), (directory / "ca.pem").string()},
		reactor::tls::Role::Peer);
	reactor::tls::configure(
		*client_context,
		{(directory / "two.pem").string(), (directory / "two.key").string(), (directory / "ca.pem").string()},
		reactor::tls::Role::Peer);
	auto pipes = AsyncPipe::pair(io);
	auto budget = std::make_shared<ByteBudget>(mode == 4 ? 0 : 64);
	BasicSession<AsyncPipe> server(std::move(pipes.first), server_context, budget, 32, 200ms);
	asio::ssl::stream<AsyncPipe> client(std::move(pipes.second), *client_context);
	std::map<kronuz::consensus::NodeId, Fingerprint> pins{{1, pin(directory / "one.pem")},
														   {2, pin(directory / "two.pem")}};
	auto first = hello(1), second = hello(2);
	bool expired = false;
	auto serve = [&]() -> asio::awaitable<void> {
		(void)co_await server.authenticate(true, first, pins);
		try {
			(void)co_await server.receive();
			assert(false);
		} catch (const asio::system_error &error) {
			expired = error.code() == asio::error::timed_out;
			if (!expired)
				throw;
		}
	};
	auto attack = [&]() -> asio::awaitable<void> {
		co_await client.async_handshake(asio::ssl::stream_base::client, asio::use_awaitable);
		auto bytes = encode_hello(second);
		co_await asio::async_write(client, asio::buffer(bytes), asio::use_awaitable);
		std::array<char, hello_size> reply{};
		co_await asio::async_read(client, asio::buffer(reply), asio::use_awaitable);
		std::array<unsigned char, 4> header{32, 0, 0, 0};
		if (mode == 0)
			header = {255, 255, 255, 255};
		if (mode == 1)
			header = {};
		if (mode == 2) {
			co_await asio::async_write(client, asio::buffer(header.data(), 1), asio::use_awaitable);
			asio::steady_timer delay(io, 100ms);
			co_await delay.async_wait(asio::use_awaitable);
			co_await asio::async_write(client, asio::buffer(header.data() + 1, 3), asio::use_awaitable);
		} else
			co_await asio::async_write(client, asio::buffer(header), asio::use_awaitable);
		if (mode == 2 || mode == 3) {
			const char byte = 'x';
			co_await asio::async_write(client, asio::buffer(&byte, 1), asio::use_awaitable);
			asio::steady_timer delay(io, 300ms);
			co_await delay.async_wait(asio::use_awaitable);
		}
	};
	auto a = asio::co_spawn(io, serve(), asio::use_future);
	auto b = asio::co_spawn(io, attack(), asio::use_future);
	io.run();
	bool rejected = false;
	try {
		a.get();
	} catch (const std::invalid_argument &) {
		rejected = true;
	} catch (const std::runtime_error &) {
		rejected = true;
	}
	try {
		b.get();
	} catch (const asio::system_error &) {
	}
	assert(mode == 2 || mode == 3 ? expired : rejected);
	assert(budget->used() == 0);
}

int main(int argc, char **argv) {
	std::signal(SIGPIPE, SIG_IGN);
	assert(argc == 2 || argc == 3);
	for (unsigned failure = 0; failure != 13; ++failure) {
		if (argc == 3)
			exchange<AsyncPipe>(argv[1], failure);
		else
			exchange<Tcp::socket>(argv[1], failure);
	}
	if (argc == 3)
		for (unsigned mode = 0; mode != 5; ++mode)
			malformed_frames(argv[1], mode);
	auto budget = std::make_shared<ByteBudget>(3);
	{
		Frame held(budget, 3);
		try {
			Frame excess(budget, 1);
			assert(false);
		} catch (const std::runtime_error &) {
		}
		Frame moved(std::move(held));
		assert(budget->used() == 3);
	}
	assert(budget->used() == 0);
	std::cout << "TLS: mutual authentication, voter pins, profile binding and owned frames passed\n";
}
