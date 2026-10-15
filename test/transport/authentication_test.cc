#include "consensus/transport/tls.h"
#include <cassert>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <openssl/pem.h>

using namespace kronuz::consensus::transport;
using Ssl = std::unique_ptr<SSL, decltype(&SSL_free)>;

Fingerprint certificate_pin(const std::filesystem::path &path) {
	std::unique_ptr<BIO, decltype(&BIO_free)> input(BIO_new_file(path.c_str(), "r"), BIO_free);
	assert(input);
	std::unique_ptr<X509, decltype(&X509_free)> leaf(
		PEM_read_bio_X509(input.get(), nullptr, nullptr, nullptr), X509_free);
	assert(leaf);
	Fingerprint result{};
	unsigned size = 0;
	assert(X509_digest(leaf.get(), EVP_sha256(), result.data(), &size) == 1 && size == result.size());
	return result;
}

// Real TLS records over an OpenSSL BIO pair. This verifies cryptographic
// admission without requiring an operating-system networking permission.
bool handshake(SSL *server, SSL *client) {
	BIO *first = nullptr, *second = nullptr;
	assert(BIO_new_bio_pair(&first, 0, &second, 0) == 1);
	SSL_set_bio(server, first, first);
	SSL_set_bio(client, second, second);
	SSL_set_accept_state(server);
	SSL_set_connect_state(client);
	for (unsigned turn = 0; turn != 100; ++turn) {
		for (auto *ssl : {client, server}) {
			if (SSL_is_init_finished(ssl))
				continue;
			auto result = SSL_do_handshake(ssl);
			if (result != 1) {
				auto error = SSL_get_error(ssl, result);
				if (error != SSL_ERROR_WANT_READ && error != SSL_ERROR_WANT_WRITE) {
					char reason[256];
					ERR_error_string_n(ERR_peek_last_error(), reason, sizeof reason);
					std::fprintf(stderr, "handshake stopped (%s side): %s\n", ssl == server ? "server" : "client", reason);
					ERR_clear_error();
					return false;
				}
			}
		}
		if (SSL_is_init_finished(server) && SSL_is_init_finished(client))
			return true;
	}
	assert(false && "TLS handshake failed to make progress");
	return false;
}

int main(int argc, char **argv) {
	assert(argc == 2);
	const std::filesystem::path directory(argv[1]);
	auto options = [&](std::string node, std::string ca = "ca") {
		return reactor::tls::Options{(directory / (node + ".pem")).string(), (directory / (node + ".key")).string(),
						  (directory / (ca + ".pem")).string()};
	};
	std::map<kronuz::consensus::NodeId, Fingerprint> pins{{1, certificate_pin(directory / "one.pem")},
														   {2, certificate_pin(directory / "two.pem")}};
	for (unsigned mode = 0; mode != 5; ++mode) {
		asio::ssl::context server_context(asio::ssl::context::tls), client_context(asio::ssl::context::tls);
		reactor::tls::configure(server_context, options("one"), mode >= 3 ? reactor::tls::Role::Server : reactor::tls::Role::Peer);
		if (mode == 2 || mode >= 3) {
			reactor::tls::configure(client_context, {"", "", (directory / "ca.pem").string()},
						  reactor::tls::Role::Client);
		} else {
			reactor::tls::configure(client_context, options("two", mode == 1 ? "rogue" : "ca"), reactor::tls::Role::Peer);
		}
		Ssl server(SSL_new(server_context.native_handle()), SSL_free);
		Ssl client(SSL_new(client_context.native_handle()), SSL_free);
		assert(server && client);
		if (mode >= 3)
			reactor::tls::bind_hostname(client.get(), mode == 3 ? "localhost" : "wrong.invalid");
		const bool accepted = handshake(server.get(), client.get());
		assert(accepted == (mode == 0 || mode == 3));
		if (mode == 0) {
			kronuz::consensus::FixedConfiguration configuration;
			configuration.local = 1;
			configuration.voters = {1, 2};
			validate_peer_identity(server_context, configuration, pins);
			auto repeated = pins;
			repeated.at(2) = repeated.at(1);
			try {
				validate_peer_identity(server_context, configuration, repeated);
				assert(false);
			} catch (const std::invalid_argument &) {
			}
			configuration.local = 2;
			try {
				validate_peer_identity(server_context, configuration, pins);
				assert(false);
			} catch (const std::invalid_argument &) {
			}
			assert(authenticated_voter(server.get(), pins) == 2);
			assert(authenticated_voter(client.get(), pins) == 1);
			auto unknown = pins;
			unknown.erase(2);
			try {
				(void)authenticated_voter(server.get(), unknown);
				assert(false);
			} catch (const std::invalid_argument &) {
			}
			auto duplicate = pins;
			duplicate.emplace(3, pins.at(2));
			try {
				(void)authenticated_voter(server.get(), duplicate);
				assert(false);
			} catch (const std::invalid_argument &) {
			}
		}
		assert(SSL_CTX_get_max_early_data(server_context.native_handle()) == 0);
		assert(SSL_CTX_get_options(server_context.native_handle()) & SSL_OP_NO_RENEGOTIATION);
	}
	std::cout << "TLS BIO: mutual authentication, unknown CA, missing certificate, voter pins and HTTPS "
				 "hostname passed\n";
}
