#pragma once
#include "hello.h"
#include "reactor_tls.h"
#include <asio/ip/address.hpp>
#include <asio/ssl.hpp>
#include <map>
#include <memory>
#include <openssl/rand.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <set>

namespace kronuz::consensus::transport {
// Caches the decoded extensions of every certificate the context holds. OpenSSL caches them on
// a certificate's first verification and publishes them with an atomic flag ThreadSanitizer
// cannot see in libcrypto; cached before loops share the context, handshakes only read them.
inline void cache_certificates(asio::ssl::context &context) {
	SSL_CTX *ctx = context.native_handle();
	auto cache = [](STACK_OF(X509) *certificates) {
		for (int i = 0; i < sk_X509_num(certificates); ++i)
			(void)X509_check_purpose(sk_X509_value(certificates, i), -1, 0);
	};
	if (X509 *own = SSL_CTX_get0_certificate(ctx))
		(void)X509_check_purpose(own, -1, 0);
	STACK_OF(X509) *chain = nullptr;
	if (SSL_CTX_get0_chain_certs(ctx, &chain) == 1)
		cache(chain);
	chain = nullptr;
	if (SSL_CTX_get_extra_chain_certs_only(ctx, &chain) == 1)
		cache(chain);
	std::unique_ptr<STACK_OF(X509), void (*)(STACK_OF(X509) *)> trusted(
		X509_STORE_get1_all_certs(SSL_CTX_get_cert_store(ctx)),
		[](STACK_OF(X509) *certificates) { sk_X509_pop_free(certificates, X509_free); });
	cache(trusted.get());
}
inline Fingerprint certificate_fingerprint(X509 *certificate) {
	if (!certificate)
		throw std::invalid_argument("missing certificate");
	Fingerprint digest{};
	unsigned length = 0;
	if (X509_digest(certificate, EVP_sha256(), digest.data(), &length) != 1 || length != digest.size())
		throw std::runtime_error("certificate fingerprint");
	return digest;
}
// A key's pin: the SHA-256 of its DER SubjectPublicKeyInfo, as kubeadm and HPKP pin. Every
// certificate for the key has it, so a renewal keeps its pin (docs/mechanisms/admission.md).
inline Fingerprint key_pin(EVP_PKEY *key) {
	unsigned char *der = nullptr;
	const int n = key ? i2d_PUBKEY(key, &der) : 0;
	Fingerprint digest{};
	unsigned length = 0;
	const bool ok = n > 0 && EVP_Digest(der, std::size_t(n), digest.data(), &length, EVP_sha256(), nullptr) == 1 &&
					length == digest.size();
	OPENSSL_free(der);
	if (!ok)
		throw std::invalid_argument("key pin: no key, or its encoding failed");
	return digest;
}
inline Fingerprint key_pin(X509 *certificate) {
	if (!certificate)
		throw std::invalid_argument("missing certificate");
	return key_pin(X509_get0_pubkey(certificate));
}
inline void validate_peer_identity(asio::ssl::context &context,
								   const kronuz::consensus::FixedConfiguration &configuration,
								   const std::map<kronuz::consensus::NodeId, Fingerprint> &pins) {
	std::set<kronuz::consensus::NodeId> voters(configuration.voters.begin(), configuration.voters.end());
	if (voters.empty() || voters.contains(0) || voters.size() != configuration.voters.size() ||
		!voters.contains(configuration.local) || pins.size() != voters.size())
		throw std::invalid_argument("invalid voter pin membership");
	std::set<Fingerprint> unique;
	for (const auto &[node, fingerprint] : pins) {
		if (!voters.contains(node) || !unique.insert(fingerprint).second)
			throw std::invalid_argument("duplicate or unknown voter pin");
	}
	const auto local = certificate_fingerprint(SSL_CTX_get0_certificate(context.native_handle()));
	if (CRYPTO_memcmp(local.data(), pins.at(configuration.local).data(), local.size()) != 0)
		throw std::invalid_argument("local voter certificate mismatch");
}
inline kronuz::consensus::Identity fresh_connection_identity() {
	kronuz::consensus::Identity result;
	do {
		if (RAND_bytes(reinterpret_cast<unsigned char *>(result.data()), result.size()) != 1)
			throw std::runtime_error("connection identity entropy");
	} while (result == kronuz::consensus::Identity{});
	return result;
}
// Pin the verified leaf certificate to one configured voter. A trusted CA alone
// cannot give an arbitrary certificate holder a voter identity.
inline kronuz::consensus::NodeId
authenticated_voter(SSL *ssl, const std::map<kronuz::consensus::NodeId, Fingerprint> &configured) {
	if (!ssl || !(SSL_get_verify_mode(ssl) & SSL_VERIFY_PEER) || SSL_get_verify_result(ssl) != X509_V_OK)
		throw std::invalid_argument("unverified peer certificate");
	std::unique_ptr<X509, decltype(&X509_free)> certificate(SSL_get1_peer_certificate(ssl), X509_free);
	if (!certificate)
		throw std::invalid_argument("missing peer certificate");
	const auto digest = certificate_fingerprint(certificate.get());
	kronuz::consensus::NodeId found = 0;
	for (const auto &[node, pin] : configured) {
		if (CRYPTO_memcmp(pin.data(), digest.data(), digest.size()) == 0) {
			if (!node || found)
				throw std::invalid_argument("ambiguous voter certificate");
			found = node;
		}
	}
	if (!found)
		throw std::invalid_argument("unknown voter certificate");
	return found;
}
} // namespace kronuz::consensus::transport
