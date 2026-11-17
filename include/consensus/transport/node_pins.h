#pragma once
#include "tls.h"
#include <map>
#include <set>

// Node identity (async-drivers Q7 (a)): one certificate per node, pinned once per host. Every
// connection of the host authenticates its peer's node against these pins, whatever groups the
// two nodes share; per-group checks belong to the group-open exchange.
namespace kronuz::consensus::transport {
class NodePins {
  public:
	// Throws std::invalid_argument unless every node is nonzero, every fingerprint is distinct,
	// and the local node's pin is the certificate `context` presents.
	NodePins(kronuz::consensus::NodeId local, std::map<kronuz::consensus::NodeId, Fingerprint> pins, SSL_CTX *context)
		: local_(local), pins_(std::move(pins)) {
		std::set<Fingerprint> unique;
		for (const auto &[node, pin] : pins_)
			if (!node || !unique.insert(pin).second)
				throw std::invalid_argument("node pins: node 0 or a repeated certificate");
		const auto mine = pins_.find(local_);
		if (!local_ || mine == pins_.end())
			throw std::invalid_argument("node pins: no pin for the local node");
		const auto presented = certificate_fingerprint(SSL_CTX_get0_certificate(context));
		if (CRYPTO_memcmp(presented.data(), mine->second.data(), presented.size()) != 0)
			throw std::invalid_argument("node pins: the local certificate is not the local node's pin");
	}
	kronuz::consensus::NodeId local() const noexcept { return local_; }
	bool contains(kronuz::consensus::NodeId node) const noexcept { return pins_.contains(node); }

	// The node a completed handshake authenticated: a verified leaf whose fingerprint is pinned
	// to a node other than this one, and `expected` when nonzero (the node a dial meant to reach).
	// Throws std::invalid_argument otherwise, before any frame is read or written.
	kronuz::consensus::NodeId authenticate(SSL *ssl, kronuz::consensus::NodeId expected) const {
		if (!ssl || !(SSL_get_verify_mode(ssl) & SSL_VERIFY_PEER) || SSL_get_verify_result(ssl) != X509_V_OK)
			throw std::invalid_argument("node pins: unverified peer certificate");
		std::unique_ptr<X509, decltype(&X509_free)> leaf(SSL_get1_peer_certificate(ssl), X509_free);
		if (!leaf)
			throw std::invalid_argument("node pins: no peer certificate");
		const auto digest = certificate_fingerprint(leaf.get());
		kronuz::consensus::NodeId found = 0;
		for (const auto &[node, pin] : pins_)
			if (CRYPTO_memcmp(pin.data(), digest.data(), digest.size()) == 0)
				found = node;
		if (!found)
			throw std::invalid_argument("node pins: unpinned peer certificate");
		if (found == local_)
			throw std::invalid_argument("node pins: the peer presented this node's certificate");
		if (expected && found != expected)
			throw std::invalid_argument("node pins: another node's certificate");
		return found;
	}

  private:
	kronuz::consensus::NodeId local_;
	std::map<kronuz::consensus::NodeId, Fingerprint> pins_;
};
} // namespace kronuz::consensus::transport
