#pragma once
#include "admission.h"
#include <atomic>
#include <map>
#include <mutex>
#include <optional>
#include <set>

// Node identity (async-drivers Q7 (a)): one key per node, pinned once per host by its key_pin, so
// a renewed certificate keeps its pin. Every connection of the host authenticates its peer's node
// against these pins, whatever groups the two nodes share; per-group checks belong to the
// group-open exchange. Nodes are admitted and removed while the host runs, the caller's trust
// policy may pin a node at its first join, and the trusted CAs reload (docs/mechanisms/admission.md).
namespace kronuz::consensus::transport {
// Copies share one directory: an admission through any copy holds for every copy, on every loop.
// Every member is safe from any thread.
class NodePins {
  public:
	// Throws std::invalid_argument unless every node is nonzero, every pin is distinct, and the
	// local node's pin is the key of the certificate `context` presents. The trust policy is the
	// cluster CA's (D149): `context`'s trusted certificates, and the cluster the local
	// certificate names (ClusterCa); reload() rebuilds it.
	NodePins(kronuz::consensus::NodeId local, std::map<kronuz::consensus::NodeId, Fingerprint> pins, SSL_CTX *context)
		: NodePins(local, std::move(pins), context, ClusterCa(context, local)) {
		directory_->cluster_ca = true;
	}
	// The same with the caller's trust policy: pre_approved_pins(), say. None admits only pins.
	NodePins(kronuz::consensus::NodeId local, std::map<kronuz::consensus::NodeId, Fingerprint> pins, SSL_CTX *context,
			 TrustPolicy policy)
		: local_(local), directory_(std::make_shared<Directory>()) {
		auto &d = *directory_;
		d.policy = std::move(policy);
		for (const auto &[node, pin] : pins) {
			if (!node || !d.owners.emplace(pin, node).second)
				throw std::invalid_argument("node pins: node 0 or a repeated certificate");
			d.nodes[node].pin = pin;
		}
		const auto mine = pins.find(local_);
		if (!local_ || mine == pins.end())
			throw std::invalid_argument("node pins: no pin for the local node");
		d.trust = trust_of(context, mine->second);
	}
	kronuz::consensus::NodeId local() const noexcept { return local_; }
	// Whether `node` is pinned and not removed.
	bool contains(kronuz::consensus::NodeId node) const { return pin(node).has_value(); }
	std::optional<Fingerprint> pin(kronuz::consensus::NodeId node) const {
		std::lock_guard hold(directory_->mutex);
		const auto it = directory_->nodes.find(node);
		return it == directory_->nodes.end() || it->second.removed ? std::nullopt : it->second.pin;
	}

	// Admits `node`: pinned to `pin`, or with none, to the certificate the trust policy accepts at
	// its first join. Lifts a removal. Throws PinRefused (repinned) when the node is pinned to
	// another certificate (remove it first), and std::invalid_argument for node 0 or a pin of
	// another node.
	void admit(kronuz::consensus::NodeId node, std::optional<Fingerprint> pin = std::nullopt) {
		if (!node)
			throw std::invalid_argument("node pins: node 0");
		auto &d = *directory_;
		std::lock_guard hold(d.mutex);
		if (pin) {
			if (const auto owner = d.owners.find(*pin); owner != d.owners.end() && owner->second != node)
				throw std::invalid_argument("node pins: a certificate pinned to another node");
			const auto it = d.nodes.find(node);
			if (it != d.nodes.end() && it->second.pin && !it->second.removed && *it->second.pin != *pin)
				throw PinRefused(PinFault::repinned, "node " + std::to_string(node) + " is pinned to another certificate");
		}
		auto &entry = d.nodes[node];
		if (entry.pin && (entry.removed || pin) && entry.pin != pin) {
			d.owners.erase(*entry.pin);  // a removed node's old certificate
			entry.pin.reset();
		}
		if (pin) {
			entry.pin = pin;
			d.owners[*pin] = node;
		}
		entry.removed = false;
	}
	// Removes `node`: its certificates are refused (removed) until it is admitted again. Each
	// removal raises epoch(). Throws std::invalid_argument for the local node.
	void remove(kronuz::consensus::NodeId node) {
		if (node == local_)
			throw std::invalid_argument("node pins: removing the local node");
		auto &d = *directory_;
		std::lock_guard hold(d.mutex);
		auto &entry = d.nodes[node];
		entry.removed = true;
		entry.removed_at = ++d.epoch;
	}
	// Removals so far. A connection that began authenticating at epoch() `since` carries `node`
	// only while current(node, since): pinned, and not removed since.
	std::uint64_t epoch() const {
		std::lock_guard hold(directory_->mutex);
		return directory_->epoch;
	}
	bool current(kronuz::consensus::NodeId node, std::uint64_t since) const {
		std::lock_guard hold(directory_->mutex);
		const auto it = directory_->nodes.find(node);
		return it != directory_->nodes.end() && it->second.pin && !it->second.removed && it->second.removed_at <= since;
	}
	// The local certificate and the CAs `context` trusts replace the ones every check uses from
	// now on (the cluster CA policy's too): a renewed certificate or a new bundle. Throws
	// std::invalid_argument when the local certificate is not the local node's key. Links whose
	// chain no longer verifies are the transport's to close (HostTransport::reload).
	void reload(SSL_CTX *context) {
		auto trust = trust_of(context, *pin(local_));
		auto &d = *directory_;
		TrustPolicy policy;
		if (d.cluster_ca)
			policy = ClusterCa(context, local_);
		std::lock_guard hold(d.mutex);
		d.trust = std::move(trust);
		if (d.cluster_ca)
			d.policy = std::move(policy);
	}
	// Whether the peer's leaf and the chain it sent on `ssl` verify, now, against the CAs trusted
	// since the last reload: false for an expired certificate, or one a reloaded bundle dropped.
	bool verifies(SSL *ssl) const {
		std::unique_ptr<X509, decltype(&X509_free)> leaf(ssl ? SSL_get1_peer_certificate(ssl) : nullptr, X509_free);
		if (!leaf)
			return false;
		std::shared_ptr<X509_STORE> trust;
		{
			std::lock_guard hold(directory_->mutex);
			trust = directory_->trust;
		}
		std::unique_ptr<X509_STORE_CTX, decltype(&X509_STORE_CTX_free)> context(X509_STORE_CTX_new(), X509_STORE_CTX_free);
		return context && X509_STORE_CTX_init(context.get(), trust.get(), leaf.get(), SSL_get_peer_cert_chain(ssl)) == 1 &&
			   X509_verify_cert(context.get()) == 1;
	}
	// Handshakes refused for `fault`, through any copy.
	std::uint64_t refused(PinFault fault) const {
		return directory_->refused[std::size_t(fault)].load(std::memory_order_relaxed);
	}

	// The node a completed handshake authenticated: a leaf TLS verified, that verifies still
	// against the CAs trusted now, whose key is pinned to a node other than this one or that the
	// trust policy admits at its first join (its pin is then recorded), and `expected` when
	// nonzero (the node a dial meant to reach). Throws PinRefused otherwise, before any frame is
	// read or written.
	kronuz::consensus::NodeId authenticate(SSL *ssl, kronuz::consensus::NodeId expected) const {
		if (!ssl || !(SSL_get_verify_mode(ssl) & SSL_VERIFY_PEER) || SSL_get_verify_result(ssl) != X509_V_OK)
			refuse(PinFault::unverified, "unverified peer certificate");
		std::unique_ptr<X509, decltype(&X509_free)> leaf(SSL_get1_peer_certificate(ssl), X509_free);
		if (!leaf)
			refuse(PinFault::unverified, "no peer certificate");
		if (!verifies(ssl))
			refuse(PinFault::unverified, "a peer certificate the CAs trusted now do not verify");
		const auto digest = key_pin(leaf.get());
		kronuz::consensus::NodeId found = 0;
		TrustPolicy policy;
		{
			std::lock_guard hold(directory_->mutex);
			found = owner(digest);
			policy = found ? TrustPolicy{} : directory_->policy;
		}
		if (!found && policy)
			found = first_join(digest, policy(leaf.get(), SSL_get_peer_cert_chain(ssl)), expected);
		if (!found)
			refuse(PinFault::unpinned, "unpinned peer certificate");
		if (found == local_)
			refuse(PinFault::own, "the peer presented this node's certificate");
		if (expected && found != expected)
			refuse(PinFault::another, "another node's certificate");
		return found;
	}

  private:
	struct Node {
		std::optional<Fingerprint> pin;  // a removed node keeps it, so its certificate is named removed
		bool removed = false;
		std::uint64_t removed_at = 0;  // the epoch of its last removal
	};
	struct Directory {
		mutable std::mutex mutex;
		std::shared_ptr<X509_STORE> trust;  // the CAs every check verifies against, replaced by reload()
		bool cluster_ca = false;            // the policy is the default ClusterCa, rebuilt by reload()
		std::map<kronuz::consensus::NodeId, Node> nodes;
		std::map<Fingerprint, kronuz::consensus::NodeId> owners;  // every pin, removed nodes' included
		std::uint64_t epoch = 0;
		TrustPolicy policy;
		std::atomic<std::uint64_t> refused[pin_faults] = {};
	};
	// The CAs `context` trusts, once its certificate is the key pinned `pin`.
	static std::shared_ptr<X509_STORE> trust_of(SSL_CTX *context, const Fingerprint &pin) {
		X509 *own = context ? SSL_CTX_get0_certificate(context) : nullptr;
		if (!own || key_pin(own) != pin)
			throw std::invalid_argument("node pins: the local certificate is not the local node's key");
		std::shared_ptr<X509_STORE> store(X509_STORE_new(), X509_STORE_free);
		std::unique_ptr<STACK_OF(X509), void (*)(STACK_OF(X509) *)> trusted(
			X509_STORE_get1_all_certs(SSL_CTX_get_cert_store(context)),
			[](STACK_OF(X509) *certificates) { sk_X509_pop_free(certificates, X509_free); });
		for (int i = 0; store && trusted && i < sk_X509_num(trusted.get()); ++i) {
			X509 *ca = sk_X509_value(trusted.get(), i);
			(void)X509_check_purpose(ca, -1, 0);  // cached before loops verify against it (tls.h)
			if (X509_STORE_add_cert(store.get(), ca) != 1)
				throw std::runtime_error("node pins: X509_STORE_add_cert");
		}
		if (!store)
			throw std::runtime_error("node pins: X509_STORE_new");
		return store;
	}
	[[noreturn]] void refuse(PinFault fault, const std::string &what) const {
		directory_->refused[std::size_t(fault)].fetch_add(1, std::memory_order_relaxed);
		throw PinRefused(fault, what);
	}
	// Under the lock: the node pinned to `digest`, or 0; a removed one's is refused.
	kronuz::consensus::NodeId owner(const Fingerprint &digest) const {
		const auto it = directory_->owners.find(digest);
		if (it == directory_->owners.end())
			return 0;
		if (directory_->nodes.at(it->second).removed)
			refuse(PinFault::removed, "node " + std::to_string(it->second) + " was removed");
		return it->second;
	}
	// The trust policy's node for an unpinned certificate, its pin recorded unless the connection
	// is refused anyway; throws when the policy admits none, or the node has a pin or was removed.
	kronuz::consensus::NodeId first_join(const Fingerprint &digest, Trust trust, kronuz::consensus::NodeId expected) const {
		if (!trust.node)
			refuse(trust.fault, trust.fault == PinFault::unpinned ? std::string("unpinned peer certificate")
																  : std::string("the trust policy refused it: ") + pin_fault_name(trust.fault));
		if (trust.node == local_ || (expected && trust.node != expected))
			return trust.node;  // refused by authenticate's own checks, unrecorded
		auto &d = *directory_;
		std::lock_guard hold(d.mutex);
		if (const auto found = owner(digest))
			return found;  // another loop recorded it meanwhile
		auto &entry = d.nodes[trust.node];
		if (entry.removed)
			refuse(PinFault::removed, "node " + std::to_string(trust.node) + " was removed");
		if (entry.pin)
			refuse(PinFault::repinned, "node " + std::to_string(trust.node) + " is pinned to another certificate");
		entry.pin = digest;
		d.owners[digest] = trust.node;
		return trust.node;
	}

	kronuz::consensus::NodeId local_;
	std::shared_ptr<Directory> directory_;
};
} // namespace kronuz::consensus::transport
