#pragma once
#include "tls.h"
#include <charconv>
#include <functional>
#include <openssl/x509_vfy.h>
#include <string>
#include <vector>

// Node admission's trust (docs/mechanisms/admission.md): why a certificate is refused, and the two
// trust policies NodePins takes for a certificate no pin names yet. The cluster CA is the default
// (D149); pre-approved pins are the alternative.
namespace kronuz::consensus::transport {
// Why a certificate is not a node's: the handshake that presented it is refused.
enum class PinFault : std::uint8_t {
	unverified,  // TLS did not verify it
	unpinned,    // no pin names it, and the trust policy admits none
	cluster,     // the trust policy's: it names no node of this cluster
	repinned,    // it names a node pinned to another certificate
	removed,     // its node was removed, and is refused until admitted again
	own,         // it is this node's
	another,     // a dial meant to reach another node
};
inline constexpr std::size_t pin_faults = 7;
inline const char *pin_fault_name(PinFault fault) noexcept {
	static constexpr const char *names[pin_faults] = {"unverified", "unpinned", "cluster", "repinned",
													  "removed",    "own",      "another"};
	return std::size_t(fault) < pin_faults ? names[std::size_t(fault)] : "?";
}
struct PinRefused : std::invalid_argument {
	PinFault fault;
	PinRefused(PinFault f, const std::string &what) : std::invalid_argument("node pins: " + what), fault(f) {}
};

// A trust policy's answer for a verified certificate no pin names: the node it joins as (its pin
// is then recorded), or node 0 and the fault that refuses it.
struct Trust {
	kronuz::consensus::NodeId node = 0;
	PinFault fault = PinFault::unpinned;
};
// A trust policy over the peer's leaf and the chain it sent. Called on the handshake's loop, on
// several loops at once, outside the pins' lock.
using TrustPolicy = std::function<Trust(X509 *leaf, STACK_OF(X509) *chain)>;

// Pre-approved pins only, as no policy: a certificate is a node's once NodePins::admit(node, pin)
// named it, whatever signed it, and every other one is unpinned.
inline TrustPolicy pre_approved_pins() { return {}; }

// The cluster CA as trust anchor: a certificate that chains to the CA and whose subjectAltName URI
// is the cluster's prefix and a decimal node id (`spiffe://<cluster>/node/` and 17, say) joins as
// that node, and NodePins records its pin then: from then on another certificate naming the node
// is refused (repinned) until the node is removed and admitted again. A certificate the CA did not
// sign is unpinned; one naming no node under the prefix, or two, or a zero-padded id, is cluster.
class ClusterCa {
  public:
	// `ca` alone, and `prefix`.
	ClusterCa(X509 *ca, std::string prefix) : ClusterCa(std::move(prefix)) {
		if (!ca || prefix_.empty() || X509_STORE_add_cert(store_.get(), ca) != 1)
			throw std::invalid_argument("cluster CA: no certificate or an empty node prefix");
		(void)X509_check_purpose(ca, -1, 0);  // cached before loops verify against it (tls.h)
	}
	// The default: the certificates `context` trusts, and the prefix of the URI by which the local
	// certificate names `local`. When it names none, the CA admits none: only pins do (unpinned).
	ClusterCa(SSL_CTX *context, kronuz::consensus::NodeId local) : ClusterCa(prefix_of(SSL_CTX_get0_certificate(context), local)) {
		std::unique_ptr<STACK_OF(X509), void (*)(STACK_OF(X509) *)> trusted(
			X509_STORE_get1_all_certs(SSL_CTX_get_cert_store(context)),
			[](STACK_OF(X509) *certificates) { sk_X509_pop_free(certificates, X509_free); });
		for (int i = 0; trusted && i < sk_X509_num(trusted.get()); ++i) {
			X509 *ca = sk_X509_value(trusted.get(), i);
			(void)X509_check_purpose(ca, -1, 0);
			if (X509_STORE_add_cert(store_.get(), ca) != 1)
				throw std::runtime_error("cluster CA: X509_STORE_add_cert");
		}
	}
	const std::string &prefix() const noexcept { return prefix_; }
	Trust operator()(X509 *leaf, STACK_OF(X509) *chain) const {
		if (prefix_.empty())
			return {0, PinFault::unpinned};  // a cluster that names no nodes: only pins admit
		std::unique_ptr<X509_STORE_CTX, decltype(&X509_STORE_CTX_free)> context(X509_STORE_CTX_new(), X509_STORE_CTX_free);
		if (!context || X509_STORE_CTX_init(context.get(), store_.get(), leaf, chain) != 1 ||
			X509_verify_cert(context.get()) != 1)
			return {0, PinFault::unpinned};
		return {node_of(leaf, prefix_), PinFault::cluster};
	}
	// The node `leaf`'s URIs name under `prefix`, or 0 when they name none, or several.
	static kronuz::consensus::NodeId node_of(X509 *leaf, std::string_view prefix) {
		kronuz::consensus::NodeId node = 0;
		for (const std::string_view uri : uris(leaf)) {
			if (!uri.starts_with(prefix))
				continue;
			const auto id = decimal(uri.substr(prefix.size()));
			if (!id || (node && node != id))
				return 0;
			node = id;
		}
		return node;
	}

	// The URI by which `certificate` names `node`, less the id: empty when none does.
	static std::string prefix_of(X509 *certificate, kronuz::consensus::NodeId node) {
		if (!certificate || !node)
			return {};
		const auto id = std::to_string(node);
		for (const std::string_view uri : uris(certificate))
			if (uri.size() > id.size() && uri.ends_with(id) && uri[uri.size() - id.size() - 1] == '/')
				return std::string(uri.substr(0, uri.size() - id.size()));
		return {};
	}

  private:
	static std::size_t asn1_length(const ASN1_STRING *s) {
#if OPENSSL_VERSION_NUMBER >= 0x40100000L
		return ASN1_STRING_get_length(s);
#else
		return std::size_t(ASN1_STRING_length(s));
#endif
	}
	explicit ClusterCa(std::string prefix) : store_(X509_STORE_new(), X509_STORE_free), prefix_(std::move(prefix)) {
		if (!store_)
			throw std::runtime_error("cluster CA: X509_STORE_new");
	}
	// A node id as decimal digits, no zero padding; 0 otherwise.
	static kronuz::consensus::NodeId decimal(std::string_view digits) {
		kronuz::consensus::NodeId id = 0;
		const auto [end, error] = std::from_chars(digits.data(), digits.data() + digits.size(), id);
		return error == std::errc{} && end == digits.data() + digits.size() && digits[0] != '0' ? id : 0;
	}
	static std::vector<std::string> uris(X509 *leaf) {
		std::vector<std::string> found;
		auto *names = static_cast<GENERAL_NAMES *>(X509_get_ext_d2i(leaf, NID_subject_alt_name, nullptr, nullptr));
		for (int i = 0; names && i < sk_GENERAL_NAME_num(names); ++i)
			if (const GENERAL_NAME *name = sk_GENERAL_NAME_value(names, i); name->type == GEN_URI) {
				const ASN1_STRING *uri = name->d.uniformResourceIdentifier;
				found.emplace_back(reinterpret_cast<const char *>(ASN1_STRING_get0_data(uri)), std::size_t(asn1_length(uri)));
			}
		GENERAL_NAMES_free(names);
		return found;
	}

	std::shared_ptr<X509_STORE> store_;
	std::string prefix_;
};
} // namespace kronuz::consensus::transport
