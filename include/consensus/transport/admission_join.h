#pragma once
#include "admission.h"
#include "deadline.h"
#include <asio/ip/tcp.hpp>
#include <asio/read.hpp>
#include <asio/ssl/stream.hpp>
#include <asio/write.hpp>
#include <filesystem>
#include <fstream>
#include <journal/codec.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <task_asio.hh>

// Issue on join (D150, docs/mechanisms/admission.md, Joining): a node with no key and no
// certificate joins with the cluster's join token. It makes its own key, pins the cluster CA by the
// token's fingerprint, and a node holding the CA key issues its certificate in exchange for the
// token's secret. The certificate names it under the cluster CA policy's prefix, so the CA policy
// admits it at its first connection and records its pin. Over TLS, DTJN0001 frames, each a 4-byte
// little-endian length and its bytes:
//   issuer -> joiner  the CA certificate (DER), before the joiner reveals anything
//   joiner -> issuer  secret length u8, the secret, requested node u64, its CSR (DER)
//   issuer -> joiner  fault u8 (0 issued, else JoinFault + 1), node u64, its certificate (DER)
namespace kronuz::consensus::transport {
inline constexpr std::size_t join_secret_bytes = 32;
inline constexpr std::size_t join_frame_limit = 64 * 1024;  // a frame holds one certificate or CSR

// What the cluster hands a joining node: the fingerprint of its CA, and the secret an issuer
// checks. Printed as "DTJT" and both in hex, 132 characters.
struct JoinToken {
	Fingerprint ca{};
	std::string secret;
	bool operator==(const JoinToken &) const = default;
};
inline JoinToken fresh_join_token(X509 *ca) {
	JoinToken token{certificate_fingerprint(ca), std::string(join_secret_bytes, '\0')};
	if (RAND_bytes(reinterpret_cast<unsigned char *>(token.secret.data()), int(token.secret.size())) != 1)
		throw std::runtime_error("join token entropy");
	return token;
}
inline std::string encode_join_token(const JoinToken &token) {
	static constexpr char hex[] = "0123456789abcdef";
	std::string text = "DTJT";
	for (std::string_view bytes : {std::string_view(reinterpret_cast<const char *>(token.ca.data()), token.ca.size()),
								   std::string_view(token.secret)})
		for (unsigned char c : bytes)
			text += {hex[c >> 4], hex[c & 15]};
	return text;
}
inline std::optional<JoinToken> decode_join_token(std::string_view text) {
	if (text.size() != 4 + 2 * (32 + join_secret_bytes) || !text.starts_with("DTJT"))
		return std::nullopt;
	std::string bytes;
	for (std::size_t i = 4; i != text.size(); i += 2) {
		unsigned value = 0;
		const auto [end, error] = std::from_chars(text.data() + i, text.data() + i + 2, value, 16);
		if (error != std::errc{} || end != text.data() + i + 2)
			return std::nullopt;
		bytes += char(value);
	}
	JoinToken token;
	std::copy_n(bytes.begin(), token.ca.size(), token.ca.begin());
	token.secret = bytes.substr(token.ca.size());
	return token;
}

// Why a join is refused: at the issuer, the token's secret is wrong or missing (token), a frame
// is not DTJN0001 (malformed), or the issuer's hook refused the node (node); at the joiner, the
// issuer does not hold the token's CA (ca), or the certificate it issued is not this joiner's (issued).
enum class JoinFault : std::uint8_t { token, malformed, node, ca, issued };
inline constexpr std::size_t join_faults = 5;
inline const char *join_fault_name(JoinFault fault) noexcept {
	static constexpr const char *names[join_faults] = {"token", "malformed", "node", "ca", "issued"};
	return std::size_t(fault) < join_faults ? names[std::size_t(fault)] : "?";
}
struct JoinRefused : std::runtime_error {
	JoinFault fault;
	JoinRefused(JoinFault f, const std::string &what) : std::runtime_error("join: " + what), fault(f) {}
};

namespace join_detail {
template <class T, void (*Free)(T *)> struct Freer {
	void operator()(T *p) const noexcept { Free(p); }
};
using Key = std::unique_ptr<EVP_PKEY, Freer<EVP_PKEY, EVP_PKEY_free>>;
using Certificate = std::unique_ptr<X509, Freer<X509, X509_free>>;
using Request = std::unique_ptr<X509_REQ, Freer<X509_REQ, X509_REQ_free>>;
using Stream = asio::ssl::stream<asio::ip::tcp::socket>;

template <class T, class Encode> std::string der(T *object, Encode encode) {
	unsigned char *bytes = nullptr;
	const int n = encode(object, &bytes);
	if (n <= 0)
		throw std::runtime_error("join: DER encoding");
	std::string r(reinterpret_cast<char *>(bytes), std::size_t(n));
	OPENSSL_free(bytes);
	return r;
}
inline std::string der(X509 *c) { return der(c, [](X509 *x, unsigned char **out) { return i2d_X509(x, out); }); }
inline std::string der(X509_REQ *r) {
	return der(r, [](X509_REQ *x, unsigned char **out) { return i2d_X509_REQ(x, out); });
}
inline Certificate certificate_of(std::string_view bytes) {
	auto *p = reinterpret_cast<const unsigned char *>(bytes.data());
	Certificate c(d2i_X509(nullptr, &p, long(bytes.size())));
	if (!c || p != reinterpret_cast<const unsigned char *>(bytes.data() + bytes.size()))
		return nullptr;
	return c;
}
inline Request request_of(std::string_view bytes) {
	auto *p = reinterpret_cast<const unsigned char *>(bytes.data());
	Request r(d2i_X509_REQ(nullptr, &p, long(bytes.size())));
	if (!r || p != reinterpret_cast<const unsigned char *>(bytes.data() + bytes.size()))
		return nullptr;
	return r;
}
template <class Write> std::string pem(Write write) {
	std::unique_ptr<BIO, decltype(&BIO_free)> bio(BIO_new(BIO_s_mem()), BIO_free);
	if (!bio || write(bio.get()) != 1)
		throw std::runtime_error("join: PEM encoding");
	char *data = nullptr;
	const long n = BIO_get_mem_data(bio.get(), &data);
	return std::string(data, std::size_t(n));
}
// Whether `leaf` chains to `ca` alone, through the intermediates in `chain`.
inline bool chains_to(X509 *leaf, STACK_OF(X509) *chain, X509 *ca) {
	std::unique_ptr<X509_STORE, decltype(&X509_STORE_free)> store(X509_STORE_new(), X509_STORE_free);
	std::unique_ptr<X509_STORE_CTX, decltype(&X509_STORE_CTX_free)> context(X509_STORE_CTX_new(), X509_STORE_CTX_free);
	return store && context && X509_STORE_add_cert(store.get(), ca) == 1 &&
		   X509_STORE_CTX_init(context.get(), store.get(), leaf, chain) == 1 && X509_verify_cert(context.get()) == 1;
}

// Ends a stream's operations at the deadline: closing the socket fails them.
struct Bounded {
	struct Close {
		Stream *stream;
		void operator()() const noexcept {
			asio::error_code ignored;
			stream->lowest_layer().close(ignored);
		}
	};
	late_deadline deadline;
	task::cancel_callback<Close> expired;
	Bounded(task::scheduler_ref loop, std::chrono::milliseconds limit, Stream &stream)
		: deadline(loop, limit), expired(deadline.token(), Close{&stream}) {}
};
inline task::task<void> write_frame(Stream &stream, std::string bytes) {
	std::string frame;
	kronuz::journal::put32(frame, std::uint32_t(bytes.size()));
	frame += bytes;
	auto [error, n] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
		[&](auto h) { asio::async_write(stream, asio::buffer(frame), std::move(h)); });
	if (error)
		throw std::system_error(error, "join: write");
}
inline task::task<std::string> read_frame(Stream &stream) {
	std::string length(4, '\0');
	auto [error, n] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
		[&](auto h) { asio::async_read(stream, asio::buffer(length), std::move(h)); });
	if (error)
		throw std::system_error(error, "join: read");
	std::string_view view(length);
	const auto size = kronuz::journal::get32(view);
	if (!size || size > join_frame_limit)
		throw JoinRefused(JoinFault::malformed, "a frame of " + std::to_string(size) + " bytes");
	std::string bytes(size, '\0');
	auto [again, m] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
		[&](auto h) { asio::async_read(stream, asio::buffer(bytes), std::move(h)); });
	if (again)
		throw std::system_error(again, "join: read");
	co_return bytes;
}
inline task::task<void> handshake(Stream &stream, asio::ssl::stream_base::handshake_type type) {
	const auto error = co_await task::await_asio<void(asio::error_code)>([&](auto h) { stream.async_handshake(type, std::move(h)); });
	if (error)
		throw std::system_error(error, "join: TLS handshake");
}
} // namespace join_detail

// The issuer: a node holding the CA key. Its hook names the node a joiner becomes, or 0 to
// refuse it, and may record the issued certificate (NodePins::admit with its pin, say).
class Issuer {
  public:
	struct Hooks {
		std::function<kronuz::consensus::NodeId(kronuz::consensus::NodeId requested)> assign;  // required
		std::function<void(kronuz::consensus::NodeId, X509 *issued)> issued;
	};
	// `context` is the issuer's own transport context, whose certificate chains to `ca`; `prefix`
	// the CA policy's (ClusterCa::prefix()). Throws std::invalid_argument when the key is not the
	// CA's, the secret is not join_secret_bytes, or the hook is missing.
	Issuer(std::shared_ptr<asio::ssl::context> context, X509 *ca, EVP_PKEY *ca_key, std::string prefix, std::string secret,
		   Hooks hooks, std::chrono::hours valid = std::chrono::hours(24 * 365))
		: context_(std::move(context)), ca_(X509_dup(ca)), prefix_(std::move(prefix)), secret_(std::move(secret)),
		  hooks_(std::move(hooks)), valid_(valid) {
		if (!context_ || !ca_ || !ca_key || X509_check_private_key(ca_.get(), ca_key) != 1 || prefix_.empty() ||
			secret_.size() != join_secret_bytes || !hooks_.assign)
			throw std::invalid_argument("issuer: CA key, prefix, secret or assign hook");
		EVP_PKEY_up_ref(ca_key);
		key_.reset(ca_key);
	}
	// Joins refused at this issuer for `fault`.
	std::uint64_t refused(JoinFault fault) const { return refused_[std::size_t(fault)].load(std::memory_order_relaxed); }

	// One joiner on an accepted `socket`, within `limit`: the node it became, or 0 when refused
	// (the joiner reads the fault) or failed.
	task::task<kronuz::consensus::NodeId> serve(asio::ip::tcp::socket socket, task::scheduler_ref loop,
												 std::chrono::milliseconds limit = std::chrono::milliseconds(5000)) {
		using namespace join_detail;
		Stream stream(std::move(socket), *context_);
		SSL_set_verify(stream.native_handle(), SSL_VERIFY_NONE, nullptr);  // a joiner has no certificate yet
		Bounded bound(loop, limit, stream);
		kronuz::consensus::NodeId node = 0;
		try {
			co_await handshake(stream, asio::ssl::stream_base::server);
			co_await write_frame(stream, der(ca_.get()));
			const auto request = co_await read_frame(stream);
			std::string answer(1, '\0');
			std::string issued;
			try {
				issued = issue(request, node);
			} catch (const JoinRefused &e) {
				refused_[std::size_t(e.fault)].fetch_add(1, std::memory_order_relaxed);
				answer[0] = char(1 + int(e.fault));
				node = 0;
			}
			kronuz::journal::put64(answer, node);
			co_await write_frame(stream, answer + issued);
		} catch (const JoinRefused &e) {
			refused_[std::size_t(e.fault)].fetch_add(1, std::memory_order_relaxed);
			node = 0;
		} catch (...) {
			node = 0;
		}
		asio::error_code ignored;
		stream.lowest_layer().close(ignored);
		co_return node;
	}

  private:
	// The certificate for a request (DER) once its secret matches: `node` set, or JoinRefused.
	std::string issue(std::string_view request, kronuz::consensus::NodeId &node) {
		using namespace join_detail;
		if (request.empty() || request.size() < 1 + std::size_t(std::uint8_t(request[0])) + 8)
			throw JoinRefused(JoinFault::malformed, "a short request");
		const std::size_t length = std::uint8_t(request[0]);
		const auto secret = request.substr(1, length);
		if (secret.size() != secret_.size() || CRYPTO_memcmp(secret.data(), secret_.data(), secret_.size()) != 0)
			throw JoinRefused(JoinFault::token, secret.empty() ? "no token" : "a wrong token");
		request.remove_prefix(1 + length);
		const auto requested = kronuz::journal::get64(request);
		auto csr = request_of(request);
		Key public_key(csr ? X509_REQ_get_pubkey(csr.get()) : nullptr);
		if (!public_key || X509_REQ_verify(csr.get(), public_key.get()) != 1)
			throw JoinRefused(JoinFault::malformed, "an unsigned request");
		node = hooks_.assign(requested);
		if (!node)
			throw JoinRefused(JoinFault::node, "the issuer refused node " + std::to_string(requested));
		auto certificate = sign(node, public_key.get());
		if (hooks_.issued)
			hooks_.issued(node, certificate.get());
		return der(certificate.get());
	}
	join_detail::Certificate sign(kronuz::consensus::NodeId node, EVP_PKEY *subject_key) const {
		join_detail::Certificate c(X509_new());
		unsigned char serial[16];
		if (!c || RAND_bytes(serial, sizeof serial) != 1)
			throw std::runtime_error("issuer: certificate");
		serial[0] &= 0x7f;  // a positive serial
		std::unique_ptr<BIGNUM, decltype(&BN_free)> number(BN_bin2bn(serial, sizeof serial, nullptr), BN_free);
		std::unique_ptr<X509_NAME, decltype(&X509_NAME_free)> subject(X509_NAME_new(), X509_NAME_free);
		const auto name = "node " + std::to_string(node);
		bool ok = number && subject && X509_set_version(c.get(), 2) == 1 &&
				  BN_to_ASN1_INTEGER(number.get(), X509_get_serialNumber(c.get())) &&
				  X509_NAME_add_entry_by_txt(subject.get(), "CN", MBSTRING_UTF8,
											 reinterpret_cast<const unsigned char *>(name.data()), int(name.size()), -1, 0) == 1 &&
				  X509_set_subject_name(c.get(), subject.get()) == 1 &&
				  X509_set_issuer_name(c.get(), X509_get_subject_name(ca_.get())) == 1 &&
				  X509_gmtime_adj(X509_getm_notBefore(c.get()), -300) &&
				  X509_gmtime_adj(X509_getm_notAfter(c.get()), long(std::chrono::seconds(valid_).count())) &&
				  X509_set_pubkey(c.get(), subject_key) == 1;
		X509V3_CTX v3;
		X509V3_set_ctx(&v3, ca_.get(), c.get(), nullptr, nullptr, 0);
		const std::string uri = "URI:" + prefix_ + std::to_string(node);
		for (const auto &[nid, value] : {std::pair{NID_basic_constraints, "critical,CA:FALSE"},
										 {NID_key_usage, "critical,digitalSignature,keyEncipherment"},
										 {NID_ext_key_usage, "serverAuth,clientAuth"},
										 {NID_subject_alt_name, uri.c_str()},
										 {NID_subject_key_identifier, "hash"},
										 {NID_authority_key_identifier, "keyid"}}) {
			X509_EXTENSION *extension = ok ? X509V3_EXT_conf_nid(nullptr, &v3, nid, value) : nullptr;
			ok = extension && X509_add_ext(c.get(), extension, -1) == 1;
			X509_EXTENSION_free(extension);
		}
		if (!ok || X509_sign(c.get(), key_.get(), EVP_sha256()) <= 0)
			throw std::runtime_error("issuer: signing");
		return c;
	}

	std::shared_ptr<asio::ssl::context> context_;
	join_detail::Certificate ca_;
	join_detail::Key key_;
	std::string prefix_, secret_;
	Hooks hooks_;
	std::chrono::hours valid_;
	std::atomic<std::uint64_t> refused_[join_faults] = {};
};

// What a joiner holds once issued: its node, its own key, its certificate and the CA's, as PEM.
struct Joined {
	kronuz::consensus::NodeId node = 0;
	std::string key, certificate, ca;
	// Writes the three files into `directory` (the key readable by its owner only) and returns
	// the options a Peer context loads them with.
	reactor::tls::Options save(const std::filesystem::path &directory) const {
		std::filesystem::create_directories(directory);
		const auto put = [&](const char *name, const std::string &bytes, bool secret) {
			const auto path = directory / name;
			std::filesystem::remove(path);
			std::ofstream(path, std::ios::binary) << bytes;
			if (secret)
				std::filesystem::permissions(path, std::filesystem::perms::owner_read | std::filesystem::perms::owner_write);
			return path.string();
		};
		return {put("node.pem", certificate, false), put("node.key", key, true), put("ca.pem", ca, false)};
	}
};

// Joins through the issuer at `endpoint` with `token`, asking to be node `requested` (0: the
// issuer's choice): a fresh P-256 key, the CA pinned by the token's fingerprint before the
// secret goes out, and the issued certificate checked against both. Throws JoinRefused with the
// fault, or std::system_error when the connection fails or `limit` passes.
inline task::task<Joined> join(asio::any_io_executor executor, task::scheduler_ref loop, asio::ip::tcp::endpoint endpoint,
							   JoinToken token, kronuz::consensus::NodeId requested = 0,
							   std::chrono::milliseconds limit = std::chrono::milliseconds(5000)) {
	using namespace join_detail;
	if (token.secret.size() > 255)
		throw std::invalid_argument("join: a secret past 255 bytes");
	Key key(EVP_EC_gen("P-256"));
	Request csr(X509_REQ_new());
	std::unique_ptr<X509_NAME, decltype(&X509_NAME_free)> subject(X509_NAME_new(), X509_NAME_free);
	if (!key || !csr || !subject || X509_REQ_set_pubkey(csr.get(), key.get()) != 1 ||
		X509_NAME_add_entry_by_txt(subject.get(), "CN", MBSTRING_ASC, reinterpret_cast<const unsigned char *>("joiner"), -1, -1, 0) != 1 ||
		X509_REQ_set_subject_name(csr.get(), subject.get()) != 1 || X509_REQ_sign(csr.get(), key.get(), EVP_sha256()) <= 0)
		throw std::runtime_error("join: key and request");
	asio::ssl::context tls(asio::ssl::context::tls_client);
	SSL_CTX_set_min_proto_version(tls.native_handle(), TLS1_2_VERSION);
	tls.set_verify_mode(asio::ssl::verify_none);  // the issuer is checked against the token's CA below
	Stream stream(asio::ip::tcp::socket(executor), tls);
	Bounded bound(loop, limit, stream);
	const auto connected = co_await task::await_asio<void(asio::error_code)>(
		[&](auto h) { stream.lowest_layer().async_connect(endpoint, std::move(h)); });
	if (connected)
		throw std::system_error(connected, "join: connect");
	co_await handshake(stream, asio::ssl::stream_base::client);
	auto ca = certificate_of(co_await read_frame(stream));
	Certificate issuer(SSL_get1_peer_certificate(stream.native_handle()));
	if (!ca || certificate_fingerprint(ca.get()) != token.ca || !issuer ||
		!chains_to(issuer.get(), SSL_get_peer_cert_chain(stream.native_handle()), ca.get()))
		throw JoinRefused(JoinFault::ca, "the issuer does not hold the token's CA");
	std::string request(1, char(token.secret.size()));
	request += token.secret;
	kronuz::journal::put64(request, requested);
	request += der(csr.get());
	co_await write_frame(stream, std::move(request));
	auto answer = co_await read_frame(stream);
	if (answer.size() < 9)
		throw JoinRefused(JoinFault::malformed, "a short answer");
	if (const auto fault = std::uint8_t(answer[0]); fault)
		throw JoinRefused(fault <= join_faults ? JoinFault(fault - 1) : JoinFault::malformed,
						  std::string("refused by the issuer: ") + join_fault_name(JoinFault(fault - 1)));
	std::string_view rest(answer);
	rest.remove_prefix(1);
	Joined joined;
	joined.node = kronuz::journal::get64(rest);
	auto certificate = certificate_of(rest);
	Key issued_key(certificate ? X509_get_pubkey(certificate.get()) : nullptr);
	if (!certificate || !issued_key || EVP_PKEY_eq(issued_key.get(), key.get()) != 1 ||
		!chains_to(certificate.get(), nullptr, ca.get()) || ClusterCa::prefix_of(certificate.get(), joined.node).empty())
		throw JoinRefused(JoinFault::issued, "the issued certificate is not this joiner's");
	joined.key = pem([&](BIO *b) { return PEM_write_bio_PrivateKey(b, key.get(), nullptr, nullptr, 0, nullptr, nullptr); });
	joined.certificate = pem([&](BIO *b) { return PEM_write_bio_X509(b, certificate.get()); });
	joined.ca = pem([&](BIO *b) { return PEM_write_bio_X509(b, ca.get()); });
	asio::error_code ignored;
	stream.lowest_layer().close(ignored);
	co_return joined;
}
} // namespace kronuz::consensus::transport
