#pragma once
#include "admission.h"
#include "deadline.h"
#include <algorithm>
#include <asio/ip/tcp.hpp>
#include <asio/read.hpp>
#include <asio/ssl/stream.hpp>
#include <asio/write.hpp>
#include <cerrno>
#include <fcntl.h>
#include <filesystem>
#include <journal/codec.h>
#include <mutex>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <system_error>
#include <task_asio.hh>
#include <unistd.h>

// Joining (D150, docs/mechanisms/admission.md, Joining): a node with a saved key and no
// certificate joins with the cluster's token v2, which names the cluster and pins its CA's key. It
// pins the CA before its secret goes out; the member it reached asks the asynchronous issuer hook,
// which checks the secret, names the node and signs. The exchange, DTJN0002: the joiner sends the
// 8-byte magic in the clear, then TLS that authenticates neither end; then frames, each a 4-byte
// little-endian length and its bytes:
//   member -> joiner  the offer: cluster id, the CA certificates it trusts (DER)
//   joiner -> member  the request: cluster id, secret, requested node, the caller's detail, a CSR
//   member -> joiner  the answer: fault and reason, node, its certificate (DER)
namespace kronuz::consensus::transport {
inline constexpr std::size_t join_secret_bytes = 32;
inline constexpr std::size_t join_frame_limit = 64 * 1024;  // a frame holds the CAs, or a CSR and the detail
inline constexpr std::size_t join_offer_cas = 16;           // CAs an offer names: a bundle mid-replacement
inline constexpr std::string_view join_magic = "DTJN0002";  // the exchange's, sent before TLS
inline constexpr std::string_view join_token_magic = "DTJT0002";

// Why a join is refused. The member's: the request's cluster is not its own (cluster), the bytes
// are not DTJN0002 (malformed, version); the hook's: the secret (token), or its own reason
// (refused). The joiner's: the token is not version 2 (version) or the member is another cluster's
// (cluster), the member does not hold the token's CA key (ca), or the certificate it issued is
// not this joiner's (issued).
enum class JoinFault : std::uint8_t { token, malformed, refused, ca, issued, version, cluster };
inline constexpr std::size_t join_faults = 7;
inline const char *join_fault_name(JoinFault fault) noexcept {
	static constexpr const char *names[join_faults] = {"token", "malformed", "refused", "ca", "issued", "version", "cluster"};
	return std::size_t(fault) < join_faults ? names[std::size_t(fault)] : "?";
}
struct JoinRefused : std::runtime_error {
	JoinFault fault;
	std::string reason;  // the hook's, when it gave one
	JoinRefused(JoinFault f, const std::string &what, std::string why = {})
		: std::runtime_error("join: " + what), fault(f), reason(std::move(why)) {}
};

// What the cluster hands a joining node: the cluster id, the pin of its CA's key (key_pin), and
// the secret the hook checks. Printed as "DTJT0002" and the three in hex, 168 characters.
struct JoinToken {
	kronuz::consensus::Identity cluster{};
	Fingerprint ca{};
	std::string secret;
	bool operator==(const JoinToken &) const = default;
};
inline constexpr std::size_t join_token_size = 8 + 2 * (16 + 32 + join_secret_bytes);
inline JoinToken fresh_join_token(const kronuz::consensus::Identity &cluster, X509 *ca) {
	JoinToken token{cluster, key_pin(ca), std::string(join_secret_bytes, '\0')};
	if (RAND_bytes(reinterpret_cast<unsigned char *>(token.secret.data()), int(token.secret.size())) != 1)
		throw std::runtime_error("join token entropy");
	return token;
}
inline std::string encode_join_token(const JoinToken &token) {
	if (token.secret.size() != join_secret_bytes || token.cluster == kronuz::consensus::Identity{})
		throw std::invalid_argument("join token: cluster zero, or a secret not of join_secret_bytes");
	static constexpr char hex[] = "0123456789abcdef";
	std::string text(join_token_magic);
	for (std::string_view bytes : {std::string_view(token.cluster.data(), token.cluster.size()),
								   std::string_view(reinterpret_cast<const char *>(token.ca.data()), token.ca.size()),
								   std::string_view(token.secret)})
		for (unsigned char c : bytes)
			text += {hex[c >> 4], hex[c & 15]};
	return text;
}
// Throws JoinRefused: version for a version 1 token ("DTJT" and 128 hex digits) or another
// version's magic ("DTJT" and four other digits), malformed for anything else not a version 2 token.
inline JoinToken decode_join_token(std::string_view text) {
	const auto digits = [&] {
		return text.size() >= 8 && std::all_of(text.begin() + 4, text.begin() + 8, [](char c) { return c >= '0' && c <= '9'; });
	};
	if (text.starts_with("DTJT") && (text.size() == 4 + 2 * (32 + join_secret_bytes) || (digits() && !text.starts_with(join_token_magic))))
		throw JoinRefused(JoinFault::version, "the token is not version 2");
	if (text.size() != join_token_size || !text.starts_with(join_token_magic))
		throw JoinRefused(JoinFault::malformed, "not a join token");
	std::string bytes;
	for (std::size_t i = join_token_magic.size(); i != text.size(); i += 2) {
		unsigned value = 0;
		const auto [end, error] = std::from_chars(text.data() + i, text.data() + i + 2, value, 16);
		if (error != std::errc{} || end != text.data() + i + 2)
			throw JoinRefused(JoinFault::malformed, "a join token's hex");
		bytes += char(value);
	}
	JoinToken token;
	std::copy_n(bytes.begin(), token.cluster.size(), token.cluster.begin());
	std::copy_n(bytes.begin() + token.cluster.size(), token.ca.size(), token.ca.begin());
	token.secret = bytes.substr(token.cluster.size() + token.ca.size());
	if (token.cluster == kronuz::consensus::Identity{})
		throw JoinRefused(JoinFault::malformed, "a join token of cluster zero");
	return token;
}
// What a cluster keeps of a token: the secret's SHA-256, compared in constant time.
inline Fingerprint join_secret_digest(std::string_view secret) {
	Fingerprint digest{};
	unsigned length = 0;
	if (EVP_Digest(secret.data(), secret.size(), digest.data(), &length, EVP_sha256(), nullptr) != 1 || length != digest.size())
		throw std::runtime_error("join: secret digest");
	return digest;
}
inline bool join_secret_matches(const Fingerprint &digest, std::string_view secret) {
	const auto presented = join_secret_digest(secret);
	return CRYPTO_memcmp(presented.data(), digest.data(), digest.size()) == 0;
}

// The exchange's three frames. Each decoder throws JoinRefused (malformed) for bytes that are not
// that frame; each encoder std::invalid_argument for a frame its decoder would refuse.
struct JoinOffer {
	kronuz::consensus::Identity cluster{};
	std::vector<std::string> cas;  // DER, 1 to join_offer_cas
	bool operator==(const JoinOffer &) const = default;
};
struct JoinRequest {
	kronuz::consensus::Identity cluster{};
	std::string secret;                       // at most 255 bytes
	kronuz::consensus::NodeId requested = 0;  // 0: the hook's choice
	std::string detail;                       // the caller's (a name, index paths), for the hook
	std::string csr;                          // DER, signed by the joiner's key
	Fingerprint key{};  // not on the wire: the CSR key's pin, set once its signature verified
	bool operator==(const JoinRequest &) const = default;
};
struct JoinAnswer {
	std::optional<JoinFault> fault;  // none: issued
	std::string reason;              // at most 255 bytes
	kronuz::consensus::NodeId node = 0;
	std::string certificate;  // DER; empty when refused
	bool operator==(const JoinAnswer &) const = default;
};
inline JoinAnswer join_refusal(JoinFault fault, std::string reason = {}) { return {fault, std::move(reason), 0, {}}; }

namespace join_detail {
inline void need(bool ok, const char *what) {
	if (!ok)
		throw JoinRefused(JoinFault::malformed, what);
}
inline std::string_view take(std::string_view &bytes, std::size_t n, const char *what) {
	need(bytes.size() >= n, what);
	const auto r = bytes.substr(0, n);
	bytes.remove_prefix(n);
	return r;
}
inline kronuz::consensus::Identity take_cluster(std::string_view &bytes) {
	kronuz::consensus::Identity cluster{};
	const auto b = take(bytes, cluster.size(), "a short frame");
	std::copy(b.begin(), b.end(), cluster.begin());
	need(cluster != kronuz::consensus::Identity{}, "cluster zero");
	return cluster;
}
inline std::uint64_t take64(std::string_view &bytes) {
	auto b = take(bytes, 8, "a short frame");
	return kronuz::journal::get64(b);
}
inline std::uint32_t take32(std::string_view &bytes) {
	auto b = take(bytes, 4, "a short frame");
	return kronuz::journal::get32(b);
}
} // namespace join_detail

inline std::string encode_join_offer(const JoinOffer &offer) {
	if (offer.cluster == kronuz::consensus::Identity{} || offer.cas.empty() || offer.cas.size() > join_offer_cas)
		throw std::invalid_argument("join offer: cluster zero, or no CA, or too many");
	std::string bytes(offer.cluster.data(), offer.cluster.size());
	bytes += char(offer.cas.size());
	for (const auto &ca : offer.cas) {
		if (ca.empty())
			throw std::invalid_argument("join offer: an empty CA");
		kronuz::journal::put32(bytes, std::uint32_t(ca.size()));
		bytes += ca;
	}
	if (bytes.size() > join_frame_limit)
		throw std::invalid_argument("join offer: past join_frame_limit");
	return bytes;
}
inline JoinOffer decode_join_offer(std::string_view bytes) {
	using namespace join_detail;
	JoinOffer offer;
	offer.cluster = take_cluster(bytes);
	const auto n = std::uint8_t(take(bytes, 1, "a short offer")[0]);
	need(n && n <= join_offer_cas, "an offer's CA count");
	for (unsigned i = 0; i != n; ++i) {
		const auto size = take32(bytes);
		need(size, "an empty CA");
		offer.cas.emplace_back(take(bytes, size, "a short CA"));
	}
	need(bytes.empty(), "bytes past the offer");
	return offer;
}
inline std::string encode_join_request(const JoinRequest &request) {
	if (request.cluster == kronuz::consensus::Identity{} || request.secret.size() > 255 || request.csr.empty())
		throw std::invalid_argument("join request: cluster zero, a secret past 255 bytes, or no CSR");
	std::string bytes(request.cluster.data(), request.cluster.size());
	bytes += char(request.secret.size());
	bytes += request.secret;
	kronuz::journal::put64(bytes, request.requested);
	kronuz::journal::put32(bytes, std::uint32_t(request.detail.size()));
	bytes += request.detail;
	bytes += request.csr;
	if (bytes.size() > join_frame_limit)
		throw std::invalid_argument("join request: past join_frame_limit");
	return bytes;
}
inline JoinRequest decode_join_request(std::string_view bytes) {
	using namespace join_detail;
	JoinRequest request;
	request.cluster = take_cluster(bytes);
	const auto secret = std::uint8_t(take(bytes, 1, "a short request")[0]);
	request.secret = take(bytes, secret, "a short secret");
	request.requested = take64(bytes);
	const auto detail = take32(bytes);
	request.detail = take(bytes, detail, "a short detail");
	need(!bytes.empty(), "no CSR");
	request.csr = bytes;
	return request;
}
inline std::string encode_join_answer(const JoinAnswer &answer) {
	const bool issued = !answer.fault;
	if (answer.reason.size() > 255 || (issued ? !answer.node || answer.certificate.empty()
											  : std::size_t(*answer.fault) >= join_faults || answer.node || !answer.certificate.empty()))
		throw std::invalid_argument("join answer: a refusal with a node or certificate, or an issue without");
	std::string bytes(1, char(issued ? 0 : 1 + int(*answer.fault)));
	bytes += char(answer.reason.size());
	bytes += answer.reason;
	kronuz::journal::put64(bytes, answer.node);
	bytes += answer.certificate;
	return bytes;
}
inline JoinAnswer decode_join_answer(std::string_view bytes) {
	using namespace join_detail;
	JoinAnswer answer;
	const auto fault = std::uint8_t(take(bytes, 1, "a short answer")[0]);
	need(fault <= join_faults, "an unknown fault");
	if (fault)
		answer.fault = JoinFault(fault - 1);
	const auto reason = std::uint8_t(take(bytes, 1, "a short answer")[0]);
	answer.reason = take(bytes, reason, "a short reason");
	answer.node = take64(bytes);
	answer.certificate = bytes;
	need(answer.fault ? !answer.node && answer.certificate.empty() : answer.node && !answer.certificate.empty(),
		 "a refusal with a node or certificate, or an issue without");
	return answer;
}

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
// A joiner's CSR and its key, the key only when it is EC P-256, the type fresh_join_key makes:
// decided before any signature work, so a request costs at most one P-256 verification
// (docs/mechanisms/admission.md, The join pool).
struct Csr {
	Request request;
	Key key;
};
inline Csr csr_of(std::string_view bytes) {
	Csr csr{request_of(bytes), nullptr};
	EVP_PKEY *key = csr.request ? X509_REQ_get0_pubkey(csr.request.get()) : nullptr;
	char group[32];
	std::size_t n = 0;
	if (key && EVP_PKEY_get_base_id(key) == EVP_PKEY_EC && EVP_PKEY_get_group_name(key, group, sizeof group, &n) == 1 &&
		std::string_view(group, n) == "prime256v1" && EVP_PKEY_up_ref(key) == 1)
		csr.key.reset(key);
	return csr;
}
// Whether the CSR's own key signed it.
inline bool verifies(const Csr &csr) { return csr.key && X509_REQ_verify(csr.request.get(), csr.key.get()) == 1; }
// The key a CSR (DER) is signed by, or null when it is not a P-256 CSR its own key signed.
inline Key signed_key(std::string_view bytes) {
	auto csr = csr_of(bytes);
	return verifies(csr) ? std::move(csr.key) : nullptr;
}
inline Key key_of(std::string_view pem) {
	std::unique_ptr<BIO, decltype(&BIO_free)> bio(BIO_new_mem_buf(pem.data(), int(pem.size())), BIO_free);
	return Key(bio ? PEM_read_bio_PrivateKey(bio.get(), nullptr, nullptr, nullptr) : nullptr);
}
template <class Write> std::string pem(Write write) {
	std::unique_ptr<BIO, decltype(&BIO_free)> bio(BIO_new(BIO_s_mem()), BIO_free);
	if (!bio || write(bio.get()) != 1)
		throw std::runtime_error("join: PEM encoding");
	char *data = nullptr;
	const long n = BIO_get_mem_data(bio.get(), &data);
	return std::string(data, std::size_t(n));
}
// Whether `leaf` chains to `ca` alone, through the intermediates in `chain`, each valid now.
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
template <class S> task::task<void> write_bytes(S &stream, const std::string &bytes) {
	auto [error, n] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
		[&](auto h) { asio::async_write(stream, asio::buffer(bytes), std::move(h)); });
	if (error)
		throw std::system_error(error, "join: write");
}
template <class S> task::task<std::string> read_bytes(S &stream, std::size_t size) {
	std::string bytes(size, '\0');
	auto [error, n] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
		[&](auto h) { asio::async_read(stream, asio::buffer(bytes), std::move(h)); });
	if (error)
		throw std::system_error(error, "join: read");
	co_return bytes;
}
inline task::task<void> write_frame(Stream &stream, std::string bytes) {
	std::string frame;
	kronuz::journal::put32(frame, std::uint32_t(bytes.size()));
	frame += bytes;
	co_await write_bytes(stream, frame);
}
inline task::task<std::string> read_frame(Stream &stream) {
	const auto length = co_await read_bytes(stream, 4);
	std::string_view view(length);
	const auto size = kronuz::journal::get32(view);
	if (!size || size > join_frame_limit)
		throw JoinRefused(JoinFault::malformed, "a frame of " + std::to_string(size) + " bytes");
	co_return co_await read_bytes(stream, size);
}
inline task::task<void> handshake(Stream &stream, asio::ssl::stream_base::handshake_type type) {
	const auto error = co_await task::await_asio<void(asio::error_code)>([&](auto h) { stream.async_handshake(type, std::move(h)); });
	if (error)
		throw std::system_error(error, "join: TLS handshake");
}
inline void close(Stream &stream) {
	asio::error_code ignored;
	stream.lowest_layer().close(ignored);
}

// Writes `bytes` whole to `<name>.new` in `directory`, syncs it and renames it over `name`, so a
// crash leaves the old file or the new one, never a part; `secret` creates it owner-only. Throws
// std::system_error naming the file.
inline std::string replace_file(const std::filesystem::path &directory, const char *name, const std::string &bytes, bool secret) {
	const auto path = directory / name, staged = directory / (std::string(name) + ".new");
	const auto fail = [&](const char *what, int fd = -1) {
		const int error = errno ? errno : EIO;
		if (fd >= 0)
			::close(fd);
		::unlink(staged.c_str());
		throw std::system_error(error, std::generic_category(), std::string("join: ") + what + " " + path.string());
	};
	errno = 0;
	::unlink(staged.c_str());  // a failed save's
	const int fd = ::open(staged.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, secret ? 0600 : 0666);
	if (fd < 0)
		fail("create");
	for (std::size_t done = 0; done != bytes.size();) {
		errno = 0;
		const auto n = ::write(fd, bytes.data() + done, bytes.size() - done);
		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			fail("write", fd);
		done += std::size_t(n);
	}
	if (::fsync(fd) != 0)
		fail("sync", fd);
	if (::close(fd) != 0)
		fail("close");
	if (::rename(staged.c_str(), path.c_str()) != 0)
		fail("rename");
	return path.string();
}
// Makes the renames in `directory` durable.
inline void sync_directory(const std::filesystem::path &directory) {
	const int fd = ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	if (fd < 0 || ::fsync(fd) != 0) {
		const int error = errno;
		if (fd >= 0)
			::close(fd);
		throw std::system_error(error, std::generic_category(), "join: sync " + directory.string());
	}
	::close(fd);
}
} // namespace join_detail

// The asynchronous issuer hook a member asks for each well-formed request of its cluster, on the
// loop serving it: the answer, issued or refused (it may also throw JoinRefused). It must be
// idempotent per key (request.key): a retry with the same key, after a lost answer or the
// joiner's crash, gets the same node. Issuer::issue is one, for the node holding the CA key.
using JoinHook = std::function<task::task<JoinAnswer>(JoinRequest)>;

struct JoinOptions {
	// From the pool's taking a joiner until its request arrived (magic, handshake, offer,
	// request), then until the answer is written (the hook and its relay): a stalled joiner frees
	// its slot.
	std::chrono::milliseconds slot{10000}, answer{10000};
	// Joiners served at once, and from one address: past either, a joiner is refused, never a
	// slot evicted. Each address opens `rate` a second, `rate` at once; past it a joiner waits.
	std::size_t pool = 64, per_source = 8;
	std::uint32_t rate = 10;
};

// A member's side of the exchange, on the transport's port (Links hands it each connection whose
// first byte is the magic's): any member serves joins of its cluster in its own pool, outside the
// handshakes', its offer naming the CAs its TLS context trusts, and the hook answers each request.
// Safe from several loops at once.
class JoinService {
  public:
	// `context` is the member's own transport context, whose leaf chains to the CAs it trusts.
	// Throws std::invalid_argument for cluster zero, a context that trusts no CA, no hook, or a
	// zero pool, per-source cap or rate.
	JoinService(std::shared_ptr<asio::ssl::context> context, kronuz::consensus::Identity cluster, JoinHook hook,
				JoinOptions options = {})
		: cluster_(cluster), hook_(std::move(hook)), options_(options) {
		if (cluster_ == kronuz::consensus::Identity{} || !hook_ || !options_.pool || !options_.per_source || !options_.rate)
			throw std::invalid_argument("join service: cluster zero, no hook, or a zero pool, cap or rate");
		reload(std::move(context));
	}
	const kronuz::consensus::Identity &cluster() const noexcept { return cluster_; }
	const JoinOptions &options() const noexcept { return options_; }
	// The member's context from now on (HostTransport::reload): its CAs are the offer's.
	void reload(std::shared_ptr<asio::ssl::context> context) {
		if (!context)
			throw std::invalid_argument("join service: no context");
		JoinOffer offer{cluster_, {}};
		std::unique_ptr<STACK_OF(X509), void (*)(STACK_OF(X509) *)> trusted(
			X509_STORE_get1_all_certs(SSL_CTX_get_cert_store(context->native_handle())),
			[](STACK_OF(X509) *certificates) { sk_X509_pop_free(certificates, X509_free); });
		for (int i = 0; trusted && i < sk_X509_num(trusted.get()) && offer.cas.size() < join_offer_cas; ++i)
			offer.cas.push_back(join_detail::der(sk_X509_value(trusted.get(), i)));
		if (offer.cas.empty())
			throw std::invalid_argument("join service: the context trusts no CA");
		auto bytes = encode_join_offer(offer);
		std::lock_guard hold(mutex_);
		context_ = std::move(context);
		offer_ = std::move(bytes);
	}
	// Joins refused at this member for `fault`, the hook's refusals included.
	std::uint64_t refused(JoinFault fault) const { return refused_[std::size_t(fault)].load(std::memory_order_relaxed); }
	// Joiners refused at the pool's caps, delayed past their address's rate, and cut at the slot
	// deadline; serving now.
	std::uint64_t busy() const { return busy_.load(std::memory_order_relaxed); }
	std::uint64_t delayed() const { return delayed_.load(std::memory_order_relaxed); }
	std::uint64_t cut() const { return cut_.load(std::memory_order_relaxed); }
	// CSR signatures verified: only P-256 requests reach one.
	std::uint64_t verified() const { return verified_.load(std::memory_order_relaxed); }
	std::size_t serving() const {
		std::lock_guard hold(mutex_);
		return serving_;
	}

	// One joiner on an accepted `socket` whose magic is unread, on `loop`, until `stop`: the node
	// it became, or 0 when it was refused (the joiner reads why), failed or cut.
	task::task<kronuz::consensus::NodeId> serve(asio::ip::tcp::socket socket, task::scheduler_ref loop,
												 task::cancel_token stop = {}) {
		using namespace join_detail;
		asio::error_code error;
		auto source = socket.remote_endpoint(error).address();
		if (source.is_v6() && source.to_v6().is_v4_mapped())
			source = asio::ip::make_address_v4(asio::ip::v4_mapped, source.to_v6());
		const auto delay = error ? std::nullopt : take(source);
		if (!delay) {
			busy_.fetch_add(1, std::memory_order_relaxed);
			socket.close(error);
			co_return 0;
		}
		Slot held(*this, source);
		std::shared_ptr<asio::ssl::context> context;
		std::string offer;
		{
			std::lock_guard hold(mutex_);
			context = context_;
			offer = offer_;
		}
		Stream stream(std::move(socket), *context);
		SSL_set_verify(stream.native_handle(), SSL_VERIFY_NONE, nullptr);  // a joiner has no certificate yet
		task::cancel_callback<Bounded::Close> stopped(stop, Bounded::Close{&stream});
		JoinAnswer answer;
		JoinRequest request;
		bool requested = false;
		try {
			{
				Bounded slot(loop, options_.slot, stream);
				try {
					if (delay->count()) {
						delayed_.fetch_add(1, std::memory_order_relaxed);
						late_deadline rate(loop, *delay);
						co_await rate.wait();
					}
					const auto magic = co_await read_bytes(stream.next_layer(), join_magic.size());
					if (magic != join_magic)
						throw JoinRefused(magic.starts_with("DTJN") ? JoinFault::version : JoinFault::malformed, "the magic");
					co_await handshake(stream, asio::ssl::stream_base::server);
					co_await write_frame(stream, offer);
					const auto bytes = co_await read_frame(stream);
					requested = true;
					request = decode_join_request(bytes);
					const auto csr = csr_of(request.csr);
					if (!csr.key)
						throw JoinRefused(JoinFault::malformed, "a request whose key is not P-256");
					verified_.fetch_add(1, std::memory_order_relaxed);
					if (!verifies(csr))
						throw JoinRefused(JoinFault::malformed, "an unsigned request");
					request.key = key_pin(csr.key.get());
					if (request.cluster != cluster_)
						throw JoinRefused(JoinFault::cluster, "a request for another cluster");
				} catch (const JoinRefused &e) {
					if (!requested)
						throw;
					answer = join_refusal(e.fault);
				} catch (...) {
					if (slot.deadline.state() == late_deadline::outcome::expired)
						cut_.fetch_add(1, std::memory_order_relaxed);
					throw;
				}
			}
			Bounded bound(loop, options_.answer, stream);
			if (!answer.fault) {
				try {
					answer = co_await hook_(std::move(request));
					(void)encode_join_answer(answer);  // a hook's answer the joiner could not read is a refusal
				} catch (const JoinRefused &e) {
					answer = join_refusal(e.fault, e.reason);
				} catch (const std::exception &) {
					answer = join_refusal(JoinFault::refused, "the issuer failed");
				}
			}
			if (answer.fault)
				count(*answer.fault);
			co_await write_frame(stream, encode_join_answer(answer));
		} catch (const JoinRefused &e) {
			count(e.fault);
			answer = join_refusal(e.fault);
		} catch (...) {
			answer = join_refusal(JoinFault::malformed);
		}
		close(stream);
		co_return answer.fault ? 0 : answer.node;
	}

  private:
	struct Source {
		double tokens = 0;
		std::chrono::steady_clock::time_point last;
		std::size_t open = 0;
	};
	// Holds a joiner's place in the pool until it ends.
	struct Slot {
		JoinService &service;
		asio::ip::address source;
		Slot(JoinService &s, asio::ip::address a) : service(s), source(std::move(a)) {}
		Slot(const Slot &) = delete;
		~Slot() {
			std::lock_guard hold(service.mutex_);
			--service.serving_;
			--service.sources_[source].open;
		}
	};
	// A place in the pool for a joiner from `source`, and how long its address's rate delays it;
	// none when the pool or the address is full.
	std::optional<std::chrono::nanoseconds> take(const asio::ip::address &source) {
		const auto now = std::chrono::steady_clock::now();
		std::lock_guard hold(mutex_);
		if (sources_.size() > 4 * options_.pool)  // addresses idle past a second hold full buckets
			std::erase_if(sources_, [&](const auto &e) { return !e.second.open && now - e.second.last > std::chrono::seconds(1); });
		if (serving_ >= options_.pool)
			return std::nullopt;
		const double rate = options_.rate;
		auto [it, fresh] = sources_.try_emplace(source, Source{rate, now, 0});
		auto &s = it->second;
		if (s.open >= options_.per_source)
			return std::nullopt;
		s.tokens = std::min(rate, s.tokens + std::chrono::duration<double>(now - s.last).count() * rate);
		s.last = now;
		s.tokens -= 1;
		++s.open;
		++serving_;
		return std::chrono::nanoseconds(s.tokens < 0 ? std::int64_t(-s.tokens / rate * 1e9) : 0);
	}
	void count(JoinFault fault) { refused_[std::size_t(fault)].fetch_add(1, std::memory_order_relaxed); }

	kronuz::consensus::Identity cluster_;
	JoinHook hook_;
	JoinOptions options_;
	mutable std::mutex mutex_;
	std::shared_ptr<asio::ssl::context> context_;
	std::string offer_;
	std::size_t serving_ = 0;
	std::map<asio::ip::address, Source> sources_;
	std::atomic<std::uint64_t> refused_[join_faults] = {};
	std::atomic<std::uint64_t> busy_{0}, delayed_{0}, cut_{0}, verified_{0};
};

// The node holding the CA key (the root's leader, in Xapiand) signs. Its `assign` hook checks the
// secret and names the node a key joins as, asynchronously and idempotently per key; the issuer
// signs once per key and node, so a retry within `retry` gets the same certificate.
class Issuer {
  public:
	struct Assignment {
		kronuz::consensus::NodeId node = 0;  // 0: refused for `fault`
		JoinFault fault = JoinFault::refused;
		std::string reason;
	};
	using Assign = std::function<task::task<Assignment>(const JoinRequest &)>;
	// `prefix` is the CA policy's (ClusterCa::prefix()). Throws std::invalid_argument when the key
	// is not the CA's, the prefix is empty, or the hook is missing.
	Issuer(X509 *ca, EVP_PKEY *ca_key, std::string prefix, Assign assign, std::chrono::hours valid = std::chrono::hours(24 * 365),
		   std::chrono::minutes retry = std::chrono::minutes(10))
		: ca_(X509_dup(ca)), prefix_(std::move(prefix)), assign_(std::move(assign)), valid_(valid), retry_(retry) {
		if (!ca_ || !ca_key || X509_check_private_key(ca_.get(), ca_key) != 1 || prefix_.empty() || !assign_)
			throw std::invalid_argument("issuer: CA key, prefix or assign hook");
		EVP_PKEY_up_ref(ca_key);
		key_.reset(ca_key);
	}
	// Certificates signed so far: one per key and node within `retry`.
	std::uint64_t signatures() const { return signatures_.load(std::memory_order_relaxed); }
	// CSR signatures verified: only for P-256 requests `assign` admitted.
	std::uint64_t verified() const { return verified_.load(std::memory_order_relaxed); }

	// The answer for a request: the hook's node and its certificate, or the refusal. A JoinHook.
	task::task<JoinAnswer> issue(JoinRequest request) {
		auto csr = join_detail::csr_of(request.csr);
		if (!csr.key)
			co_return join_refusal(JoinFault::malformed);
		request.key = key_pin(csr.key.get());
		const auto assignment = co_await assign_(request);  // the secret, before any signature work
		if (!assignment.node)
			co_return join_refusal(assignment.fault, assignment.reason);
		verified_.fetch_add(1, std::memory_order_relaxed);
		if (!join_detail::verifies(csr))
			co_return join_refusal(JoinFault::malformed);
		const auto key = std::move(csr.key);
		std::lock_guard hold(mutex_);
		const auto now = std::chrono::steady_clock::now();
		std::erase_if(recent_, [&](const auto &entry) { return now - entry.second.at > retry_; });
		auto &recent = recent_[request.key];
		if (recent.node != assignment.node || recent.certificate.empty()) {
			recent = {assignment.node, join_detail::der(sign(assignment.node, key.get()).get()), now};
			signatures_.fetch_add(1, std::memory_order_relaxed);
		}
		co_return JoinAnswer{std::nullopt, {}, recent.node, recent.certificate};
	}

  private:
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

	struct Recent {
		kronuz::consensus::NodeId node = 0;
		std::string certificate;
		std::chrono::steady_clock::time_point at;
	};
	join_detail::Certificate ca_;
	join_detail::Key key_;
	std::string prefix_;
	Assign assign_;
	std::chrono::hours valid_;
	std::chrono::minutes retry_;
	std::mutex mutex_;
	std::map<Fingerprint, Recent> recent_;  // what was signed within `retry`, by key
	std::atomic<std::uint64_t> signatures_{0}, verified_{0};
};

// A joiner's key, made once and saved (save_join_key) before its first request, so a retry after a
// crash or a lost answer is the same node: P-256, as PEM.
inline std::string fresh_join_key() {
	join_detail::Key key(EVP_EC_gen("P-256"));
	if (!key)
		throw std::runtime_error("join: key");
	return join_detail::pem([&](BIO *b) { return PEM_write_bio_PrivateKey(b, key.get(), nullptr, nullptr, 0, nullptr, nullptr); });
}
// Saves `key` (PEM) as `directory`/node.key, owner-only, by rename.
inline std::string save_join_key(const std::filesystem::path &directory, const std::string &key) {
	std::filesystem::create_directories(directory);
	auto path = join_detail::replace_file(directory, "node.key", key, true);
	join_detail::sync_directory(directory);
	return path;
}

// What a joiner holds once issued: its node, its own key, its certificate and the CA's, as PEM.
struct Joined {
	kronuz::consensus::NodeId node = 0;
	std::string key, certificate, ca;
	// Writes the three files into `directory` and returns the options a Peer context loads them
	// with. Each is written whole to `<name>.new`, synced and renamed over the old one, the key
	// owner-only; the certificate goes last, so a crash before it leaves the saved key and no new
	// certificate, which joins again as the same node. Throws std::system_error naming the file.
	reactor::tls::Options save(const std::filesystem::path &directory) const {
		std::filesystem::create_directories(directory);
		using join_detail::replace_file;
		reactor::tls::Options files;
		files.private_key = replace_file(directory, "node.key", key, true);
		files.certificate_authority = replace_file(directory, "ca.pem", ca, false);
		files.certificate = replace_file(directory, "node.pem", certificate, false);
		join_detail::sync_directory(directory);
		return files;
	}
};

// Joins the cluster through the member at `endpoint` with `token` and the caller's saved `key`
// (PEM), asking to be node `requested` (0: the hook's choice), with `detail` for the hook. The
// member's cluster, and its leaf's chain to the CA the token pins, are checked before the secret
// goes out; the issued certificate against the key and the CA. Throws JoinRefused with the
// fault, or std::system_error when the connection fails or `limit` passes.
inline task::task<Joined> join(asio::any_io_executor executor, task::scheduler_ref loop, asio::ip::tcp::endpoint endpoint,
							   JoinToken token, std::string key_pem, std::string detail = {},
							   kronuz::consensus::NodeId requested = 0,
							   std::chrono::milliseconds limit = std::chrono::milliseconds(10000)) {
	using namespace join_detail;
	if (token.secret.size() > 255)
		throw std::invalid_argument("join: a secret past 255 bytes");
	auto key = key_of(key_pem);
	Request csr(X509_REQ_new());
	std::unique_ptr<X509_NAME, decltype(&X509_NAME_free)> subject(X509_NAME_new(), X509_NAME_free);
	if (!key || !csr || !subject || X509_REQ_set_pubkey(csr.get(), key.get()) != 1 ||
		X509_NAME_add_entry_by_txt(subject.get(), "CN", MBSTRING_ASC, reinterpret_cast<const unsigned char *>("joiner"), -1, -1, 0) != 1 ||
		X509_REQ_set_subject_name(csr.get(), subject.get()) != 1 || X509_REQ_sign(csr.get(), key.get(), EVP_sha256()) <= 0)
		throw std::invalid_argument("join: the saved key, or its request");
	asio::ssl::context tls(asio::ssl::context::tls_client);
	SSL_CTX_set_min_proto_version(tls.native_handle(), TLS1_2_VERSION);
	tls.set_verify_mode(asio::ssl::verify_none);  // the member is checked against the token's CA below
	Stream stream(asio::ip::tcp::socket(executor), tls);
	Bounded bound(loop, limit, stream);
	const auto connected = co_await task::await_asio<void(asio::error_code)>(
		[&](auto h) { stream.lowest_layer().async_connect(endpoint, std::move(h)); });
	if (connected)
		throw std::system_error(connected, "join: connect");
	co_await write_bytes(stream.next_layer(), std::string(join_magic));
	co_await handshake(stream, asio::ssl::stream_base::client);
	const auto offer = decode_join_offer(co_await read_frame(stream));
	if (offer.cluster != token.cluster)
		throw JoinRefused(JoinFault::cluster, "the member is another cluster's");
	Certificate ca;
	for (const auto &bytes : offer.cas)
		if (auto c = certificate_of(bytes); c && key_pin(c.get()) == token.ca) {
			ca = std::move(c);
			break;
		}
	Certificate member(SSL_get1_peer_certificate(stream.native_handle()));
	if (!ca || !member || !chains_to(member.get(), SSL_get_peer_cert_chain(stream.native_handle()), ca.get()))
		throw JoinRefused(JoinFault::ca, "the member does not hold the token's CA");
	co_await write_frame(stream, encode_join_request({token.cluster, token.secret, requested, std::move(detail), der(csr.get()), {}}));
	const auto answer = decode_join_answer(co_await read_frame(stream));
	close(stream);
	if (answer.fault)
		throw JoinRefused(*answer.fault,
						  std::string("refused by the member: ") + join_fault_name(*answer.fault) +
							  (answer.reason.empty() ? std::string() : " (" + answer.reason + ")"),
						  answer.reason);
	auto certificate = certificate_of(answer.certificate);
	if (!certificate || EVP_PKEY_eq(X509_get0_pubkey(certificate.get()), key.get()) != 1 ||
		!chains_to(certificate.get(), nullptr, ca.get()) || ClusterCa::prefix_of(certificate.get(), answer.node).empty())
		throw JoinRefused(JoinFault::issued, "the issued certificate is not this joiner's");
	Joined joined;
	joined.node = answer.node;
	joined.key = std::move(key_pem);
	joined.certificate = pem([&](BIO *b) { return PEM_write_bio_X509(b, certificate.get()); });
	joined.ca = pem([&](BIO *b) { return PEM_write_bio_X509(b, ca.get()); });
	co_return joined;
}
} // namespace kronuz::consensus::transport
