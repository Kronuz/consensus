#pragma once
#include <consensus/snapshot_transfer.h>
#include <functional>
#include <journal/codec.h>
#include <optional>
#include <string>
#include <system_error>

namespace kronuz::consensus::transport {
// The fetch lane (docs/mechanisms/peer-fetch.md): a voter asks a peer for a name and gets the
// bytes the peer's reader holds, or not-found. Its frames ride the bulk lane beside snapshot
// transfer frames, one per lane frame, told apart by their magic.
//
// A host's reader: a name's bytes, or nullopt when it holds none. It runs on the owner executor,
// so its run time adds to consensus latency, and gets the peer's name unchecked.
using FetchReader = std::function<std::optional<std::string>(std::string_view name)>;
// A fetch's outcome, on the owner executor: an error when no answer came, else the peer's bytes,
// or nullopt when the peer holds none.
using FetchDone = std::function<void(std::error_code error, std::optional<std::string> bytes)>;

enum class FetchKind : std::uint8_t { Request = 1, Data, Missing, Failed };
// Request carries the name; Data a chunk of a reply of `total` bytes, in order; Missing and
// Failed (the peer's reader threw, or the reply exceeds its fetch_bytes) end a reply.
struct FetchFrame {
	FetchKind kind = FetchKind::Request;
	std::uint64_t id = 0, total = 0;
	std::string_view body;
};
inline constexpr std::string_view fetch_magic{"DTFT0001", 8};
inline constexpr std::size_t fetch_header_bytes = 28;
inline constexpr std::size_t fetch_chunk_bytes = kronuz::consensus::maximum_transfer_bytes - fetch_header_bytes;

inline bool is_fetch_frame(std::string_view bytes) noexcept { return bytes.starts_with(fetch_magic); }
inline bool valid_fetch(const FetchFrame &frame) noexcept {
	if (!frame.id || frame.body.size() > fetch_chunk_bytes)
		return false;
	switch (frame.kind) {
	case FetchKind::Request:
		return !frame.total && !frame.body.empty();
	case FetchKind::Data:
		return frame.body.size() <= frame.total && (frame.total ? !frame.body.empty() : true);
	case FetchKind::Missing:
	case FetchKind::Failed:
		return !frame.total && frame.body.empty();
	}
	return false;
}
inline std::string encode_fetch(const FetchFrame &frame) {
	if (!valid_fetch(frame))
		throw std::invalid_argument("invalid fetch frame");
	std::string bytes;
	bytes.reserve(fetch_header_bytes + frame.body.size());
	bytes.append(fetch_magic);
	bytes.push_back(static_cast<char>(frame.kind));
	bytes.append(3, '\0');
	kronuz::journal::put64(bytes, frame.id);
	kronuz::journal::put64(bytes, frame.total);
	bytes.append(frame.body);
	return bytes;
}
// The frame borrows `bytes` for its body.
inline FetchFrame decode_fetch(std::string_view bytes) {
	using kronuz::journal::Corruption;
	if (bytes.size() < fetch_header_bytes || !is_fetch_frame(bytes) || bytes.substr(9, 3) != std::string_view("\0\0\0", 3))
		throw Corruption("invalid fetch frame");
	FetchFrame frame;
	frame.kind = static_cast<FetchKind>(static_cast<std::uint8_t>(bytes[8]));
	bytes.remove_prefix(12);
	frame.id = kronuz::journal::get64(bytes);
	frame.total = kronuz::journal::get64(bytes);
	frame.body = bytes;
	if (frame.kind < FetchKind::Request || frame.kind > FetchKind::Failed || !valid_fetch(frame))
		throw Corruption("invalid fetch frame fields");
	return frame;
}
} // namespace kronuz::consensus::transport
