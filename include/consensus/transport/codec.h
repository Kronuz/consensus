#pragma once
#include <algorithm>
#include <consensus/types.h>
#include <consensus/membership.h>
#include <journal/codec.h>
#include <limits>
#include <type_traits>

namespace kronuz::consensus::transport {
inline std::size_t control_frame_bound(kronuz::consensus::Limits limits) {
	constexpr auto maximum = std::numeric_limits<std::size_t>::max();
	if (limits.rpc_bytes > maximum - 68 || limits.rpc_entries > (maximum - 68 - limits.rpc_bytes) / 32)
		throw std::invalid_argument("control frame bound overflow");
	return 68 + 32 * limits.rpc_entries + limits.rpc_bytes;
}
// Versioned, bounded binary control messages. TLS admission supplies identity;
// decoded bytes never create an authenticated peer capability.
inline std::string encode(const kronuz::consensus::Message &message,
						  kronuz::consensus::Limits limits = {}) {
	using namespace kronuz::consensus;
	using kronuz::journal::put64;
	std::string bytes("DTC2", 4);
	const auto frame_bound = control_frame_bound(limits);
	auto number = [&](std::uint64_t value) {
		if (bytes.size() > frame_bound || 8 > frame_bound - bytes.size())
			throw std::invalid_argument("control frame bound");
		put64(bytes, value);
	};
	std::visit(
		[&](const auto &value) {
			using T = std::decay_t<decltype(value)>;
			// Stable wire tags are independent of the variant's declaration order.
			if constexpr (std::is_same_v<T, VoteRequest>)
				number(1);
			else if constexpr (std::is_same_v<T, VoteResponse>)
				number(2);
			else if constexpr (std::is_same_v<T, AppendRequest>)
				number(3);
			else if constexpr (std::is_same_v<T, AppendResponse>)
				number(4);
			else if constexpr (std::is_same_v<T, SnapshotResponse>)
				number(5);
			else if constexpr (std::is_same_v<T, PreVoteRequest>)
				number(6);
			else if constexpr (std::is_same_v<T, PreVoteResponse>)
				number(7);
			else
				static_assert(!sizeof(T), "missing control wire tag");
			number(value.term);
			if constexpr (std::is_same_v<T, VoteRequest>) {
				number(value.last_index);
				number(value.last_term);
				number(value.transfer);
			} else if constexpr (std::is_same_v<T, PreVoteRequest>) {
				number(value.last_index);
				number(value.last_term);
			} else if constexpr (std::is_same_v<T, PreVoteResponse>) {
				number(value.request_term);
				number(value.granted);
			} else if constexpr (std::is_same_v<T, VoteResponse>) {
				number(value.granted);
			} else if constexpr (std::is_same_v<T, AppendRequest>) {
				number(value.rpc);
				number(value.previous);
				number(value.previous_term);
				number(value.commit);
				number(value.read_probe);
				if (value.entries.size() > limits.rpc_entries)
					throw std::invalid_argument("entry count bound");
				number(value.entries.size());
				std::size_t payload_bytes = 0;
				for (const auto &entry : value.entries) {
					if (static_cast<unsigned>(entry.kind) > static_cast<unsigned>(EntryKind::Configuration))
						throw std::invalid_argument("invalid control entry kind");
					number(entry.index);
					number(entry.term);
					number(static_cast<unsigned>(entry.kind));
					if (entry.payload.size() > limits.command_bytes ||
						entry.payload.size() > limits.rpc_bytes - payload_bytes)
						throw std::invalid_argument("command bound");
					number(entry.payload.size());
					payload_bytes += entry.payload.size();
					if (entry.payload.size() > frame_bound - bytes.size())
						throw std::invalid_argument("control frame bound");
					bytes += entry.payload;
				}
			} else if constexpr (std::is_same_v<T, AppendResponse>) {
				number(value.rpc);
				number(value.success);
				number(value.matched);
				number(value.next_hint);
				number(value.read_probe);
				number(value.compacted.has_value());
				if (value.compacted) {
					number(value.compacted->index);
					number(value.compacted->term);
				}
			} else if constexpr (std::is_same_v<T, SnapshotResponse>) {
				if (!valid_snapshot_response(value))
					throw std::invalid_argument("invalid snapshot correlation");
				number(value.key.leader_term);
				number(value.key.transfer);
				number(value.key.boundary.index);
				number(value.key.boundary.term);
				number(static_cast<unsigned>(value.result));
			}
		},
		message);
	return bytes;
}
inline kronuz::consensus::Message decode(std::string_view bytes, kronuz::consensus::Limits limits = {}) {
	using namespace kronuz::consensus;
	using kronuz::journal::Corruption;
	using kronuz::journal::get64;
	if (bytes.size() > control_frame_bound(limits) || !bytes.starts_with("DTC2"))
		throw Corruption("invalid control frame");
	bytes.remove_prefix(4);
	auto boolean = [&] {
		auto value = get64(bytes);
		if (value > 1)
			throw Corruption("invalid boolean");
		return value != 0;
	};
	auto kind = get64(bytes);
	auto term = get64(bytes);
	Message result;
	switch (kind) {
	case 1: {
		auto index = get64(bytes);
		auto last_term = get64(bytes);
		result = VoteRequest{term, index, last_term, boolean()};
		break;
	}
	case 6: {
		auto index = get64(bytes);
		auto last_term = get64(bytes);
		result = PreVoteRequest{term, index, last_term};
		break;
	}
	case 7: {
		auto request_term = get64(bytes);
		result = PreVoteResponse{term, request_term, boolean()};
		break;
	}
	case 2:
		result = VoteResponse{term, boolean()};
		break;
	case 3: {
		AppendRequest value{term, get64(bytes), get64(bytes), get64(bytes), get64(bytes), get64(bytes), {}};
		auto count = get64(bytes);
		// A count is validated against both policy and remaining framing before allocation.
		if (count > limits.rpc_entries || count > bytes.size() / 32)
			throw Corruption("entry count bound");
		value.entries.reserve(static_cast<std::size_t>(count));
		std::size_t payload_bytes = 0;
		for (std::uint64_t i = 0; i < count; ++i) {
			auto index = get64(bytes), entry_term = get64(bytes), entry_kind = get64(bytes),
				 size = get64(bytes);
			if (entry_kind > static_cast<unsigned>(EntryKind::Configuration) || size > limits.command_bytes ||
				size > bytes.size() || size > limits.rpc_bytes - payload_bytes)
				throw Corruption("invalid control entry");
			auto payload = bytes.substr(0, static_cast<std::size_t>(size));
			payload_bytes += payload.size();
			bytes.remove_prefix(payload.size());
			if (entry_kind == static_cast<unsigned>(EntryKind::Configuration))
				(void)decode_membership(payload, limits);  // throws Corruption
			value.entries.push_back(
				{index, entry_term, static_cast<EntryKind>(entry_kind), std::string(payload)});
		}
		result = std::move(value);
		break;
	}
	case 4: {
		auto rpc = get64(bytes);
		auto success = boolean();
		auto matched = get64(bytes), hint = get64(bytes), probe = get64(bytes);
		AppendResponse value{term, rpc, success, matched, hint, probe, {}};
		if (boolean()) {
			auto index = get64(bytes), boundary_term = get64(bytes);
			value.compacted = LogBoundary{index, boundary_term};
		}
		result = value;
		break;
	}
	case 5: {
		SnapshotKey key{get64(bytes), get64(bytes), {get64(bytes), get64(bytes)}};
		auto outcome = get64(bytes);
		if (outcome > static_cast<unsigned>(SnapshotReply::Rejected))
			throw Corruption("invalid snapshot response");
		SnapshotResponse value{term, key, static_cast<SnapshotReply>(outcome)};
		if (!valid_snapshot_response(value))
			throw Corruption("invalid snapshot correlation");
		result = value;
		break;
	}
	default:
		throw Corruption("unknown control kind");
	}
	if (!bytes.empty())
		throw Corruption("trailing control bytes");
	return result;
}
} // namespace kronuz::consensus::transport
