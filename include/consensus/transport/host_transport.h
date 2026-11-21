#pragma once
#include "codec.h"
#include "hello.h"
#include "links.h"
#include "rpc.h"
#include <algorithm>
#include <asio/post.hpp>
#include <asio/steady_timer.hpp>
#include <atomic>
#include <deque>
#include <functional>
#include <journal/host.h>
#include <optional>

// The host transport (docs/mechanisms/host-transport.md): a Control and a Bulk connection per
// peer node, each on the loop its node places on, shared by every group of the two nodes. A
// frame names its group and incarnations; the reader batches frames by the group's loop and
// posts each batch once per read. Senders keep per-group queues and ready rings: control frames
// first, round robin across ready groups, and data frames within each group's credit; a feed
// does O(1) work per frame. A group's frames are admitted per frame (R7) once the peer's group
// open matched on that connection. The RPC lane rides Bulk beside them (docs/mechanisms/rpc-lane.md).
namespace kronuz::consensus::transport {
using GroupId = std::uint64_t;

// What a frame carries: a control or a data message for a group, credit back for data, a
// group's open, or the receiver's refusal of the group on this connection (open it again).
enum class FrameKind : std::uint8_t { control = 1, data = 2, credit = 3, open = 4, closed = 5 };
// The group and the incarnations a message goes from and to.
struct Route {
	GroupId group = 0;
	kronuz::consensus::NodeId source = 0, destination = 0;
};
constexpr std::size_t frame_header = 25;  // kind, then group, source and destination (credit: group, bytes)
constexpr std::size_t open_frame = 89;    // kind, group, then cluster, group identity, epoch and profile
constexpr std::size_t closed_frame = 9;   // kind, group

// What every member of a group agrees on, checked per connection by the group-open exchange
// (async-drivers Q7 (a)): what Peers' per-group hello checks. None of it is membership.
struct GroupOpen {
	kronuz::consensus::Identity cluster{}, group{}, epoch{};
	Fingerprint profile{};
};
// Which field of a peer's open differs from this host's, or null when none does.
inline const char *open_fault(const GroupOpen &local, const GroupOpen &remote) noexcept {
	if (remote.cluster != local.cluster)
		return "cluster";
	if (remote.group != local.group)
		return "group";
	if (remote.epoch != local.epoch)
		return "epoch";
	if (remote.profile != local.profile)
		return "profile";
	return nullptr;
}
inline std::string encode_open(GroupId group, const GroupOpen &open) {
	std::string frame(1, char(FrameKind::open));
	kronuz::journal::put64(frame, group);
	for (auto *id : {&open.cluster, &open.group, &open.epoch})
		frame.append(id->data(), id->size());
	frame.append(reinterpret_cast<const char *>(open.profile.data()), open.profile.size());
	return frame;
}
// An open frame's group and fields; nullopt when it is not one.
inline std::optional<std::pair<GroupId, GroupOpen>> decode_open(std::string_view frame) {
	if (frame.size() != open_frame || frame[0] != char(FrameKind::open))
		return std::nullopt;
	frame.remove_prefix(1);
	std::pair<GroupId, GroupOpen> r;
	r.first = kronuz::journal::get64(frame);
	for (auto *id : {&r.second.cluster, &r.second.group, &r.second.epoch}) {
		std::copy_n(frame.begin(), id->size(), id->begin());
		frame.remove_prefix(id->size());
	}
	std::copy_n(frame.begin(), r.second.profile.size(), r.second.profile.begin());
	return r;
}

// Why a message frame is refused, per R7 (docs/mechanisms/host-transport.md, Identity).
enum class Admission : std::uint8_t { admitted, unstored, unopened, misrouted, spoofed };
// `held` is the group's incarnation on this host (0: not stored), `opened` whether the
// connection's open matched, `node` the connection's authenticated node, and `member_of` the
// member the receiver's membership names for an incarnation. An incarnation it does not name (a
// joiner's leader, a leader a follower lags) is admitted: Core decides by term, incarnation and log.
template <class MemberOf>
Admission admit(const Route &route, kronuz::consensus::NodeId node, kronuz::consensus::NodeId held, bool opened,
				MemberOf &&member_of) {
	if (!held)
		return Admission::unstored;
	if (!opened)
		return Admission::unopened;
	if (route.destination != held)
		return Admission::misrouted;
	if (auto source = member_of(route.group, route.source); source && source->node != node)
		return Admission::spoofed;
	return Admission::admitted;
}
// The member the group's membership names for an incarnation, or nullopt where it names none.
// Called on the group's loop, so loops call it at once for different groups.
using MemberOf = std::function<std::optional<kronuz::consensus::Member>(GroupId, kronuz::consensus::NodeId)>;

// A message frame as the group's loop receives it; `node` is the connection's authenticated node.
struct Received {
	Route route;
	kronuz::consensus::NodeId node = 0;
	bool data = false;
	std::string payload;
};

// The one per-group table of the transport (docs/mechanisms/host-transport.md, Tables): entries
// dense in a vector, found through an open-addressed index of their slots (linear probing,
// load 0.5-0.75, backward-shift deletion). An erase moves the last entry into the hole.
template <class V> class GroupTable {
  public:
	struct Entry {
		GroupId id;
		V value;
	};
	V *find(GroupId id) noexcept {
		const auto i = position(id);
		return i == npos ? nullptr : &entries_[index_[i] - 1].value;
	}
	const V *find(GroupId id) const noexcept { return const_cast<GroupTable *>(this)->find(id); }
	bool contains(GroupId id) const noexcept { return position(id) != npos; }
	// The entry, a value-initialized one when absent; an insert may move every entry.
	V &operator[](GroupId id) {
		if (auto *v = find(id))
			return *v;
		if ((entries_.size() + 1) * 4 > index_.size() * 3)
			rehash((entries_.size() + 1) * 2);
		if (entries_.size() == entries_.capacity())
			entries_.reserve(entries_.size() + entries_.size() / 2 + 4);
		entries_.push_back({id, V{}});
		place(id, std::uint32_t(entries_.size()));
		return entries_.back().value;
	}
	bool erase(GroupId id) {
		auto hole = position(id);
		if (hole == npos)
			return false;
		const std::size_t slot = index_[hole] - 1, n = index_.size();
		for (auto j = next(hole); index_[j]; j = next(j))
			if ((j + n - home(entries_[index_[j] - 1].id)) % n >= (j + n - hole) % n) {
				index_[hole] = index_[j];
				hole = j;
			}
		index_[hole] = 0;
		if (const auto last = entries_.size() - 1; slot != last) {
			entries_[slot] = std::move(entries_[last]);
			index_[position(entries_[slot].id)] = std::uint32_t(slot + 1);
		}
		entries_.pop_back();
		if (entries_.empty()) {
			clear();
		} else if (index_.size() > 16 && entries_.size() * 8 < index_.size()) {
			entries_.shrink_to_fit();
			rehash(entries_.size() * 2);
		}
		return true;
	}
	void clear() noexcept {
		std::vector<Entry>().swap(entries_);
		std::vector<std::uint32_t>().swap(index_);
	}
	std::size_t size() const noexcept { return entries_.size(); }
	bool empty() const noexcept { return entries_.empty(); }
	auto begin() noexcept { return entries_.begin(); }
	auto end() noexcept { return entries_.end(); }

  private:
	static constexpr std::size_t npos = ~std::size_t{0};
	// splitmix64's finalizer, then (h * n) >> 64: an index position without a division.
	std::size_t home(GroupId id) const noexcept {
		id = (id ^ (id >> 30)) * 0xbf58476d1ce4e5b9ull;
		id = (id ^ (id >> 27)) * 0x94d049bb133111ebull;
		id ^= id >> 31;
		return std::size_t((static_cast<unsigned __int128>(id) * index_.size()) >> 64);
	}
	std::size_t next(std::size_t i) const noexcept { return i + 1 == index_.size() ? 0 : i + 1; }
	std::size_t position(GroupId id) const noexcept {
		if (index_.empty())
			return npos;
		for (auto i = home(id);; i = next(i)) {
			if (!index_[i])
				return npos;
			if (entries_[index_[i] - 1].id == id)
				return i;
		}
	}
	void place(GroupId id, std::uint32_t slot) noexcept {
		auto i = home(id);
		while (index_[i])
			i = next(i);
		index_[i] = slot;
	}
	void rehash(std::size_t n) {
		index_.assign(std::max<std::size_t>(n, 8), 0);
		for (std::size_t s = 0; s != entries_.size(); ++s)
			place(entries_[s].id, std::uint32_t(s + 1));
	}
	std::vector<Entry> entries_;
	std::vector<std::uint32_t> index_;  // an entry's slot + 1; 0 is empty
};

// The RPC lane's host-wide pools (D5) and each connection's call table.
struct RpcOptions {
	std::size_t serving_bytes = std::size_t{256} << 20;  // served requests' bodies
	std::size_t calling_bytes = std::size_t{256} << 20;  // this host's calls' responses
	std::size_t outgoing_bytes = std::size_t{64} << 20;  // body bytes queued to send
	rpc::LaneOptions lane;
};
// A call on the lane, on its connection's loop (loop_of_node(node)): one this host made or serves.
struct RpcCall {
	kronuz::consensus::NodeId node = 0;
	std::uint64_t id = 0;
};
// The lane's counters of one loop, readable from any thread; no group's counter moves for it.
struct LaneStats {
	std::atomic<std::uint64_t> calls{0}, served{0}, frames_in{0}, frames_out{0}, bytes_in{0}, bytes_out{0};
	std::atomic<std::uint64_t> outcomes[rpc::outcomes] = {};  // calls ended here, by rpc::Outcome
	std::uint64_t ended(rpc::Outcome o) const noexcept { return outcomes[std::size_t(o)]; }
};

struct HostTransportOptions {
	LinkOptions links;
	RpcOptions rpc;
	std::uint64_t window = 4 << 20;     // data bytes a group may have sent and not yet credited, per connection
	std::size_t control_queue = 64;     // control frames a group queues per connection; more are dropped
	std::uint64_t data_queue = 8 << 20; // data bytes a group queues per connection; more are dropped
	std::size_t feed = 64 * 1024;       // bytes handed to a connection once its queue is out
	std::chrono::milliseconds redial{200};
	MemberOf member_of;  // required: the per-frame rule's source check
};

// Counters of one loop, readable from any thread.
struct HostTransportStats {
	std::atomic<std::uint64_t> links{0}, sent{0}, received{0}, reads{0}, posts{0}, dropped{0}, refused{0}, malformed{0};
	// The group-open exchange and the per-frame rule: opens matched and not, the receiver's
	// refusals that came back (closed), and message frames refused by each Admission.
	std::atomic<std::uint64_t> opens{0}, mismatched{0}, closed{0}, unstored{0}, unopened{0}, misrouted{0}, spoofed{0};
	// Groups a feed examined to choose its frames: one per frame, whatever the group count.
	std::atomic<std::uint64_t> visited{0};
	// A level, not a count: data bytes this loop's connections sent that their peers have not
	// credited yet. Zero once every group's credit is back.
	std::atomic<std::uint64_t> uncredited{0};
};

// A loop of the host: where its tasks run and its sockets wait.
struct HostLoop {
	asio::any_io_executor executor;
	task::scheduler_ref scheduler;
};

// Destroy it after stop(), once drained() on every loop and the loops have stopped running.
class HostTransport {
  public:
	// On the group's loop, for each received message the per-frame rule admitted: false when the
	// group cannot take it now, and the frame is dropped and counted (Raft resends). Its credit
	// returns either way.
	using Deliver = std::function<bool(const Received &)>;
	// On the group's loop: the node limits its peer's hello reported for a source incarnation, once
	// per incarnation and connection, before that source's first admitted frame on it (Core's
	// PeerLimits, which caps what the group's leader sends that member).
	using ReportLimits = std::function<void(GroupId, const kronuz::consensus::PeerLimits &)>;

	// A served call: its events, or none to answer unknown_method. The handler acts on the call
	// from its events, which run on the call's loop; `call` names it to write, consume and stop.
	using Serve = std::function<std::optional<rpc::Events>(RpcCall call, const rpc::Incoming &)>;

	HostTransport(std::vector<HostLoop> loops, std::shared_ptr<asio::ssl::context> tls, const NodePins &pins,
				  kronuz::consensus::Identity cluster, const std::map<kronuz::consensus::NodeId, PeerAddress> &addresses,
				  HostTransportOptions options = {})
		: options_(std::move(options)), serving_(options_.rpc.serving_bytes), calling_(options_.rpc.calling_bytes),
		  outgoing_(options_.rpc.outgoing_bytes), loops_(loops.size()) {
		for (auto &[node, address] : addresses)
			nodes_.push_back(node);
		if (loops_.empty() || loops_.size() > 4096)
			throw std::invalid_argument("host transport: 1 to 4,096 loops");
		if (!options_.member_of)
			throw std::invalid_argument("host transport: member_of is required");
		kronuz::consensus::validate_limits(options_.links.limits);
		if (options_.links.connection.maximum_frame < frame_header + control_frame_bound(options_.links.limits))
			throw std::invalid_argument("host transport: maximum_frame below a frame of this node's rpc_bytes");
		if (options_.links.rpc_max && options_.links.connection.maximum_frame < rpc::frame_bytes)
			throw std::invalid_argument("host transport: maximum_frame below the RPC lane's largest frame");
		for (std::uint32_t k = 0; k != loops_.size(); ++k) {
			auto &loop = loops_[k];
			loop.at = loops[k];
			loop.out.resize(loops_.size());
			loop.in.resize(loops_.size());
			loop.links = std::make_unique<Links>(loop.at.executor, loop.at.scheduler, tls, pins, cluster, addresses, options_.links);
			loop.links->owner = [this](kronuz::consensus::NodeId node) { return loops_[loop_of_node(node)].links.get(); };
			loop.links->on_established = [this, k](const Link &link) { established(k, link); };
			loop.links->on_ended = [this, k](const Link &link) { ended(k, link); };
			loop.links->on_frame = [this, k](const Link &link, std::string_view frame) { arrived(k, link, frame); };
		}
	}
	HostTransport(const HostTransport &) = delete;
	HostTransport &operator=(const HostTransport &) = delete;

	std::uint32_t loops() const noexcept { return std::uint32_t(loops_.size()); }
	std::uint32_t loop_of_group(GroupId group) const noexcept {
		return kronuz::journal::host_detail::loop(kronuz::journal::host_detail::slot(group), loops());
	}
	std::uint32_t loop_of_node(kronuz::consensus::NodeId node) const noexcept {
		return kronuz::journal::host_detail::loop(kronuz::journal::host_detail::slot(node), loops());
	}
	const HostTransportStats &stats(std::uint32_t loop) const { return loops_.at(loop).stats; }
	const LaneStats &lane_stats(std::uint32_t loop) const { return loops_.at(loop).lane_stats; }
	const rpc::ByteBudget &serving_pool() const noexcept { return serving_; }
	const rpc::ByteBudget &calling_pool() const noexcept { return calling_; }
	const rpc::ByteBudget &outgoing_pool() const noexcept { return outgoing_; }
	// Before any frame arrives: what loop `loop` delivers.
	void on_deliver(std::uint32_t loop, Deliver deliver) { loops_.at(loop).deliver = std::move(deliver); }
	// Before any frame arrives: where loop `loop` reports its peers' limits.
	void on_peer_limits(std::uint32_t loop, ReportLimits report) { loops_.at(loop).report = std::move(report); }
	// On `loop`: its connections' counters, hellos refused by version among them.
	const LinkStats &link_stats(std::uint32_t loop) const { return loops_.at(loop).links->stats(); }
	// On the group's loop: the host stores `group` as its incarnation `incarnation`. A peer's
	// frames for it are admitted on a connection once that peer's open matches `open`; storing
	// again replaces both and opens the group again on every connection. A zero cluster, group
	// or epoch is refused, as a hello refused it.
	void store(GroupId group, kronuz::consensus::NodeId incarnation, const GroupOpen &open) {
		if (!incarnation)
			throw std::invalid_argument("host transport: incarnation 0");
		using kronuz::consensus::Identity;
		if (open.cluster == Identity{} || open.group == Identity{} || open.epoch == Identity{})
			throw std::invalid_argument("host transport: a zero identity in a group's open");
		auto &here = loops_[loop_of_group(group)];
		here.stored[group] = {incarnation, {open, ++here.serial}};
		unopen(here, group);
	}
	// On the group's loop: the host no longer stores `group`; its frames are refused, and it
	// queues none (frames already queued on a connection still go).
	void forget(GroupId group) {
		auto &here = loops_[loop_of_group(group)];
		here.stored.erase(group);
		unopen(here, group);
	}

	// Accepts on `acceptor`, a socket of loop 0, until stop().
	void serve(asio::ip::tcp::acceptor &acceptor) { task::spawn(loops_[0].at.scheduler, loops_[0].links->serve(acceptor)); }

	// On `loop`, the group's: queues a message for the member on `node`. A loop's frames go to
	// each connection's loop in one post once the running work yields. A group the host does not
	// store sends nothing.
	void send(std::uint32_t loop, LinkLane lane, FrameKind kind, Route route, kronuz::consensus::NodeId node,
			  std::string_view payload) {
		if (kind == FrameKind::credit)
			throw std::invalid_argument("host transport: credit is the transport's own");
		std::string frame;
		frame.reserve(frame_header + payload.size());
		frame.push_back(char(kind));
		for (auto value : {route.group, route.source, route.destination})
			kronuz::journal::put64(frame, value);
		frame.append(payload);
		auto &here = loops_.at(loop);
		const auto *stored = here.stored.find(route.group);
		if (frame.size() > options_.links.connection.maximum_frame || !stored) {
			++here.stats.dropped;  // no connection carries it, or no open
			return;
		}
		const auto to = loop_of_node(node);
		here.out[to].push_back({node, lane, kind, route.group, std::move(frame), stored->open});
		if (!here.flushing) {
			here.flushing = true;
			asio::post(here.at.executor, [this, loop] { flush(loop); });
		}
	}

	// Before serve(): the handler of `method` (D6). Calls of a method with none end unknown_method.
	void handle(rpc::MethodId method, Serve serve) {
		if (!method)
			throw std::invalid_argument("host transport: method 0");
		serves_[method] = std::move(serve);
	}
	// On loop_of_node(node): starts a call on the lane to `node` (rpc::Lane::call), on its Bulk
	// connection, which it dials when there is none: any node with an address, sharing a group or
	// not. Its events run on this loop; `done` runs once, at once when the node has no address.
	RpcCall call(kronuz::consensus::NodeId node, rpc::MethodId method, rpc::CallOptions options, std::string request,
				 rpc::Events events) {
		const auto total = options.total.value_or(request.size());
		if (!method || request.size() > rpc::chunk_bytes || !options.window || request.size() > total)
			throw std::invalid_argument("host transport: rpc call");
		const auto k = loop_of_node(node);
		auto &here = loops_[k];
		const RpcCall c{node, ++here.rpc_ids};
		++here.lane_stats.calls;
		events.done = ending(k, c, std::move(events.done));
		if (here.stopping || std::find(nodes_.begin(), nodes_.end(), node) == nodes_.end()) {
			events.done({rpc::Outcome::not_connected});
			return c;
		}
		auto &peer = here.rpc[node];
		peer.calls[c.id] = 0;
		if (peer.lane)
			begin(peer, c.id, {method, std::move(options), std::move(request), std::move(events)});
		else
			peer.pending.push_back({c.id, {method, options, std::move(request), std::move(events)}, options.deadline - rpc::Clock::now()});
		pump(k, node, LinkLane::Bulk);
		arm(k, node);
		return c;
	}
	// On the call's loop, each as rpc::Lane's own; a call that ended, or waits for its connection,
	// takes nothing (sendable 0).
	std::size_t sendable(RpcCall c) const {
		const auto *lane = lane_of(c);
		return lane ? lane->sendable(handle_of(c)) : 0;
	}
	void write(RpcCall c, std::string bytes, bool end) {
		if (auto *lane = lane_of(c))
			lane->send(handle_of(c), std::move(bytes), end);
	}
	void consume(RpcCall c, std::size_t n) {
		if (auto *lane = lane_of(c))
			lane->consume(handle_of(c), n);
	}
	void total(RpcCall c, std::uint64_t total) {
		if (auto *lane = lane_of(c))
			lane->total(handle_of(c), total);
	}
	void stop(RpcCall c, rpc::Code code, std::string_view text = {}) {
		auto &here = loops_[loop_of_node(c.node)];
		const auto it = here.rpc.find(c.node);
		if (it == here.rpc.end() || !it->second.calls.contains(c.id))
			return;
		if (auto *lane = lane_of(c))
			return lane->stop(handle_of(c), code, text);
		auto &pending = it->second.pending;  // not yet on a connection: it ends here alone
		const auto p = std::find_if(pending.begin(), pending.end(), [&](const Pending &p) { return p.id == c.id; });
		if (p == pending.end())
			return;
		auto done = std::move(p->call.events.done);
		pending.erase(p);
		done({code == rpc::Code::deadline_exceeded ? rpc::Outcome::timed_out : rpc::Outcome::cancel, std::uint32_t(code),
			  std::string(text.substr(0, rpc::text_bytes))});
	}

	// From any thread: every loop stops dialing and accepting and aborts its connections; calls
	// not yet on a connection end not_connected.
	void stop() {
		for (std::uint32_t k = 0; k != loops_.size(); ++k)
			asio::post(loops_[k].at.executor, [this, k] {
				auto &loop = loops_[k];
				loop.stopping = true;
				loop.links->stop();
				for (auto &[node, peer] : loop.rpc) {
					if (peer.timer)
						peer.timer->cancel();
					fail(peer, rpc::Outcome::not_connected);
				}
			});
	}
	// On `loop`: its connections and their tasks are gone.
	bool drained(std::uint32_t loop) const { return loops_.at(loop).links->drained(); }

  private:
	// A stored group's open; `serial` rises with each store of the group (its loop's counter), so
	// a sender opens it again.
	struct Opening {
		GroupOpen open;
		std::uint64_t serial = 0;
	};
	struct Outgoing {
		kronuz::consensus::NodeId node;
		LinkLane lane;
		FrameKind kind;
		GroupId group;
		std::string frame;
		Opening open;
	};
	// A connection of the host, as its frames' group loops know it.
	struct Peer {
		kronuz::consensus::NodeId node = 0;
		LinkLane lane = LinkLane::Control;
		std::uint64_t generation = 0;
	};
	// A group this host stores, on its loop: its incarnation here and its open.
	struct Stored {
		kronuz::consensus::NodeId incarnation = 0;
		Opening open;
	};
	// On a group loop, one connection: the stored groups whose peer open matched on it, each with
	// the source whose limits were reported last (0: none yet). A batch of a newer generation
	// empties it: the connection changed.
	struct Opened {
		std::uint64_t generation = 0;
		GroupTable<kronuz::consensus::NodeId> groups;
	};
	// One read's message and open frames for one loop, and the connection they came on.
	struct Batch {
		std::uint32_t from = 0;
		Peer peer;
		kronuz::consensus::Limits limits;  // the node limits the connection's peer sent in its hello
		std::vector<std::pair<Received, bool>> frames;  // the frame and whether it is an open
	};
	// The ready rings of a lane's sending groups: control, data.
	enum : int { control_ring = 0, data_ring = 1 };
	// A group with something to send on a lane, or data sent and not yet credited; an idle group has none.
	struct Active {
		std::deque<std::string> control, data;
		std::uint64_t queued = 0, outstanding = 0;  // data bytes queued; sent and not yet credited
		Opening open;                               // the open of the latest frame queued
		GroupId next[2] = {};                       // the next group in each ring
		std::uint8_t in = 0;                        // the rings it is in, a bit each
	};
	// What this host owes the peer for a group it receives: credit, and a closed when the peer's
	// frames were refused here.
	struct Owed {
		GroupId group = 0;
		std::uint64_t bytes = 0;
		bool close = false;
	};
	static constexpr std::size_t coalesce_floor = 4096;
	// A FIFO of groups, linked through their Active records.
	struct Ring {
		GroupId head = 0, tail = 0;
		std::size_t size = 0;
	};
	// One lane to one peer node, on the loop that holds its connection.
	struct Lane {
		GroupTable<std::uint64_t> opened;  // the groups whose open the connection carries, by serial
		GroupTable<Active> active;
		std::vector<Owed> owed;  // in the order the batches came; owed[repaid..] is still owed
		std::size_t repaid = 0, coalesce_at = coalesce_floor;
		Ring rings[2];
		const Link *link = nullptr;
		std::uint64_t generation = 0, dial_after = 0;
		bool dialing = false, lane_turn = false;  // lane_turn: Bulk's next chunk is the RPC lane's (D1)
	};
	// A call made before its connection is up.
	struct Made {
		rpc::MethodId method;
		rpc::CallOptions options;
		std::string request;
		rpc::Events events;
	};
	struct Pending {
		std::uint64_t id;
		Made call;
		rpc::Clock::duration timeout{};  // its deadline's span, for the late rule
		bool extended = false;
	};
	// T7: deadline.h's rule, for call deadlines.
	static rpc::Clock::duration late_rule(rpc::Clock::duration late, rpc::Clock::duration timeout) {
		const auto ns = [](rpc::Clock::duration d) { return std::uint64_t(std::max<std::int64_t>(0, std::chrono::nanoseconds(d).count())); };
		return std::chrono::duration_cast<rpc::Clock::duration>(std::chrono::nanoseconds(late_extension(ns(late), ns(timeout))));
	}
	// The lane to one peer node, on the loop of its connections: the Bulk link's call table, the
	// calls waiting for it, and each live call's handle there (0 while it waits).
	struct RpcPeer {
		const Link *link = nullptr;
		std::unique_ptr<rpc::Lane> lane;
		std::vector<Pending> pending;
		std::unordered_map<std::uint64_t, rpc::Handle> calls;
		std::unique_ptr<asio::steady_timer> timer;
		std::optional<rpc::Clock::time_point> armed, redial;
		bool feeding = false;
	};
	struct Loop {
		HostLoop at;
		std::unique_ptr<Links> links;
		Deliver deliver;
		ReportLimits report;
		std::map<std::pair<kronuz::consensus::NodeId, LinkLane>, Lane> lanes;
		std::vector<std::vector<Outgoing>> out;  // this loop's groups' frames, by connection loop
		std::vector<Batch> in;                   // this read's frames, by group loop
		GroupTable<Stored> stored;               // the groups this host stores on this loop
		std::map<std::pair<kronuz::consensus::NodeId, LinkLane>, Opened> opened;  // by connection
		std::uint64_t serial = 0;
		HostTransportStats stats;
		bool flushing = false, stopping = false;
		std::map<kronuz::consensus::NodeId, RpcPeer> rpc;  // by peer node, for the connections here
		std::uint64_t rpc_ids = 0;
		LaneStats lane_stats;
	};

	template <class F> void on(std::uint32_t loop, F f) {
		asio::post(loops_[loop].at.executor, std::move(f));
	}
	// The group opens again on every connection: a store or a forget.
	static void unopen(Loop &here, GroupId group) {
		for (auto &[peer, opened] : here.opened)
			opened.groups.erase(group);
	}
	// The group loop's queued frames, one post per connection loop.
	void flush(std::uint32_t k) {
		auto &here = loops_[k];
		here.flushing = false;
		for (std::uint32_t to = 0; to != loops_.size(); ++to) {
			if (here.out[to].empty())
				continue;
			auto frames = std::move(here.out[to]);
			here.out[to].clear();
			if (to == k)
				enqueue(to, std::move(frames));
			else
				on(to, [this, to, frames = std::move(frames)]() mutable { enqueue(to, std::move(frames)); });
		}
	}

	// The rings. A group is in `control` while it has a control frame, and in `data` while its
	// next data frame fits its credit (one always fits when nothing is outstanding).
	bool data_ready(const Active &a) const noexcept {
		return !a.data.empty() && (!a.outstanding || a.outstanding + a.data.front().size() <= options_.window);
	}
	static void push(Lane &lane, int ring, GroupId id, Active &a) {
		if (a.in & (1u << ring))
			return;
		a.in |= std::uint8_t(1u << ring);
		auto &r = lane.rings[ring];
		if (r.size++)
			lane.active.find(r.tail)->next[ring] = id;
		else
			r.head = id;
		r.tail = id;
	}
	static GroupId pop(Lane &lane, int ring, HostTransportStats &stats) {
		auto &r = lane.rings[ring];
		const auto id = r.head;
		auto &a = *lane.active.find(id);
		++stats.visited;
		a.in &= std::uint8_t(~(1u << ring));
		if (--r.size)
			r.head = a.next[ring];
		return id;
	}
	// An Active with nothing left and in no ring goes.
	static void settle(Lane &lane, GroupId id) {
		if (const auto *a = lane.active.find(id); a && !a->in && a->control.empty() && a->data.empty() && !a->outstanding)
			lane.active.erase(id);
	}
	// A batch's records join what the lane owes; while its connection cannot send, they merge
	// per group each time they double, so the lane owes O(groups) records.
	static void owe(Lane &lane, std::vector<Owed> owed) {
		if (lane.repaid == lane.owed.size()) {
			lane.owed = std::move(owed);
			lane.repaid = 0;
			return;
		}
		lane.owed.insert(lane.owed.end(), owed.begin(), owed.end());
		if (lane.owed.size() - lane.repaid < lane.coalesce_at)
			return;
		auto &v = lane.owed;
		v.erase(v.begin(), v.begin() + std::ptrdiff_t(lane.repaid));
		lane.repaid = 0;
		std::stable_sort(v.begin(), v.end(), [](const Owed &a, const Owed &b) { return a.group < b.group; });
		std::size_t n = 0;
		for (std::size_t i = 0; i != v.size(); ++i) {
			if (n && v[n - 1].group == v[i].group) {
				v[n - 1].bytes += v[i].bytes;
				v[n - 1].close |= v[i].close;
			} else {
				v[n++] = v[i];
			}
		}
		v.resize(n);
		lane.coalesce_at = std::max(coalesce_floor, 2 * n);
	}
	// A connection begins or ends: it carries no open, nothing is outstanding or owed, and the
	// rings are rebuilt from what is queued.
	void reset(Lane &lane, HostTransportStats &stats) {
		lane.opened.clear();
		std::vector<Owed>().swap(lane.owed);
		lane.repaid = 0;
		lane.coalesce_at = coalesce_floor;
		for (auto &r : lane.rings)
			r = {};
		for (auto &[id, a] : lane.active) {
			stats.uncredited -= a.outstanding;
			a.outstanding = 0;
			a.in = 0;
		}
		std::vector<GroupId> idle;
		for (auto &[id, a] : lane.active) {
			if (!a.control.empty())
				push(lane, control_ring, id, a);
			if (data_ready(a))
				push(lane, data_ring, id, a);
			if (!a.in)
				idle.push_back(id);
		}
		for (auto id : idle)
			settle(lane, id);
	}

	void enqueue(std::uint32_t k, std::vector<Outgoing> frames) {
		auto &here = loops_[k];
		std::vector<std::pair<kronuz::consensus::NodeId, LinkLane>> touched;
		for (auto &f : frames) {
			auto &lane = here.lanes[{f.node, f.lane}];
			auto &a = lane.active[f.group];
			const bool data = f.kind == FrameKind::data;
			a.open = f.open;
			if (data ? a.queued && a.queued + f.frame.size() > options_.data_queue : a.control.size() >= options_.control_queue) {
				++here.stats.dropped;
			} else {
				a.queued += data ? f.frame.size() : 0;
				(data ? a.data : a.control).push_back(std::move(f.frame));
				if (!data || data_ready(a))
					push(lane, data ? data_ring : control_ring, f.group, a);
			}
			settle(lane, f.group);
			if (touched.empty() || touched.back() != std::pair{f.node, f.lane})
				touched.emplace_back(f.node, f.lane);
		}
		for (auto &[node, lane] : touched)
			pump(k, node, lane);
	}
	// Feeds the lane's connection, or dials it.
	void pump(std::uint32_t k, kronuz::consensus::NodeId node, LinkLane which) {
		auto &here = loops_[k];
		auto &lane = here.lanes[{node, which}];
		if (lane.link) {
			feed(k, lane);
			return;
		}
		if (lane.dialing || here.stopping || here.at.scheduler.now() < lane.dial_after)
			return;
		lane.dialing = true;
		task::spawn(here.at.scheduler, here.links->dial(node, which),
					[this, k, node, which](std::exception_ptr, std::optional<bool> ok) {
						auto &lane = loops_[k].lanes[{node, which}];
						lane.dialing = false;
						if (!lane.link && !(ok && *ok)) {
							lane.dial_after = loops_[k].at.scheduler.now() +
											  std::uint64_t(std::chrono::nanoseconds(options_.redial).count());
							if (auto it = loops_[k].rpc.find(node); which == LinkLane::Bulk && it != loops_[k].rpc.end() &&
																	 !it->second.pending.empty()) {
								it->second.redial = rpc::Clock::now() + options_.redial;  // calls wait to their deadlines
								arm(k, node);
							}
						}
					});
	}
	std::size_t put(std::uint32_t k, Lane &lane, std::string_view frame) {
		lane.link->connection->send(frame);
		++loops_[k].stats.sent;
		return frame.size();
	}
	// Once the connection's queue is out, hands it up to `feed` bytes: credit and closed first,
	// then control frames, then data frames, so a control frame waits behind at most one data
	// frame. Each frame takes the head of one ring: no group is looked at but the one it sends.
	void feed(std::uint32_t k, Lane &lane) {
		auto &connection = *lane.link->connection;
		if (connection.queued() || connection.draining())
			return;
		auto &stats = loops_[k].stats;
		// D1: on Bulk the lane's small frames go first, and its chunks alternate with groups' data.
		rpc::Lane *calls = lane.link->lane == LinkLane::Bulk ? rpc_lane(k, *lane.link) : nullptr;
		for (std::size_t fed = 0; fed < options_.feed;) {
			if (lane.repaid != lane.owed.size()) {
				fed += repay(k, lane, stats);
				continue;
			}
			if (calls && (calls->small_ready() || (calls->chunk_ready() && (lane.lane_turn || !lane.rings[data_ring].size)))) {
				const bool chunk = !calls->small_ready();
				fed += put_rpc(k, lane, *calls->next());
				lane.lane_turn = lane.lane_turn && !chunk;
				continue;
			}
			GroupId id;
			if (lane.rings[control_ring].size) {
				id = pop(lane, control_ring, stats);
				auto &a = *lane.active.find(id);
				fed += open_first(k, lane, id, a);
				const auto frame = std::move(a.control.front());
				a.control.pop_front();
				fed += put(k, lane, frame);
				if (!a.control.empty())
					push(lane, control_ring, id, a);
			} else if (lane.rings[data_ring].size) {
				id = pop(lane, data_ring, stats);
				auto &a = *lane.active.find(id);
				fed += open_first(k, lane, id, a);
				const auto frame = std::move(a.data.front());
				a.data.pop_front();
				a.queued -= frame.size();
				a.outstanding += frame.size();
				stats.uncredited += frame.size();
				fed += put(k, lane, frame);
				lane.lane_turn = true;
				if (data_ready(a))
					push(lane, data_ring, id, a);
			} else {
				break;
			}
			settle(lane, id);
		}
	}
	// The next record owed: its credit, then its closed. The records go once all are repaid.
	std::size_t repay(std::uint32_t k, Lane &lane, HostTransportStats &stats) {
		const auto o = lane.owed[lane.repaid++];
		const auto id = o.group;
		++stats.visited;
		if (lane.repaid == lane.owed.size()) {
			std::vector<Owed>().swap(lane.owed);
			lane.repaid = 0;
		}
		std::size_t fed = 0;
		std::string frame;
		if (o.bytes) {
			frame.push_back(char(FrameKind::credit));
			kronuz::journal::put64(frame, id);
			kronuz::journal::put64(frame, o.bytes);
			fed += put(k, lane, frame);
		}
		if (o.close) {
			frame.assign(1, char(FrameKind::closed));
			kronuz::journal::put64(frame, id);
			fed += put(k, lane, frame);
		}
		return fed;
	}
	// Before a group's frame: its open, unless the connection carries the group's latest one.
	std::size_t open_first(std::uint32_t k, Lane &lane, GroupId group, const Active &a) {
		auto &carried = lane.opened[group];
		if (carried == a.open.serial)
			return 0;
		carried = a.open.serial;
		return put(k, lane, encode_open(group, a.open.open));
	}

	void established(std::uint32_t k, const Link &link) {
		auto &lane = loops_[k].lanes[{link.peer, link.lane}];
		++loops_[k].stats.links;
		lane.link = &link;
		const auto generation = ++lane.generation;
		reset(lane, loops_[k].stats);
		link.connection->on_read = [this, k, &link, generation] { read(k, link, generation); };
		link.connection->on_drained = [this, k, node = link.peer, which = link.lane] {
			auto &lane = loops_[k].lanes[{node, which}];
			if (lane.link)
				feed(k, lane);
		};
		if (link.lane == LinkLane::Bulk)
			open_rpc(k, link);
		feed(k, lane);
	}
	void ended(std::uint32_t k, const Link &link) {
		auto &lane = loops_[k].lanes[{link.peer, link.lane}];
		if (lane.link != &link)
			return;
		lane.link = nullptr;
		reset(lane, loops_[k].stats);
		bool waiting = false;
		if (auto it = loops_[k].rpc.find(link.peer); link.lane == LinkLane::Bulk && it != loops_[k].rpc.end()) {
			close_rpc(it->second);
			waiting = !it->second.pending.empty();  // calls its calls' done made again
		}
		if (!lane.active.empty() || waiting)
			pump(k, link.peer, link.lane);
	}
	// On the connection's loop, each frame: credit and closed apply here; a message or an open
	// joins its group loop's batch for this read; a lane frame goes to the call table at once (T5).
	void arrived(std::uint32_t k, const Link &link, std::string_view frame) {
		auto &here = loops_[k];
		if (frame.size() >= 4 && frame.substr(0, 4) == "DTRP")
			return rpc_arrived(k, link, frame);
		const auto kind = frame.empty() ? 0 : int(static_cast<unsigned char>(frame[0]));
		auto rest = frame.substr(std::min<std::size_t>(frame.size(), 1));
		if (kind == int(FrameKind::closed) && frame.size() == closed_frame) {
			auto &lane = here.lanes[{link.peer, link.lane}];
			lane.opened.erase(kronuz::journal::get64(rest));
			++here.stats.closed;
			return;
		}
		if (kind == int(FrameKind::open) && frame.size() == open_frame) {
			Received r;
			r.route.group = kronuz::journal::get64(rest);
			r.node = link.peer;
			r.payload.assign(frame);
			here.in[loop_of_group(r.route.group)].frames.emplace_back(std::move(r), true);
			return;
		}
		if (kind == int(FrameKind::credit) && frame.size() == 17) {
			auto &lane = here.lanes[{link.peer, link.lane}];
			const auto group = kronuz::journal::get64(rest);
			const auto bytes = kronuz::journal::get64(rest);
			if (auto *a = lane.active.find(group)) {
				const auto credited = std::min(bytes, a->outstanding);
				a->outstanding -= credited;
				here.stats.uncredited -= credited;
				if (data_ready(*a))
					push(lane, data_ring, group, *a);
				settle(lane, group);
			}
			return;
		}
		if ((kind != int(FrameKind::control) && kind != int(FrameKind::data)) || frame.size() < frame_header) {
			++here.stats.malformed;
			link.connection->abort();
			return;
		}
		Received r;
		r.route.group = kronuz::journal::get64(rest);
		r.route.source = kronuz::journal::get64(rest);
		r.route.destination = kronuz::journal::get64(rest);
		r.node = link.peer;
		r.data = kind == int(FrameKind::data);
		r.payload.assign(rest);
		++here.stats.received;
		here.in[loop_of_group(r.route.group)].frames.emplace_back(std::move(r), false);
	}
	// After each read: each loop's batch goes there in one post, the reader's own delivered here.
	void read(std::uint32_t k, const Link &link, std::uint64_t generation) {
		auto &here = loops_[k];
		bool any = false;
		for (std::uint32_t to = 0; to != loops_.size(); ++to) {
			auto &batch = here.in[to];
			if (batch.frames.empty())
				continue;
			if (!any)
				++here.stats.reads;  // before its posts, so a reader loading posts first sees at most L - 1 a read
			any = true;
			batch.from = k;
			batch.peer = {link.peer, link.lane, generation};
			batch.limits = link.remote.limits;
			auto moved = std::move(batch);
			batch = Batch{};
			if (to == k) {
				deliver(k, std::move(moved));
			} else {
				++here.stats.posts;
				on(to, [this, to, moved = std::move(moved)]() mutable { deliver(to, std::move(moved)); });
			}
		}
		if (auto &lane = here.lanes[{link.peer, link.lane}]; lane.link == &link)
			feed(k, lane);  // credit that arrived in this read
	}
	// On the group's loop: each open checked, each message frame through the per-frame rule to
	// the host; then the batch's data credit and the groups it refused back to its connection.
	void deliver(std::uint32_t k, Batch batch) {
		auto &here = loops_[k];
		auto &connection = here.opened[{batch.peer.node, batch.peer.lane}];
		if (batch.peer.generation > connection.generation) {
			connection.generation = batch.peer.generation;
			connection.groups.clear();
		}
		const bool current = batch.peer.generation == connection.generation;
		std::vector<Owed> owed;  // a run of one group's frames shares a record
		auto owe_for = [&](GroupId group) -> Owed & {
			if (owed.empty())
				owed.reserve(batch.frames.size());
			if (owed.empty() || owed.back().group != group)
				owed.push_back({group});
			return owed.back();
		};
		for (auto &[r, open] : batch.frames) {
			const auto *stored = here.stored.find(r.route.group);
			if (open) {
				if (!opening(here, stored, current, connection, r.route.group, r.payload))
					owe_for(r.route.group).close = true;
				continue;
			}
			bool taken = false;
			try {
				const bool opened = stored && current && connection.groups.contains(r.route.group);
				switch (admit(r.route, r.node, stored ? stored->incarnation : 0, opened, options_.member_of)) {
				case Admission::admitted:
					report(here, connection, r.route, batch.limits);
					taken = here.deliver && here.deliver(r);
					here.stats.refused += !taken;
					break;
				case Admission::unstored:
					++here.stats.unstored;
					owe_for(r.route.group).close = true;
					break;
				case Admission::unopened:
					++here.stats.unopened;
					owe_for(r.route.group).close = true;
					break;
				case Admission::misrouted:
					++here.stats.misrouted;
					break;
				case Admission::spoofed:
					++here.stats.spoofed;
					break;
				}
			} catch (...) {
				++here.stats.refused;
			}
			if (r.data) {
				owe_for(r.route.group).bytes += frame_header + r.payload.size();
			}
		}
		if (owed.empty())
			return;
		auto grant = [this, from = batch.from, peer = batch.peer, owed = std::move(owed)]() mutable {
			auto &lane = loops_[from].lanes[{peer.node, peer.lane}];
			if (lane.generation != peer.generation || !lane.link)
				return;  // the connection the frames came on ended; its credit went with it
			owe(lane, std::move(owed));
			feed(from, lane);
		};
		if (batch.from == k)
			grant();
		else
			on(batch.from, std::move(grant));
	}

	// A source's node limits, once per connection, before its first admitted frame there.
	static void report(Loop &here, Opened &connection, const Route &route, const kronuz::consensus::Limits &limits) {
		auto *last = connection.groups.find(route.group);  // admitted: open on this connection
		if (!last || *last == route.source || !here.report)
			return;
		*last = route.source;
		here.report(route.group, {route.source, limits});
	}
	// A peer's open on its connection: it opens the group there when the group is stored here and
	// every field matches; otherwise it closes it there (false: tell the peer).
	static bool opening(Loop &here, const Stored *stored, bool current, Opened &connection, GroupId group, std::string_view frame) {
		const auto open = decode_open(frame);
		if (!stored) {
			++here.stats.unstored;
			return false;
		}
		if (!current) {
			++here.stats.mismatched;
			return false;  // a stale connection's open changes nothing on the current one
		}
		if (!open || open_fault(stored->open.open, open->second)) {
			connection.groups.erase(group);
			++here.stats.mismatched;
			return false;
		}
		(void)connection.groups[group];  // an open again keeps the source already reported
		++here.stats.opens;
		return true;
	}

	// The RPC lane (docs/mechanisms/rpc-lane.md, On Bulk), on the connection's loop.
	rpc::Lane *rpc_lane(std::uint32_t k, const Link &link) {
		const auto it = loops_[k].rpc.find(link.peer);
		return it != loops_[k].rpc.end() && it->second.link == &link ? it->second.lane.get() : nullptr;
	}
	const rpc::Lane *lane_of(RpcCall c) const {
		const auto &here = loops_[loop_of_node(c.node)];
		const auto it = here.rpc.find(c.node);
		if (it == here.rpc.end() || !it->second.lane)
			return nullptr;
		const auto call = it->second.calls.find(c.id);
		return call != it->second.calls.end() && call->second ? it->second.lane.get() : nullptr;
	}
	rpc::Lane *lane_of(RpcCall c) { return const_cast<rpc::Lane *>(std::as_const(*this).lane_of(c)); }
	rpc::Handle handle_of(RpcCall c) const { return loops_[loop_of_node(c.node)].rpc.at(c.node).calls.at(c.id); }
	// A call's done, wrapped: the call leaves its peer's table and is counted, then the caller's runs.
	std::function<void(const rpc::Status &)> ending(std::uint32_t k, RpcCall c, std::function<void(const rpc::Status &)> done) {
		return [this, k, c, done = std::move(done)](const rpc::Status &status) {
			auto &here = loops_[k];
			if (auto it = here.rpc.find(c.node); it != here.rpc.end())
				it->second.calls.erase(c.id);
			++here.lane_stats.outcomes[std::size_t(status.outcome)];
			if (done)
				done(status);
		};
	}
	// Hands a call to the peer's lane; its done may run within, and leaves no handle behind.
	static void begin(RpcPeer &peer, std::uint64_t id, Made made) {
		const auto h = peer.lane->call(made.method, std::move(made.options), std::move(made.request), std::move(made.events));
		if (auto it = peer.calls.find(id); it != peer.calls.end())
			it->second = h;
	}
	// Ends every call still waiting for a connection.
	static void fail(RpcPeer &peer, rpc::Outcome outcome) {
		auto pending = std::move(peer.pending);
		peer.pending.clear();
		for (auto &p : pending)
			p.call.events.done({outcome});
	}
	// A Bulk link is up: its call table, at the version the hellos agreed (T1), takes the calls
	// that waited for it. A link with the lane off ends them protocol_not_supported.
	void open_rpc(std::uint32_t k, const Link &link) {
		auto &peer = loops_[k].rpc[link.peer];
		close_rpc(peer);
		peer.link = &link;
		peer.redial.reset();
		if (!link.rpc)
			return fail(peer, rpc::Outcome::protocol_not_supported);
		auto options = options_.rpc.lane;
		options.version = link.rpc;
		options.late = late_rule;
		const auto node = link.peer;
		peer.lane = std::make_unique<rpc::Lane>(
			link.dialed, rpc::Pools{serving_, calling_, outgoing_}, options,
			[this, k, node](const rpc::Incoming &in) { return accept_rpc(k, node, in); },
			[this, k](std::function<void()> f) { on(k, std::move(f)); }, [this, k, node] { rpc_ready(k, node); });
		auto pending = std::move(peer.pending);
		peer.pending.clear();
		for (auto &p : pending)
			if (peer.lane)
				begin(peer, p.id, std::move(p.call));
		arm(k, node);
	}
	// The link ended: its calls end connection_aborted, or not_processed when never sent.
	static void close_rpc(RpcPeer &peer) {
		peer.link = nullptr;
		if (auto lane = std::move(peer.lane))
			lane->close();
	}
	std::optional<rpc::Events> accept_rpc(std::uint32_t k, kronuz::consensus::NodeId node, const rpc::Incoming &in) {
		const auto serve = serves_.find(in.method);
		if (serve == serves_.end())
			return std::nullopt;
		auto &here = loops_[k];
		const RpcCall c{node, ++here.rpc_ids};
		auto &peer = here.rpc[node];
		peer.calls[c.id] = in.handle;
		std::optional<rpc::Events> events;
		try {
			events = serve->second(c, in);
		} catch (...) {
			peer.calls.erase(c.id);
			throw;  // the call table fails the call alone (handler_failed)
		}
		if (!events) {
			peer.calls.erase(c.id);
			return std::nullopt;
		}
		++here.lane_stats.served;
		events->done = ending(k, c, std::move(events->done));
		return events;
	}
	// The lane has a frame to write: the connection is fed once the running work yields.
	void rpc_ready(std::uint32_t k, kronuz::consensus::NodeId node) {
		auto &peer = loops_[k].rpc[node];
		if (std::exchange(peer.feeding, true))
			return;
		on(k, [this, k, node] {
			loops_[k].rpc[node].feeding = false;
			if (auto &lane = loops_[k].lanes[{node, LinkLane::Bulk}]; lane.link)
				feed(k, lane);
		});
	}
	std::size_t put_rpc(std::uint32_t k, Lane &lane, rpc::Outgoing out) {
		lane.link->connection->send(out.bytes);
		auto &stats = loops_[k].lane_stats;
		++stats.frames_out;
		stats.bytes_out += out.bytes.size();
		return out.bytes.size();  // the frame's charge returns here, at the connection's queue
	}
	void rpc_arrived(std::uint32_t k, const Link &link, std::string_view frame) {
		auto &here = loops_[k];
		auto *lane = link.lane == LinkLane::Bulk ? rpc_lane(k, link) : nullptr;
		if (lane) {
			++here.lane_stats.frames_in;
			here.lane_stats.bytes_in += frame.size();
			try {
				lane->receive(frame);
				return arm(k, link.peer);
			} catch (...) {
				close_rpc(here.rpc[link.peer]);  // D3: a frame that breaks a rule closes the connection
			}
		}
		++here.stats.malformed;
		link.connection->abort();
	}
	// The peer's timer, at its earliest deadline: a waiting call's, the lane's, or the redial.
	void arm(std::uint32_t k, kronuz::consensus::NodeId node) {
		auto &here = loops_[k];
		auto &peer = here.rpc[node];
		auto next = peer.lane ? peer.lane->next_deadline() : std::nullopt;
		for (auto &p : peer.pending)
			if (!next || p.call.options.deadline < *next)
				next = p.call.options.deadline;
		if (peer.redial && (!next || *peer.redial < *next))
			next = peer.redial;
		if (!next || here.stopping || (peer.armed && *peer.armed <= *next))
			return;
		if (!peer.timer)
			peer.timer = std::make_unique<asio::steady_timer>(here.at.executor);
		peer.armed = next;
		peer.timer->expires_at(*next);
		peer.timer->async_wait([this, k, node](asio::error_code error) {
			if (!error)
				expire_rpc(k, node);
		});
	}
	void expire_rpc(std::uint32_t k, kronuz::consensus::NodeId node) {
		auto &peer = loops_[k].rpc[node];
		const auto now = rpc::Clock::now();
		peer.armed.reset();
		std::vector<rpc::Events> due;
		std::erase_if(peer.pending, [&](Pending &p) {
			if (p.call.options.deadline <= now && !p.extended)
				if (const auto more = late_rule(now - p.call.options.deadline, p.timeout); more > rpc::Clock::duration::zero()) {
					p.call.options.deadline = now + more;
					p.extended = true;
				}
			if (p.call.options.deadline > now)
				return false;
			due.push_back(std::move(p.call.events));
			return true;
		});
		for (auto &events : due)
			events.done({rpc::Outcome::timed_out});
		if (peer.lane)
			peer.lane->expire();
		if (peer.redial && *peer.redial <= now) {
			peer.redial.reset();
			if (!peer.pending.empty()) {
				pump(k, node, LinkLane::Bulk);
				if (const auto &lane = loops_[k].lanes[{node, LinkLane::Bulk}]; !lane.link && !lane.dialing)
					peer.redial = now + std::chrono::milliseconds(10);  // the dial's own spacing has not passed
			}
		}
		arm(k, node);
	}

	HostTransportOptions options_;
	rpc::ByteBudget serving_, calling_, outgoing_;  // the lane's pools, shared by every loop (D5); outlive every Lane
	std::vector<Loop> loops_;
	std::vector<kronuz::consensus::NodeId> nodes_;  // the nodes with an address
	std::map<rpc::MethodId, Serve> serves_;          // set before serve(), then only read
};

// The Driver's Send for `group`, from its incarnation `local` to the member on `node`: Raft
// messages go on Control, an append carrying entries as data and every other message as control.
inline void send_message(HostTransport &transport, std::uint32_t loop, GroupId group, kronuz::consensus::NodeId local,
						 kronuz::consensus::NodeId node, const kronuz::consensus::Send &send,
						 const kronuz::consensus::Limits &limits = {}) {
	const auto *append = std::get_if<kronuz::consensus::AppendRequest>(&send.message);
	const auto kind = append && !append->entries.empty() ? FrameKind::data : FrameKind::control;
	transport.send(loop, LinkLane::Control, kind, {group, local, send.peer}, node, encode(send.message, limits));
}
// The Driver's SendTransfer for `group`, from its incarnation `local` to `peer` on `node`: one
// snapshot transfer frame (RFTXFR01), a data frame on Bulk within the group's credit.
inline void send_transfer(HostTransport &transport, std::uint32_t loop, GroupId group, kronuz::consensus::NodeId local,
						  kronuz::consensus::NodeId node, kronuz::consensus::NodeId peer, std::string_view frame) {
	transport.send(loop, LinkLane::Bulk, FrameKind::data, {group, local, peer}, node, frame);
}
// Whether a received message is a snapshot transfer frame (the Driver's transfer()), not a Receive.
inline bool is_transfer(const Received &received) noexcept {
	return std::string_view(received.payload).substr(0, 8) == "RFTXFR01";
}
// A received message as the Driver's Receive from its source incarnation; nullopt when its bytes
// do not decode. The transport admitted the source by the per-frame rule.
inline std::optional<kronuz::consensus::Receive> receive_message(const Received &received,
																  const kronuz::consensus::Limits &limits = {}) {
	try {
		return kronuz::consensus::Receive{received.route.source, decode(received.payload, limits)};
	} catch (const std::exception &) {
		return std::nullopt;
	}
}
} // namespace kronuz::consensus::transport
