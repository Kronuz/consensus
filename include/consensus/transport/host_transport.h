#pragma once
#include "codec.h"
#include "links.h"
#include <asio/post.hpp>
#include <atomic>
#include <deque>
#include <journal/host.h>
#include <optional>

// The host transport (docs/mechanisms/host-transport.md): a Control and a Bulk connection per
// peer node, each on the loop its node places on, shared by every group of the two nodes. A
// frame names its group and incarnations; the reader batches frames by the group's loop and
// posts each batch once per read. Senders keep per-group queues: control frames first, round
// robin across groups, and data frames within each group's credit.
namespace kronuz::consensus::transport {
using GroupId = std::uint64_t;

// What a frame carries: a control or a data message for a group, or credit back for data.
enum class FrameKind : std::uint8_t { control = 1, data = 2, credit = 3 };
// The group and the incarnations a message goes from and to.
struct Route {
	GroupId group = 0;
	kronuz::consensus::NodeId source = 0, destination = 0;
};
constexpr std::size_t frame_header = 25;  // kind, then group, source and destination (credit: group, bytes)

// A message frame as the group's loop receives it; `node` is the connection's authenticated node.
struct Received {
	Route route;
	kronuz::consensus::NodeId node = 0;
	bool data = false;
	std::string payload;
};

struct HostTransportOptions {
	LinkOptions links;
	std::uint64_t window = 4 << 20;     // data bytes a group may have sent and not yet credited, per connection
	std::size_t control_queue = 64;     // control frames a group queues per connection; more are dropped
	std::uint64_t data_queue = 8 << 20; // data bytes a group queues per connection; more are dropped
	std::size_t feed = 64 * 1024;       // bytes handed to a connection once its queue is out
	std::chrono::milliseconds redial{200};
};

// Counters of one loop, readable from any thread.
struct HostTransportStats {
	std::atomic<std::uint64_t> links{0}, sent{0}, received{0}, reads{0}, posts{0}, dropped{0}, refused{0}, malformed{0};
};

// A loop of the host: where its tasks run and its sockets wait.
struct HostLoop {
	asio::any_io_executor executor;
	task::scheduler_ref scheduler;
};

// Destroy it after stop(), once drained() on every loop and the loops have stopped running.
class HostTransport {
  public:
	// On the group's loop, for each received message: false when the group cannot take it now,
	// and the frame is dropped and counted (Raft resends). Its credit returns either way.
	using Deliver = std::function<bool(const Received &)>;

	HostTransport(std::vector<HostLoop> loops, std::shared_ptr<asio::ssl::context> tls, const NodePins &pins,
				  kronuz::consensus::Identity cluster, const std::map<kronuz::consensus::NodeId, PeerAddress> &addresses,
				  HostTransportOptions options = {})
		: options_(std::move(options)), loops_(loops.size()) {
		if (loops_.empty() || loops_.size() > 4096)
			throw std::invalid_argument("host transport: 1 to 4,096 loops");
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
	// Before any frame arrives: what loop `loop` delivers.
	void on_deliver(std::uint32_t loop, Deliver deliver) { loops_.at(loop).deliver = std::move(deliver); }
	// Accepts on `acceptor`, a socket of loop 0, until stop().
	void serve(asio::ip::tcp::acceptor &acceptor) { task::spawn(loops_[0].at.scheduler, loops_[0].links->serve(acceptor)); }

	// On `loop`, the group's: queues a message for the member on `node`. A loop's frames go to
	// each connection's loop in one post once the running work yields.
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
		if (frame.size() > options_.links.connection.maximum_frame) {
			++here.stats.dropped;  // no connection carries it
			return;
		}
		const auto to = loop_of_node(node);
		here.out[to].push_back({node, lane, kind, route.group, std::move(frame)});
		if (!here.flushing) {
			here.flushing = true;
			asio::post(here.at.executor, [this, loop] { flush(loop); });
		}
	}

	// From any thread: every loop stops dialing and accepting and aborts its connections.
	void stop() {
		for (auto &loop : loops_)
			asio::post(loop.at.executor, [&loop] {
				loop.stopping = true;
				loop.links->stop();
			});
	}
	// On `loop`: its connections and their tasks are gone.
	bool drained(std::uint32_t loop) const { return loops_.at(loop).links->drained(); }

  private:
	struct Outgoing {
		kronuz::consensus::NodeId node;
		LinkLane lane;
		FrameKind kind;
		GroupId group;
		std::string frame;
	};
	// One read's message frames for one loop, and the connection they came on.
	struct Batch {
		std::uint32_t from = 0;
		kronuz::consensus::NodeId node = 0;
		LinkLane lane = LinkLane::Control;
		std::uint64_t generation = 0;
		std::vector<Received> frames;
	};
	// One lane to one peer node, on the loop that holds its connection.
	struct Lane {
		struct Group {
			std::deque<std::string> control, data;
			std::uint64_t queued = 0, outstanding = 0;  // data bytes queued; sent and not yet credited
		};
		std::map<GroupId, Group> groups;           // only groups with something queued or uncredited
		std::map<GroupId, std::uint64_t> grants;   // credit owed to the peer, coalesced
		GroupId control_at = 0, data_at = 0;       // the round-robin cursors
		const Link *link = nullptr;
		std::uint64_t generation = 0, dial_after = 0;
		bool dialing = false;
	};
	struct Loop {
		HostLoop at;
		std::unique_ptr<Links> links;
		Deliver deliver;
		std::map<std::pair<kronuz::consensus::NodeId, LinkLane>, Lane> lanes;
		std::vector<std::vector<Outgoing>> out;  // this loop's groups' frames, by connection loop
		std::vector<Batch> in;                   // this read's frames, by group loop
		HostTransportStats stats;
		bool flushing = false, stopping = false;
	};

	template <class F> void on(std::uint32_t loop, F f) {
		asio::post(loops_[loop].at.executor, std::move(f));
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
	void enqueue(std::uint32_t k, std::vector<Outgoing> frames) {
		auto &here = loops_[k];
		std::vector<std::pair<kronuz::consensus::NodeId, LinkLane>> touched;
		for (auto &f : frames) {
			auto &lane = here.lanes[{f.node, f.lane}];
			auto &group = lane.groups[f.group];
			const bool data = f.kind == FrameKind::data;
			if (data ? group.queued && group.queued + f.frame.size() > options_.data_queue
					 : group.control.size() >= options_.control_queue) {
				++here.stats.dropped;
			} else {
				group.queued += data ? f.frame.size() : 0;
				(data ? group.data : group.control).push_back(std::move(f.frame));
			}
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
						if (!lane.link && !(ok && *ok))
							lane.dial_after = loops_[k].at.scheduler.now() +
											  std::uint64_t(std::chrono::nanoseconds(options_.redial).count());
					});
	}
	// The next group in round-robin order after `at` with a frame ready: any control frame, or a
	// data frame within the group's credit (one frame always fits when nothing is outstanding).
	std::optional<GroupId> next(Lane &lane, bool data) {
		auto ready = [&](const Lane::Group &g) {
			if (!data)
				return !g.control.empty();
			return !g.data.empty() && (!g.outstanding || g.outstanding + g.data.front().size() <= options_.window);
		};
		auto &at = data ? lane.data_at : lane.control_at;
		for (auto it = lane.groups.upper_bound(at); it != lane.groups.end(); ++it)
			if (ready(it->second))
				return at = it->first;
		for (auto it = lane.groups.begin(); it != lane.groups.end() && it->first <= at; ++it)
			if (ready(it->second))
				return at = it->first;
		return std::nullopt;
	}
	// Once the connection's queue is out, hands it up to `feed` bytes: credit and control frames
	// first, then data frames, so a control frame waits behind at most one data frame.
	void feed(std::uint32_t k, Lane &lane) {
		auto &connection = *lane.link->connection;
		if (connection.queued() || connection.draining())
			return;
		for (std::size_t fed = 0; fed < options_.feed;) {
			std::string frame;
			if (!lane.grants.empty()) {
				auto [group, bytes] = *lane.grants.begin();
				lane.grants.erase(lane.grants.begin());
				frame.push_back(char(FrameKind::credit));
				kronuz::journal::put64(frame, group);
				kronuz::journal::put64(frame, bytes);
			} else if (auto group = next(lane, false)) {
				auto &g = lane.groups[*group];
				frame = std::move(g.control.front());
				g.control.pop_front();
			} else if (auto group = next(lane, true)) {
				auto &g = lane.groups[*group];
				frame = std::move(g.data.front());
				g.data.pop_front();
				g.queued -= frame.size();
				g.outstanding += frame.size();
			} else {
				break;
			}
			fed += frame.size();
			connection.send(frame);
			++loops_[k].stats.sent;
		}
		std::erase_if(lane.groups, [](const auto &g) {
			return g.second.control.empty() && g.second.data.empty() && !g.second.outstanding;
		});
	}

	void established(std::uint32_t k, const Link &link) {
		auto &lane = loops_[k].lanes[{link.peer, link.lane}];
		++loops_[k].stats.links;
		lane.link = &link;
		const auto generation = ++lane.generation;
		lane.grants.clear();
		for (auto &[id, group] : lane.groups)
			group.outstanding = 0;
		link.connection->on_read = [this, k, &link, generation] { read(k, link, generation); };
		link.connection->on_drained = [this, k, node = link.peer, which = link.lane] {
			auto &lane = loops_[k].lanes[{node, which}];
			if (lane.link)
				feed(k, lane);
		};
		feed(k, lane);
	}
	void ended(std::uint32_t k, const Link &link) {
		auto &lane = loops_[k].lanes[{link.peer, link.lane}];
		if (lane.link != &link)
			return;
		lane.link = nullptr;
		lane.grants.clear();
		for (auto &[id, group] : lane.groups)
			group.outstanding = 0;
		if (!lane.groups.empty())
			pump(k, link.peer, link.lane);
	}
	// On the connection's loop, each frame: credit applies here; a message joins its group
	// loop's batch for this read.
	void arrived(std::uint32_t k, const Link &link, std::string_view frame) {
		auto &here = loops_[k];
		const auto kind = frame.empty() ? 0 : int(static_cast<unsigned char>(frame[0]));
		auto rest = frame.substr(std::min<std::size_t>(frame.size(), 1));
		if (kind == int(FrameKind::credit) && frame.size() == 17) {
			auto &lane = here.lanes[{link.peer, link.lane}];
			const auto group = kronuz::journal::get64(rest);
			const auto bytes = kronuz::journal::get64(rest);
			if (auto it = lane.groups.find(group); it != lane.groups.end())
				it->second.outstanding -= std::min(bytes, it->second.outstanding);
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
		here.in[loop_of_group(r.route.group)].frames.push_back(std::move(r));
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
			batch.node = link.peer;
			batch.lane = link.lane;
			batch.generation = generation;
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
	// On the group's loop: each frame to the host, then the batch's data credit back to its connection.
	void deliver(std::uint32_t k, Batch batch) {
		auto &here = loops_[k];
		std::map<GroupId, std::uint64_t> credit;
		for (auto &r : batch.frames) {
			bool taken = false;
			try {
				taken = here.deliver && here.deliver(r);
			} catch (...) {
			}
			here.stats.refused += !taken;
			if (r.data)
				credit[r.route.group] += frame_header + r.payload.size();
		}
		if (credit.empty())
			return;
		auto grant = [this, from = batch.from, node = batch.node, which = batch.lane, generation = batch.generation,
					  credit = std::move(credit)] {
			auto &lane = loops_[from].lanes[{node, which}];
			if (lane.generation != generation || !lane.link)
				return;  // the connection the frames came on ended; its credit went with it
			for (auto &[group, bytes] : credit)
				lane.grants[group] += bytes;
			feed(from, lane);
		};
		if (batch.from == k)
			grant();
		else
			on(batch.from, std::move(grant));
	}

	HostTransportOptions options_;
	std::vector<Loop> loops_;
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
// A received message as the Driver's Receive from its source incarnation; nullopt when its bytes
// do not decode. Which sources a group admits is the host's rule.
inline std::optional<kronuz::consensus::Receive> receive_message(const Received &received,
																  const kronuz::consensus::Limits &limits = {}) {
	try {
		return kronuz::consensus::Receive{received.route.source, decode(received.payload, limits)};
	} catch (const std::exception &) {
		return std::nullopt;
	}
}
} // namespace kronuz::consensus::transport
