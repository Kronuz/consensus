/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */


// Track C T7 on the host transport (docs/perf/flow-control.md). A node is one loop thread holding
// a Core per group, over loopback mutual TLS with the test fixture's certificates; its Persists
// go to its storage, one thread that writes every pending batch into a preallocated file and
// makes them durable with one fdatasync (a turn barrier). `netem` on lo adds the round trip.
//   flow_bench barrier DIR SECONDS                        the storage's own rate: 2 MiB per barrier
//   flow_bench one FIXTURE DIR PIPELINE FRAME COMMAND SECONDS
//            one three-voter group led by node 1, proposing COMMAND-byte commands as fast as its
//            leader persists them, Flow{PIPELINE, 4 MiB, FRAME}: committed MB/s, commit latency
//   flow_bench many FIXTURE DIR SLOW HOT SECONDS
//            100 three-voter groups on one connection per node pair, each proposing a 4 KiB command
//            every 10 ms; node 2's barrier for group 0 is stretched SLOW times; HOT 1 makes group 0
//            propose 256 KiB commands as fast as it can: the other groups' commit and node-2 apply
//            latency, and group 0's bytes sent to node 2 and not yet delivered there
//   flow_bench gap FIXTURE LANE FRAME SECONDS
//            a control frame every 10 ms from node 1 to node 2 while four groups keep FRAME-byte
//            data frames queued on LANE (control, bulk or none): each control frame's delay
#include "consensus/core.h"
#include "consensus/transport/host_transport.h"
#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>
#include <openssl/pem.h>
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <filesystem>
#include <fstream>
#include <future>
#include <map>
#include <mutex>
#include <string>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <vector>

using namespace kronuz::consensus::transport;
using namespace kronuz::consensus;
using namespace std::chrono_literals;
using steady = std::chrono::steady_clock;
using tcp = asio::ip::tcp;

namespace {
std::filesystem::path fixture;
const char *names[] = {"", "one", "two", "three", "four"};
const steady::time_point began = steady::now();
std::uint64_t us_now() { return std::uint64_t(std::chrono::duration_cast<std::chrono::microseconds>(steady::now() - began).count()); }
std::uint64_t ms_now() { return us_now() / 1000 + 1; }
std::atomic<bool> measuring{false};

Fingerprint pin_of(const std::string &name) {
	std::unique_ptr<BIO, decltype(&BIO_free)> input(BIO_new_file((fixture / (name + ".pem")).c_str(), "r"), BIO_free);
	std::unique_ptr<X509, decltype(&X509_free)> leaf(PEM_read_bio_X509(input.get(), nullptr, nullptr, nullptr), X509_free);
	if (!leaf)
		throw std::runtime_error("no certificate " + name);
	return certificate_fingerprint(leaf.get());
}
std::shared_ptr<asio::ssl::context> context(const std::string &name) {
	auto c = std::make_shared<asio::ssl::context>(asio::ssl::context::tls);
	reactor::tls::configure(*c, {(fixture / (name + ".pem")).string(), (fixture / (name + ".key")).string(), (fixture / "ca.pem").string()},
							reactor::tls::Role::Peer);
	return c;
}

// Microsecond samples and their percentiles.
struct Samples {
	std::vector<std::uint64_t> v;
	void add(std::uint64_t x) { v.push_back(x); }
	std::uint64_t at(double q) {
		if (v.empty())
			return 0;
		std::sort(v.begin(), v.end());
		return v[std::size_t(q * double(v.size() - 1))];
	}
	void merge(const Samples &o) { v.insert(v.end(), o.v.begin(), o.v.end()); }
};

// One loop on its own thread.
struct Loop {
	asio::io_context io;
	task::asio_scheduler scheduler{io.get_executor()};
	asio::executor_work_guard<asio::io_context::executor_type> work = asio::make_work_guard(io);
	std::thread thread;
	void start() {
		thread = std::thread([this] { io.run(); });
	}
	void join() {
		work.reset();
		io.stop();
		if (thread.joinable())
			thread.join();
	}
	template <class F> auto at(F f) -> decltype(f()) {
		std::packaged_task<decltype(f())()> call(std::move(f));
		auto result = call.get_future();
		asio::post(io, [&call] { call(); });
		return result.get();
	}
	~Loop() { join(); }
};

GroupOpen bench_open() {
	GroupOpen o;
	o.cluster.fill('T');
	o.group.fill('G');
	o.epoch.fill('E');
	o.profile.fill(0x5A);
	return o;
}
// A host transport for node `node` (incarnation k is on node k) over `loops`.
std::unique_ptr<HostTransport> transport_of(NodeId node, std::vector<std::unique_ptr<Loop>> &loops, const std::map<NodeId, PeerAddress> &addresses) {
	auto tls = context(names[node]);
	std::map<NodeId, Fingerprint> pins;
	for (NodeId n = 1; n <= 4; ++n)
		pins[n] = pin_of(names[n]);
	HostTransportOptions options;
	options.member_of = [](GroupId, NodeId incarnation) -> std::optional<Member> {
		if (incarnation < 1 || incarnation > 4)
			return std::nullopt;
		return Member{incarnation, incarnation, MemberRole::Voter};
	};
	std::vector<HostLoop> at;
	for (auto &l : loops)
		at.push_back({l->io.get_executor(), task::scheduler_ref::of(l->scheduler)});
	Identity cluster{};
	cluster.fill('K');
	return std::make_unique<HostTransport>(at, tls, NodePins(node, pins, tls->native_handle()), cluster, addresses, options);
}
// The transport stops and drains; the loops keep running.
void quiesce(HostTransport &t, std::vector<std::unique_ptr<Loop>> &loops) {
	t.stop();
	for (std::uint32_t k = 0; k != loops.size(); ++k)
		for (int i = 0; !loops[k]->at([&] { return t.drained(k); }); ++i) {
			if (i == 2000)
				throw std::runtime_error("the transport does not drain");
			std::this_thread::sleep_for(5ms);
		}
}

// A node's durable storage: one thread writes every pending batch into a preallocated 64 MiB
// ring and makes them durable with one barrier, then completes each; `slow` stretches each
// barrier to `slow` times its own length.
class Storage {
  public:
	Storage(const std::filesystem::path &file, unsigned slow = 1) : slow_(slow) {
		fd_ = ::open(file.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0600);
		if (fd_ < 0)
			throw std::system_error(errno, std::generic_category(), file.string());
		const std::string zeros(1 << 20, '\0');
		for (std::uint64_t o = 0; o < ring; o += zeros.size())
			write(zeros);
		barrier();
		thread_ = std::thread([this] { run(); });
	}
	~Storage() {
		{
			std::lock_guard hold(mutex_);
			done_ = true;
		}
		wake_.notify_one();
		thread_.join();
		::close(fd_);
	}
	void submit(std::string bytes, std::function<void()> done) {
		{
			std::lock_guard hold(mutex_);
			queue_.push_back({std::move(bytes), std::move(done)});
		}
		wake_.notify_one();
	}
	// Barrier latencies (us) and bytes per barrier, while measuring.
	std::mutex stats_mutex;
	Samples barriers;
	std::uint64_t bytes = 0, count = 0;

  private:
	static constexpr std::uint64_t ring = 64 << 20;
	struct Job {
		std::string bytes;
		std::function<void()> done;
	};
	void write(std::string_view s) {
		while (!s.empty()) {
			if (offset_ == ring)
				offset_ = 0;
			const auto n = std::min<std::uint64_t>(s.size(), ring - offset_);
			const auto w = ::pwrite(fd_, s.data(), n, off_t(offset_));
			if (w <= 0)
				throw std::system_error(errno, std::generic_category(), "pwrite");
			offset_ += std::uint64_t(w);
			s.remove_prefix(std::size_t(w));
		}
	}
	void barrier() {
#ifdef __APPLE__
		const int r = ::fcntl(fd_, F_FULLFSYNC);
#else
		const int r = ::fdatasync(fd_);
#endif
		if (r != 0)
			throw std::system_error(errno, std::generic_category(), "barrier");
	}
	void run() {
		for (;;) {
			std::deque<Job> jobs;
			{
				std::unique_lock lock(mutex_);
				wake_.wait(lock, [&] { return done_ || !queue_.empty(); });
				if (queue_.empty())
					return;
				jobs.swap(queue_);
			}
			const auto start = steady::now();
			std::uint64_t n = 0;
			for (auto &j : jobs) {
				write(j.bytes);
				n += j.bytes.size();
			}
			barrier();
			const auto took = steady::now() - start;
			if (slow_ > 1)
				std::this_thread::sleep_for(took * (slow_ - 1));
			if (measuring) {
				std::lock_guard hold(stats_mutex);
				barriers.add(std::uint64_t(std::chrono::duration_cast<std::chrono::microseconds>(steady::now() - start).count()));
				bytes += n;
				++count;
			}
			for (auto &j : jobs)
				j.done();
		}
	}
	int fd_ = -1;
	unsigned slow_;
	std::uint64_t offset_ = 0;
	std::mutex mutex_;
	std::condition_variable wake_;
	std::deque<Job> queue_;
	bool done_ = false;
	std::thread thread_;
};

std::string bytes_of(const StorageBatch &b) {
	std::string s(64, '\0');  // the hard state and the batch's header
	if (b.log)
		for (const auto &e : b.log->entries) {
			s.append(24, '\0');
			s.append(e.payload);
		}
	return s;
}
std::uint64_t entry_bytes(const Message &m) {
	std::uint64_t n = 0;
	if (const auto *a = std::get_if<AppendRequest>(&m))
		for (const auto &e : a->entries)
			n += e.payload.size();
	return n;
}

struct Options {
	Limits limits{};
	Flow flow{};
	std::size_t command = 4096;
	std::uint64_t period_us = 0;  // between a group's proposals; 0: whenever its leader is not persisting
	GroupId slow_group = 0;       // its storage at `slow_node` is its own, stretched `slow` times
	NodeId slow_node = 2;
	unsigned slow = 1;
	GroupId hot = 0;  // a group that proposes `hot_command` bytes whenever it can, whatever period_us
	std::size_t hot_command = 256 << 10;
};

// What every node knows of a group: when each index was proposed, and its bytes on the wire to
// the slow node.
struct Track {
	std::mutex mutex;
	std::unordered_map<Index, std::uint64_t> proposed;
	std::atomic<std::uint64_t> sent{0}, delivered{0};
};
std::map<GroupId, std::unique_ptr<Track>> tracks;

struct Node;
// A group's Core on its node, with what it holds while the Core persists.
struct Group {
	GroupId id;
	std::unique_ptr<Core> core;
	Storage *storage;
	Track *track;
	std::deque<Receive> inbox;
	std::uint64_t next_propose = 0;
	RequestId request = 0;
	std::unordered_map<RequestId, std::uint64_t> asked;
	// While measuring: the leader's commits, a node's applies, and inbox overflows.
	Samples commit_us;
	std::uint64_t committed = 0, commands = 0, inbox_drops = 0;
};

struct Node {
	NodeId id;
	const Options &o;
	std::vector<std::unique_ptr<Loop>> loops;
	tcp::acceptor acceptor;
	std::unique_ptr<HostTransport> transport;
	std::unique_ptr<Storage> storage, slow_storage;
	std::map<GroupId, std::unique_ptr<Group>> groups;
	asio::steady_timer ticker;
	std::uint64_t election_delay;

	Node(NodeId n, const Options &options, const std::filesystem::path &dir, const std::vector<GroupId> &ids)
		: id(n), o(options), loops(one_loop()), acceptor(loops[0]->io, {asio::ip::address_v4::loopback(), 0}), ticker(loops[0]->io),
		  election_delay(n == 1 ? Timing{}.election_min : Timing{}.election_max) {
		storage = std::make_unique<Storage>(dir / ("node" + std::to_string(n)));
		if (o.slow_group && n == o.slow_node)
			slow_storage = std::make_unique<Storage>(dir / ("node" + std::to_string(n) + "-slow"), o.slow);
		for (auto g : ids) {
			FixedConfiguration c;
			c.cluster[0] = 'T';
			c.configuration[0] = 'G';
			c.local = n;
			c.voters = {1, 2, 3};
			RecoveredState s;
			s.configuration = c;
			auto group = std::make_unique<Group>();
			group->id = g;
			group->core = std::make_unique<Core>(c, std::move(s), o.limits, Timing{}, o.flow);
			group->storage = g == o.slow_group && slow_storage ? slow_storage.get() : storage.get();
			group->track = tracks.at(g).get();
			groups[g] = std::move(group);
		}
	}
	static std::vector<std::unique_ptr<Loop>> one_loop() {
		std::vector<std::unique_ptr<Loop>> r;
		r.push_back(std::make_unique<Loop>());
		return r;
	}
	std::uint16_t port() const { return acceptor.local_endpoint().port(); }
	void start(const std::map<NodeId, PeerAddress> &addresses) {
		transport = transport_of(id, loops, addresses);
		for (auto &[g, group] : groups)
			transport->store(g, id, bench_open());
		transport->on_deliver(0, [this](const Received &r) { return deliver(r); });
		transport->serve(acceptor);
		loops[0]->start();
		asio::post(loops[0]->io, [this] {
			for (auto &[g, group] : groups) {
				step(*group, Tick{ms_now(), election_delay});
				step(*group, Start{});
			}
			tick();
		});
	}
	void stop() {
		quiesce(*transport, loops);
		loops[0]->at([this] { ticker.cancel(); });
		loops[0]->join();
	}
	template <class F> auto at(F f) { return loops[0]->at(std::move(f)); }

	void tick() {
		for (auto &[g, group] : groups) {
			step(*group, Tick{ms_now(), election_delay});
			settle(*group);
		}
		ticker.expires_after(5ms);
		ticker.async_wait([this](std::error_code e) {
			if (!e)
				tick();
		});
	}
	bool deliver(const Received &r) {
		const auto it = groups.find(r.route.group);
		if (it == groups.end())
			return false;
		auto &g = *it->second;
		auto receive = receive_message(r, o.limits);
		if (!receive)
			return false;
		if (g.id == o.slow_group && id == o.slow_node)
			g.track->delivered += entry_bytes(receive->message);
		if (g.core->busy() || !g.inbox.empty()) {
			if (g.inbox.size() >= 256) {
				if (measuring)
					++g.inbox_drops;
				return false;
			}
			g.inbox.push_back(std::move(*receive));
			return true;
		}
		step(g, Tick{ms_now(), election_delay});
		step(g, std::move(*receive));
		settle(g);
		return true;
	}
	void step(Group &g, Event event) {
		for (auto &a : g.core->step(std::move(event)))
			act(g, a);
	}
	void act(Group &g, Action &a) {
		if (auto *p = std::get_if<Persist>(&a)) {
			g.storage->submit(bytes_of(p->batch), [this, &g, token = p->token] {
				asio::post(loops[0]->io, [this, &g, token] {
					step(g, Tick{ms_now(), election_delay});
					step(g, Persisted{token});
					settle(g);
				});
			});
		} else if (auto *s = std::get_if<Send>(&a)) {
			if (g.id == o.slow_group && s->peer == o.slow_node)
				g.track->sent += entry_bytes(s->message);
			send_message(*transport, 0, g.id, id, s->peer, *s, o.limits);
		} else if (auto *placed = std::get_if<ProposalPlaced>(&a)) {
			const auto asked = g.asked.find(placed->request);
			if (asked != g.asked.end()) {
				std::lock_guard hold(g.track->mutex);
				g.track->proposed[placed->index] = asked->second;
				g.asked.erase(asked);
			}
		} else if (auto *c = std::get_if<Committed>(&a)) {
			if (c->entries.empty())
				return;
			if (measuring) {
				const auto now = us_now();
				std::lock_guard hold(g.track->mutex);
				for (const auto &e : c->entries)
					if (e.kind == EntryKind::Command) {
						const auto t = g.track->proposed.find(e.index);
						if (t != g.track->proposed.end())
							g.commit_us.add(now - t->second);
						g.committed += e.payload.size();
						++g.commands;
					}
			}
			step(g, Applied{c->entries.back().index});
		} else if (auto *r = std::get_if<Reject>(&a)) {
			g.asked.erase(r->request);
		}
	}
	// Steps what arrived while the Core persisted, merging one leader's contiguous appends into
	// one Receive as the Worker does, then proposes.
	void settle(Group &g) {
		while (!g.core->busy() && !g.inbox.empty()) {
			Receive r = std::move(g.inbox.front());
			g.inbox.pop_front();
			if (const auto *a = std::get_if<AppendRequest>(&r.message); a && !a->entries.empty()) {
				Index end = a->previous + a->entries.size();
				while (!g.inbox.empty() && r.more.size() < 255) {
					auto &n = g.inbox.front();
					auto *b = std::get_if<AppendRequest>(&n.message);
					if (!b || b->entries.empty() || n.authenticated_peer != r.authenticated_peer || b->term != a->term || b->previous != end)
						break;
					end = b->previous + b->entries.size();
					r.more.push_back(std::move(*b));
					g.inbox.pop_front();
				}
			}
			step(g, Tick{ms_now(), election_delay});
			step(g, std::move(r));
		}
		propose(g);
	}
	void propose(Group &g) {
		if (g.core->role() != Role::Leader || g.core->busy())
			return;
		const auto now = us_now();
		const bool hot = g.id == o.hot;
		if (!hot && o.period_us) {
			if (now < g.next_propose)
				return;
			g.next_propose = std::max(g.next_propose + o.period_us, now - std::min(now, std::uint64_t{100'000}));
		}
		g.asked[++g.request] = now;
		step(g, Propose{g.request, std::string(hot ? o.hot_command : o.command, 'c')});
	}
};

// Three nodes over loopback, each group led by node 1 once elected.
struct Cluster {
	std::vector<std::unique_ptr<Node>> nodes;
	Cluster(const Options &o, const std::filesystem::path &dir, const std::vector<GroupId> &ids) {
		std::filesystem::remove_all(dir);
		std::filesystem::create_directories(dir);
		for (auto g : ids)
			tracks[g] = std::make_unique<Track>();
		for (NodeId n = 1; n <= 3; ++n)
			nodes.push_back(std::make_unique<Node>(n, o, dir, ids));
		for (auto &n : nodes) {
			std::map<NodeId, PeerAddress> peers;
			for (auto &m : nodes)
				if (m != n)
					peers[m->id] = {"127.0.0.1", m->port()};
			n->start(peers);
		}
	}
	~Cluster() {
		for (auto &n : nodes)
			n->stop();
		nodes.clear();
	}
	// Until node 1 leads every group and has committed in each.
	void led(const std::vector<GroupId> &ids) {
		for (int i = 0;; ++i) {
			const bool all = nodes[0]->at([&] {
				return std::all_of(ids.begin(), ids.end(), [&](GroupId g) {
					auto &c = *nodes[0]->groups.at(g)->core;
					return c.role() == Role::Leader && c.committed() > 0;
				});
			});
			if (all)
				return;
			if (i == 2000)
				throw std::runtime_error("node 1 does not lead every group");
			std::this_thread::sleep_for(5ms);
		}
	}
	Group &group(NodeId n, GroupId g) { return *nodes[n - 1]->groups.at(g); }
};

void barrier_bench(const std::filesystem::path &dir, unsigned seconds) {
	std::filesystem::create_directories(dir);
	Storage s(dir / "barrier");
	const std::string batch(2 << 20, 'b');
	measuring = true;
	const auto start = steady::now();
	while (steady::now() - start < std::chrono::seconds(seconds)) {
		std::promise<void> done;
		s.submit(batch, [&] { done.set_value(); });
		done.get_future().wait();
	}
	measuring = false;
	const double secs = double(us_now() - std::uint64_t(std::chrono::duration_cast<std::chrono::microseconds>(start - began).count())) / 1e6;
	std::lock_guard hold(s.stats_mutex);
	std::printf("barrier: %llu barriers of 2 MiB in %.2f s: %.1f MB/s; barrier p50 %llu us p99 %llu us\n", (unsigned long long)s.count, secs,
				double(s.bytes) / secs / 1e6, (unsigned long long)s.barriers.at(0.5), (unsigned long long)s.barriers.at(0.99));
}

std::string storage_line(Storage &s) {
	std::lock_guard hold(s.stats_mutex);
	char line[200];
	std::snprintf(line, sizeof line, "%llu barriers, p50 %llu us p99 %llu us, %.0f KiB each", (unsigned long long)s.count,
				  (unsigned long long)s.barriers.at(0.5), (unsigned long long)s.barriers.at(0.99),
				  s.count ? double(s.bytes) / double(s.count) / 1024 : 0.0);
	return line;
}

void one(const std::filesystem::path &dir, std::uint32_t pipeline, std::size_t frame, std::size_t command, unsigned seconds) {
	Options o;
	o.flow = Flow{pipeline, 4 << 20, frame};
	o.command = command;
	const std::vector<GroupId> ids{1000};
	Cluster c(o, dir, ids);
	c.led(ids);
	std::this_thread::sleep_for(1s);  // warm-up: windows open, buffers grow
	measuring = true;
	const auto start = us_now();
	std::this_thread::sleep_for(std::chrono::seconds(seconds));
	measuring = false;
	const double secs = double(us_now() - start) / 1e6;
	auto [committed, commands, p50, p99] = c.nodes[0]->at([&] {
		auto &g = c.group(1, 1000);
		return std::tuple{g.committed, g.commands, g.commit_us.at(0.5), g.commit_us.at(0.99)};
	});
	std::printf("one: pipeline %u frame %zu command %zu: %.1f MB/s committed (%llu commands in %.2f s), commit p50 %llu us p99 %llu us\n", pipeline,
				frame, command, double(committed) / secs / 1e6, (unsigned long long)commands, secs, (unsigned long long)p50, (unsigned long long)p99);
	for (NodeId n = 1; n <= 3; ++n)
		std::printf("  node %llu storage: %s\n", (unsigned long long)n, storage_line(*c.nodes[n - 1]->storage).c_str());
	const auto dropped = c.nodes[0]->transport->stats(0).dropped.load();
	std::printf("  leader transport dropped %llu frames\n", (unsigned long long)dropped);
}

void many(const std::filesystem::path &dir, unsigned slow, bool hot, unsigned seconds) {
	Options o;
	o.flow = Flow{4, 4 << 20, 256 << 10};
	o.command = 4096;
	o.period_us = 10'000;
	std::vector<GroupId> ids;
	for (GroupId g = 1000; g != 1100; ++g)
		ids.push_back(g);
	o.slow_group = ids[0];
	o.slow = slow;
	o.hot = hot ? ids[0] : 0;
	Cluster c(o, dir, ids);
	c.led(ids);
	std::this_thread::sleep_for(1s);
	auto &track = *tracks.at(ids[0]);
	measuring = true;
	const auto start = us_now();
	std::uint64_t unsent = 0;
	while (us_now() - start < seconds * 1'000'000ull) {
		unsent = std::max(unsent, track.sent.load() - std::min(track.sent.load(), track.delivered.load()));
		std::this_thread::sleep_for(2ms);
	}
	measuring = false;
	const double secs = double(us_now() - start) / 1e6;
	struct Seen {
		Samples us;
		std::uint64_t bytes = 0, drops = 0;
	};
	auto seen = [&](NodeId n, bool slow_group) {
		return c.nodes[n - 1]->at([&] {
			Seen s;
			for (auto g : ids)
				if ((g == ids[0]) == slow_group) {
					auto &x = c.group(n, g);
					s.us.merge(x.commit_us);
					s.bytes += x.committed;
					s.drops += x.inbox_drops;
				}
			return s;
		});
	};
	auto lead = seen(1, false), apply = seen(2, false), lead0 = seen(1, true), apply0 = seen(2, true);
	std::printf("many: slow %u hot %d, 99 groups: leader commit %.2f MB/s p50 %llu us p99 %llu us; node 2 applies %.2f MB/s p50 %llu us p99 %llu us\n", slow,
				int(hot), double(lead.bytes) / secs / 1e6, (unsigned long long)lead.us.at(0.5), (unsigned long long)lead.us.at(0.99),
				double(apply.bytes) / secs / 1e6, (unsigned long long)apply.us.at(0.5), (unsigned long long)apply.us.at(0.99));
	std::printf("  group 0: leader commit %.2f MB/s p99 %llu us; node 2 applies %.2f MB/s p99 %llu us, inbox drops %llu; most bytes sent to node 2 and not delivered %llu (Flow window %llu, transport window %llu + queue %llu)\n",
				double(lead0.bytes) / secs / 1e6, (unsigned long long)lead0.us.at(0.99), double(apply0.bytes) / secs / 1e6,
				(unsigned long long)apply0.us.at(0.99), (unsigned long long)apply0.drops, (unsigned long long)unsent,
				(unsigned long long)o.flow.window_bytes, (unsigned long long)HostTransportOptions{}.window,
				(unsigned long long)HostTransportOptions{}.data_queue);
	std::printf("  node 2 storage: %s; its slow storage: %s; inbox drops elsewhere at node 2: %llu\n", storage_line(*c.nodes[1]->storage).c_str(),
				storage_line(*c.nodes[1]->slow_storage).c_str(), (unsigned long long)apply.drops);
	std::printf("  leader transport dropped %llu frames\n", (unsigned long long)c.nodes[0]->transport->stats(0).dropped.load());
}

// The first group ids from 1000 that place on loop `k` of L.
std::vector<GroupId> groups_on(std::uint32_t L, std::uint32_t k, std::size_t n) {
	std::vector<GroupId> r;
	for (GroupId g = 1000; r.size() < n; ++g)
		if (kronuz::journal::host_detail::loop(kronuz::journal::host_detail::slot(g), L) == k)
			r.push_back(g);
	return r;
}
std::uint64_t sysctl_max(const char *path) {
	std::ifstream in(path);
	std::uint64_t a = 0, b = 0, c = 0;
	in >> a >> b >> c;
	return c;
}

void gap(const std::string &lane, std::size_t frame, unsigned seconds) {
	constexpr std::uint32_t L = 2;
	auto make = [] {
		std::vector<std::unique_ptr<Loop>> r;
		for (std::uint32_t k = 0; k != L; ++k)
			r.push_back(std::make_unique<Loop>());
		return r;
	};
	auto loops1 = make(), loops2 = make();
	tcp::acceptor a1(loops1[0]->io, {asio::ip::address_v4::loopback(), 0}), a2(loops2[0]->io, {asio::ip::address_v4::loopback(), 0});
	auto one = transport_of(1, loops1, {{2, {"127.0.0.1", a2.local_endpoint().port()}}});
	auto two = transport_of(2, loops2, {{1, {"127.0.0.1", a1.local_endpoint().port()}}});
	const auto reader = two->loop_of_node(1), other = 1 - reader;
	const auto bulk = groups_on(L, reader, 4);
	const auto beat = groups_on(L, other, 1)[0];
	for (auto *t : {one.get(), two.get()}) {
		for (auto g : bulk)
			t->store(g, t == one.get() ? 1 : 2, bench_open());
		t->store(beat, t == one.get() ? 1 : 2, bench_open());
	}
	std::atomic<std::uint64_t> delivered{0}, sent{0};
	std::uint64_t last = 0, worst_gap = 0, beats = 0;
	Samples delay;
	two->on_deliver(reader, [&](const Received &r) {
		delivered += r.payload.size();
		return true;
	});
	two->on_deliver(other, [&](const Received &r) {
		const auto now = us_now();
		if (r.payload.size() == 8 && measuring) {
			delay.add(now - kronuz::journal::get64(r.payload));
			if (last)
				worst_gap = std::max(worst_gap, now - last);
			last = now;
			++beats;
		}
		return true;
	});
	one->serve(a1);
	two->serve(a2);
	for (auto *ls : {&loops1, &loops2})
		for (auto &l : *ls)
			l->start();
	const auto lane_of = lane == "bulk" ? LinkLane::Bulk : LinkLane::Control;
	const std::string payload(frame, 'd');
	auto heartbeat = [&] {
		const auto k = one->loop_of_group(beat);
		asio::post(loops1[k]->io, [&, k] {
			std::string p;
			kronuz::journal::put64(p, us_now());
			one->send(k, LinkLane::Control, FrameKind::control, {beat, 1, 2}, 2, p);
		});
	};
	auto top_up = [&] {
		if (lane == "none")
			return;
		// Each group keeps its window and two frames more queued: the feed never runs dry.
		const std::uint64_t target = bulk.size() * (HostTransportOptions{}.window + 2 * frame);
		while (sent - std::min(sent.load(), delivered.load()) < target)
			for (auto g : bulk) {
				const auto k = one->loop_of_group(g);
				asio::post(loops1[k]->io, [&, g, k] { one->send(k, lane_of, FrameKind::data, {g, 1, 2}, 2, payload); });
				sent += frame;
			}
	};
	for (int i = 0; i != 100; ++i) {  // the connections come up
		heartbeat();
		std::this_thread::sleep_for(10ms);
	}
	const auto warm = steady::now();
	while (steady::now() - warm < 1s) {
		top_up();
		std::this_thread::sleep_for(1ms);
	}
	const auto before = delivered.load();
	measuring = true;
	const auto start = us_now();
	for (std::uint64_t next = start; us_now() - start < seconds * 1'000'000ull;) {
		if (us_now() >= next) {
			heartbeat();
			next += 10'000;
		}
		top_up();
		std::this_thread::sleep_for(500us);
	}
	std::this_thread::sleep_for(50ms);
	loops2[other]->at([&] { measuring = false; });
	const double secs = double(us_now() - start) / 1e6;
	const double rate = double(delivered - before) / secs;  // bytes per second
	quiesce(*one, loops1);
	quiesce(*two, loops2);
	const auto wmem = sysctl_max("/proc/sys/net/ipv4/tcp_wmem"), rmem = sysctl_max("/proc/sys/net/ipv4/tcp_rmem");
	const double bound_ms = rate > 0 ? double(frame + HostTransportOptions{}.feed + wmem + rmem) / rate * 1000 : 0;
	std::printf("gap: lane %s frame %zu: data %.1f MB/s; %llu control frames, delay p50 %llu us p99 %llu us max %llu us, longest gap %llu us\n", lane.c_str(), frame,
				rate / 1e6, (unsigned long long)beats, (unsigned long long)delay.at(0.5), (unsigned long long)delay.at(0.99), (unsigned long long)delay.at(1.0),
				(unsigned long long)worst_gap);
	std::printf("  stated bound: one frame %zu + feed %zu + tcp_wmem max %llu + tcp_rmem max %llu at %.1f MB/s = %.1f ms; election_min %llu ms; leader dropped %llu\n",
				frame, HostTransportOptions{}.feed, (unsigned long long)wmem, (unsigned long long)rmem, rate / 1e6, bound_ms,
				(unsigned long long)Timing{}.election_min, (unsigned long long)(one->stats(0).dropped + one->stats(1).dropped));
	for (auto *ls : {&loops1, &loops2})
		for (auto &l : *ls)
			l->join();
}
} // namespace

int main(int argc, char **argv) {
	std::setvbuf(stdout, nullptr, _IOLBF, 0);
	const std::vector<std::string> a(argv + 1, argv + argc);
	auto usage = [] {
		std::fprintf(stderr, "usage: flow_bench barrier DIR SECONDS | one FIXTURE DIR PIPELINE FRAME COMMAND SECONDS | many FIXTURE DIR SLOW HOT SECONDS | gap FIXTURE LANE FRAME SECONDS\n");
		return 2;
	};
	try {
		if (a.size() == 3 && a[0] == "barrier") {
			barrier_bench(a[1], unsigned(std::stoul(a[2])));
			return 0;
		}
		if (a.size() < 2)
			return usage();
		fixture = a[1];
		if (a.size() == 7 && a[0] == "one")
			one(a[2], std::uint32_t(std::stoul(a[3])), std::stoul(a[4]), std::stoul(a[5]), unsigned(std::stoul(a[6])));
		else if (a.size() == 6 && a[0] == "many")
			many(a[2], unsigned(std::stoul(a[3])), a[4] == "1", unsigned(std::stoul(a[5])));
		else if (a.size() == 5 && a[0] == "gap")
			gap(a[2], std::stoul(a[3]), unsigned(std::stoul(a[4])));
		else
			return usage();
	} catch (const std::exception &e) {
		std::fprintf(stderr, "flow_bench: %s\n", e.what());
		return 1;
	}
	return 0;
}
