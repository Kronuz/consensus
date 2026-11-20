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

// One consensus node as its own process, for bench/netlab/bench.py: a Driver over a journal on
// DIR and the host transport (mutual TLS with the test fixture's certificates), one voter of a
// fixed group of VOTERS. It takes commands on stdin and writes one JSON line per event on stdout,
// each stamped with CLOCK_MONOTONIC microseconds ("t"), so the harness compares nodes' clocks.
//   consensus_netlab_node FIXTURE DIR ID VOTERS PORT J=HOST:PORT...
// Commands:
//   w ID KEY VALUE BYTES      propose "KEY=VALUE" padded to BYTES, answered once committed here
//   r ID KEY                  a linearizable read (Driver::read) of KEY's value
//   load ID SECONDS BYTES INFLIGHT MAX
//                             closed loop: INFLIGHT proposals of BYTES at once, for SECONDS or MAX
//                             commits: commits, bytes, and commit latency percentiles
//   rpc ID PEER BYTES SECONDS MAX
//                             RPC-lane echo calls of BYTES to PEER, one at a time: latency percentiles
//   quit
// Events: ready, role (each RoleChanged), w, r, load, rpc. A refused call answers ok 0 with its
// reason; "superseded" and "rejected" are definite failures, anything else is unknown.
#include "consensus/driver.h"
#include "consensus/transport/host_transport.h"
#include <completion/posix.h>
#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>
#include <openssl/pem.h>
#include <signal.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace kronuz::consensus::transport;
using namespace kronuz::consensus;
using namespace std::chrono_literals;
using tcp = asio::ip::tcp;
using steady = std::chrono::steady_clock;
namespace j = kronuz::journal;

namespace {
std::filesystem::path fixture;
const char *names[] = {"", "one", "two", "three", "four", "five"};
constexpr GroupId group = 1;
constexpr rpc::MethodId echo = 7;

std::uint64_t now_us() { return std::uint64_t(std::chrono::duration_cast<std::chrono::microseconds>(steady::now().time_since_epoch()).count()); }

std::mutex out_mutex;
// One JSON line: `fields` is the object's body after "ev" and "t".
void emit(const std::string &ev, const std::string &fields = {}) {
	std::lock_guard hold(out_mutex);
	std::printf("{\"ev\":\"%s\",\"t\":%llu%s%s}\n", ev.c_str(), (unsigned long long)now_us(), fields.empty() ? "" : ",", fields.c_str());
	std::fflush(stdout);
}
std::string json_string(std::string_view s) {
	std::string r = "\"";
	for (char c : s) {
		if (c == '"' || c == '\\')
			r += '\\';
		if (static_cast<unsigned char>(c) >= 0x20)
			r += c;
	}
	return r + "\"";
}

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
	std::uint64_t at(double q) {
		if (v.empty())
			return 0;
		std::sort(v.begin(), v.end());
		return v[std::size_t(q * double(v.size() - 1))];
	}
	std::string fields() {
		std::ostringstream s;
		s << "\"n\":" << v.size() << ",\"p50\":" << at(0.5) << ",\"p90\":" << at(0.9) << ",\"p99\":" << at(0.99) << ",\"max\":" << at(1.0);
		return s.str();
	}
};

std::string why(std::exception_ptr e) {
	try {
		std::rethrow_exception(e);
	} catch (const rejected &r) {
		static const char *reasons[] = {"Busy",	   "NotLeader",			"NotReady",			  "LogFull",		 "TooLarge",	  "DuplicateRequest",
										"InvalidCheckpoint", "InvalidRepair", "LeaderMustTransfer", "InvalidMembership", "InvalidTarget"};
		const auto i = std::size_t(r.reason);
		return std::string("rejected ") + (i < std::size(reasons) ? reasons[i] : "?");
	} catch (const superseded &) {
		return "superseded";
	} catch (const std::exception &x) {
		return std::string("unknown ") + x.what();
	} catch (...) {
		return "unknown";
	}
}

const j::TurnLogOptions turn_options{8u << 20, 32u << 10, 1, 4, 1u << 20};
const j::AdmissionLimits admission{{1024u << 20, 65536}, {256 * 1024, 3}, {64u << 20, 4}, 64, 3};

struct Node;
struct Sink {
	Node *node = nullptr;
	void operator()(GroupId g, Send n);
	void operator()(GroupId g, Committed n);
	void operator()(GroupId g, RoleChanged n);
	template <class N> void operator()(GroupId, N &&) {}
};

struct Node {
	NodeId id;
	std::uint32_t voters;
	asio::io_context io;
	task::asio_scheduler scheduler{io.get_executor()};
	asio::executor_work_guard<asio::io_context::executor_type> work = asio::make_work_guard(io);
	tcp::acceptor acceptor;
	std::unique_ptr<HostTransport> transport;
	std::unique_ptr<j::Host> journal;
	j::InlineCompletion completion;
	Sink sink;
	std::unique_ptr<Driver<Sink>> driver;
	task::cancel_source stop;
	std::atomic<bool> turns_done{false}, driver_done{false};
	std::thread thread;
	// The application: each key's latest value, and the last index applied.
	std::map<std::string, std::string> values;
	Index applied = 0;

	Node(NodeId n, std::uint32_t N, const std::filesystem::path &dir, std::uint16_t port, const std::map<NodeId, PeerAddress> &peers)
		: id(n), voters(N), acceptor(io, {asio::ip::address_v4::any(), port}) {
		auto tls = context(names[n]);
		std::map<NodeId, Fingerprint> pins;
		for (NodeId m = 1; m <= N; ++m)
			pins[m] = pin_of(names[m]);
		HostTransportOptions options;
		options.member_of = [N](GroupId, NodeId incarnation) -> std::optional<Member> {
			if (incarnation < 1 || incarnation > N)
				return std::nullopt;
			return Member{incarnation, incarnation, MemberRole::Voter};
		};
		Identity cluster{};
		cluster.fill('K');
		transport = std::make_unique<HostTransport>(std::vector<HostLoop>{{io.get_executor(), task::scheduler_ref::of(scheduler)}}, tls,
													NodePins(n, pins, tls->native_handle()), cluster, peers, options);
		std::filesystem::create_directories(dir);
		::chmod(dir.c_str(), 0700);
		journal = std::make_unique<j::Host>(std::make_shared<j::PosixIO>(dir), j::Identity{'N', char('0' + n)}, j::HostOptions{1, turn_options});
		sink.node = this;
		auto ref = task::scheduler_ref::of(scheduler);
		driver = std::make_unique<Driver<Sink>>(Seams{ref, j::completion_ref::of(completion), *journal, 0, n * 31}, sink);
		task::spawn(ref, journal->driver(0).run(j::completion_ref::of(completion), stop.token()), [this](std::exception_ptr) { turns_done = true; });
		task::spawn(ref, driver->run(stop.token()), [this](std::exception_ptr) { driver_done = true; });
		transport->on_deliver(0, [this](const Received &r) {
			auto receive = receive_message(r);
			if (!receive || r.route.group != group)
				return false;
			driver->receive(group, std::move(*receive));
			return true;
		});
		serve_echo();
		transport->serve(acceptor);
		thread = std::thread([this] { io.run(); });
		std::promise<void> opened;
		auto done = opened.get_future();
		asio::post(io, [&, this] {
			GroupOptions o;
			o.configuration.local = id;
			for (NodeId m = 1; m <= N; ++m)
				o.configuration.voters.push_back(m);
			o.configuration.cluster[0] = 'T';
			o.configuration.configuration[0] = 'G';
			o.admission = admission;
			o.limits.log_bytes = 768u << 20;  // no checkpoints: a run's whole log stays retained
			driver->open(group, std::move(o));
			auto opening = journal->status(group) == j::GroupStatus::stored ? driver->recover(group, [](j::ArtifactReader &) {})
																			 : driver->create(group, j::Identity{'G', char('0' + id)});
			task::spawn(task::scheduler_ref::of(scheduler), std::move(opening), [this, &opened](std::exception_ptr e) {
				if (e) {
					emit("error", "\"why\":" + json_string(why(e)));
					std::_Exit(3);
				}
				GroupOpen open;
				open.cluster.fill('T');
				open.group.fill('G');
				open.epoch.fill('E');
				open.profile.fill(0x5A);
				transport->store(group, id, open);
				opened.set_value();
			});
		});
		done.wait();
	}
	template <class F> void post(F f) { asio::post(io, std::move(f)); }
	void spawn(task::task<void> t) {
		task::spawn(task::scheduler_ref::of(scheduler), std::move(t), [](std::exception_ptr) {});
	}

	// Method 7: echoes its request once read.
	void serve_echo() {
		transport->handle(echo, [this](RpcCall c, const rpc::Incoming &in) -> std::optional<rpc::Events> {
			auto got = std::make_shared<std::string>();
			rpc::Events e;
			const auto total = in.total;
			e.body = [this, c, got, total](std::string_view bytes, bool end) {
				got->append(bytes);
				transport->consume(c, bytes.size());
				if (end) {
					transport->total(c, total);
					transport->write(c, *got, true);
				}
			};
			e.done = [](const rpc::Status &) {};
			return e;
		});
	}

	void command(const std::string &line);
	task::task<void> write(std::string id, std::string payload);
	task::task<void> read(std::string id, std::string key);
	task::task<void> load(std::string id, double seconds, std::size_t bytes, std::size_t inflight, std::uint64_t max);
	void rpc_calls(std::string id, NodeId peer, std::size_t bytes, double seconds, std::uint64_t max);

	void shutdown() {
		std::promise<void> stopped;
		auto done = stopped.get_future();
		transport->stop();
		for (int i = 0; i != 400; ++i) {
			std::promise<bool> drained;
			auto d = drained.get_future();
			post([&] { drained.set_value(transport->drained(0)); });
			if (d.get())
				break;
			std::this_thread::sleep_for(5ms);
		}
		post([this] { stop.request(task::cancel_reason::shutdown); });
		for (int i = 0; i != 400 && !(turns_done && driver_done); ++i)
			std::this_thread::sleep_for(5ms);
		post([&] {
			driver.reset();
			journal.reset();
			stopped.set_value();
		});
		done.wait();
		work.reset();
		io.stop();
		thread.join();
	}
};

void Sink::operator()(GroupId g, Send n) { send_message(*node->transport, 0, g, node->id, n.peer, n); }
void Sink::operator()(GroupId g, Committed n) {
	Index last = 0;
	for (auto &e : n.entries) {
		last = e.index;
		if (e.index <= node->applied || e.kind != EntryKind::Command)
			continue;
		const std::string_view p(e.payload);
		const auto eq = p.find('=');
		if (eq != std::string_view::npos) {
			const auto end = p.find('\0', eq);
			node->values[std::string(p.substr(0, eq))] = std::string(p.substr(eq + 1, end == std::string_view::npos ? end : end - eq - 1));
		}
	}
	if (last > node->applied) {
		node->applied = last;
		node->post([n = node, g, last] {
			if (n->driver)
				n->driver->applied(g, Applied{last});
		});
	}
}
void Sink::operator()(GroupId, RoleChanged n) {
	static const char *roles[] = {"follower", "candidate", "leader", "fenced"};
	emit("role", std::string("\"role\":\"") + roles[std::size_t(n.role)] + "\",\"term\":" + std::to_string(n.term) + ",\"leader\":" + std::to_string(n.leader));
}

task::task<void> Node::write(std::string id, std::string payload) {
	const auto t0 = now_us();
	std::exception_ptr error;
	Placement placed;
	try {
		placed = co_await driver->propose(group, std::move(payload));
		co_await driver->committed(group, placed);
	} catch (...) {
		error = std::current_exception();
	}
	if (error)
		emit("w", "\"id\":" + id + ",\"ok\":0,\"why\":" + json_string(why(error)));
	else
		emit("w", "\"id\":" + id + ",\"ok\":1,\"index\":" + std::to_string(placed.index) + ",\"us\":" + std::to_string(now_us() - t0));
}

task::task<void> Node::read(std::string id, std::string key) {
	std::exception_ptr error;
	ReadReady ready{};
	try {
		ready = co_await driver->read(group);
	} catch (...) {
		error = std::current_exception();
	}
	if (error) {
		emit("r", "\"id\":" + id + ",\"ok\":0,\"why\":" + json_string(why(error)));
		co_return;
	}
	const auto it = values.find(key);
	emit("r", "\"id\":" + id + ",\"ok\":1,\"index\":" + std::to_string(ready.index) + ",\"value\":" + (it == values.end() ? "null" : json_string(it->second)));
}

// A closed loop of `inflight` proposers sharing one deadline and one budget of commits.
struct Load {
	std::uint64_t until, max, started = 0, commits = 0, bytes = 0, begin = now_us(), end = 0;
	std::size_t live = 0;
	std::map<std::string, std::uint64_t> failures;
	Samples latency;
	std::string id;
};
task::task<void> proposer(Node *n, std::shared_ptr<Load> load, std::size_t size) {
	const std::string payload(size, 'x');
	while (now_us() < load->until && load->started < load->max) {
		++load->started;
		const auto t0 = now_us();
		std::exception_ptr error;
		try {
			auto placed = co_await n->driver->propose(group, payload);
			co_await n->driver->committed(group, placed);
		} catch (...) {
			error = std::current_exception();
		}
		if (error) {
			++load->failures[why(error)];
			if (why(error) != "rejected Busy")
				break;
			continue;
		}
		load->latency.v.push_back(now_us() - t0);
		++load->commits;
		load->bytes += size;
	}
	load->end = std::max(load->end, now_us());
	if (--load->live == 0) {
		std::string failed;
		for (auto &[k, v] : load->failures)
			failed += (failed.empty() ? "" : ",") + json_string(k) + ":" + std::to_string(v);
		const double seconds = double(load->end - load->begin) / 1e6;
		std::ostringstream s;
		s << "\"id\":" << load->id << ",\"commits\":" << load->commits << ",\"bytes\":" << load->bytes << ",\"seconds\":" << seconds << ","
		  << load->latency.fields() << ",\"failures\":{" << failed << "}";
		emit("load", s.str());
	}
}
task::task<void> Node::load(std::string id, double seconds, std::size_t bytes, std::size_t inflight, std::uint64_t max) {
	auto l = std::make_shared<Load>();
	l->until = now_us() + std::uint64_t(seconds * 1e6);
	l->max = max;
	l->id = id;
	l->live = inflight;
	for (std::size_t i = 0; i != inflight; ++i)
		spawn(proposer(this, l, bytes));
	co_return;
}

// One call at a time on the lane to `peer`, each starting when the last ends.
struct Calls {
	Node *node;
	NodeId peer;
	std::string id, request;
	std::uint64_t until, max, failed = 0;
	Samples latency;
	void next(std::shared_ptr<Calls> self) {
		if (now_us() >= until || latency.v.size() + failed >= max) {
			emit("rpc", "\"id\":" + id + ",\"failed\":" + std::to_string(failed) + "," + latency.fields());
			return;
		}
		auto &t = *node->transport;
		const auto t0 = now_us();
		auto got = std::make_shared<std::uint64_t>(0);
		auto call = std::make_shared<RpcCall>();
		rpc::Events e;
		e.body = [&t, got, call](std::string_view bytes, bool) {
			*got += bytes.size();
			t.consume(*call, bytes.size());
		};
		e.done = [self, t0, got](const rpc::Status &s) {
			if (s.outcome == rpc::Outcome::ok && *got == self->request.size())
				self->latency.v.push_back(now_us() - t0);
			else
				++self->failed;
			self->node->post([self] { self->next(self); });
		};
		*call = t.call(peer, echo, {.deadline = rpc::Clock::now() + 10s, .total = request.size()}, request, std::move(e));
	}
};
void Node::rpc_calls(std::string id, NodeId peer, std::size_t bytes, double seconds, std::uint64_t max) {
	auto c = std::make_shared<Calls>();
	c->node = this;
	c->peer = peer;
	c->id = id;
	c->request.assign(std::min<std::size_t>(bytes, 1024), 'q');
	c->until = now_us() + std::uint64_t(seconds * 1e6);
	c->max = max;
	c->next(c);
}

void Node::command(const std::string &line) {
	std::istringstream in(line);
	std::string op, id;
	in >> op >> id;
	if (op == "w") {
		std::string key, value;
		std::size_t bytes = 0;
		in >> key >> value >> bytes;
		std::string payload = key + "=" + value;
		if (payload.size() < bytes)
			payload.resize(bytes, '\0');
		post([this, id, payload] { spawn(write(id, payload)); });
	} else if (op == "r") {
		std::string key;
		in >> key;
		post([this, id, key] { spawn(read(id, key)); });
	} else if (op == "load") {
		double seconds = 0;
		std::size_t bytes = 0, inflight = 1;
		std::uint64_t max = 0;
		in >> seconds >> bytes >> inflight >> max;
		post([=, this] { spawn(load(id, seconds, bytes, std::max<std::size_t>(inflight, 1), max)); });
	} else if (op == "rpc") {
		NodeId peer = 0;
		std::size_t bytes = 0;
		double seconds = 0;
		std::uint64_t max = 0;
		in >> peer >> bytes >> seconds >> max;
		post([=, this] { rpc_calls(id, peer, bytes, seconds, max); });
	} else {
		emit("error", "\"why\":" + json_string("unknown command " + line));
	}
}
} // namespace

int main(int argc, char **argv) {
	if (argc < 6) {
		std::fprintf(stderr, "usage: %s FIXTURE DIR ID VOTERS PORT J=HOST:PORT...\n", argv[0]);
		return 2;
	}
	std::signal(SIGPIPE, SIG_IGN);
	fixture = argv[1];
	const std::filesystem::path dir = argv[2];
	const NodeId id = std::stoull(argv[3]);
	const std::uint32_t voters = std::uint32_t(std::stoul(argv[4]));
	const auto port = std::uint16_t(std::stoul(argv[5]));
	if (voters < 1 || voters > 5 || id < 1 || id > voters) {
		std::fprintf(stderr, "1 to 5 voters, and ID among them\n");
		return 2;
	}
	std::map<NodeId, PeerAddress> peers;
	for (int i = 6; i < argc; ++i) {
		const std::string a = argv[i];
		const auto eq = a.find('='), colon = a.rfind(':');
		if (eq == std::string::npos || colon == std::string::npos || colon < eq) {
			std::fprintf(stderr, "a peer is J=HOST:PORT, not %s\n", a.c_str());
			return 2;
		}
		peers[std::stoull(a.substr(0, eq))] = {a.substr(eq + 1, colon - eq - 1), std::uint16_t(std::stoul(a.substr(colon + 1)))};
	}
	try {
		Node node(id, voters, dir, port, peers);
		emit("ready", "\"node\":" + std::to_string(id));
		for (std::string line; std::getline(std::cin, line);) {
			if (line == "quit")
				break;
			if (!line.empty())
				node.command(line);
		}
		node.shutdown();
	} catch (const std::exception &e) {
		emit("error", "\"why\":" + json_string(e.what()));
		return 1;
	}
	return 0;
}
