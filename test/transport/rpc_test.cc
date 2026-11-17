// The RPC lane (consensus/transport/rpc.h): the codec's rules 1-5, the call table's rules 6-10, a
// call's life, credit and pools, the model's eight mutants (rpc-lane-model.py) as cases named for
// them, and two lanes over pipes on two threads (run under TSan). `seed DIR` writes the fuzz seeds.
#include "consensus/transport/rpc.h"
#include "async_pipe.h"

#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <future>
#include <list>
#include <random>
#include <thread>

using namespace kronuz::consensus::transport::rpc;
using namespace std::chrono_literals;
namespace journal = kronuz::journal;

namespace {
#define CHECK(x) ((x) ? void() : fail(#x, __LINE__))
[[noreturn]] void fail(const char *what, int line) {
	std::fprintf(stderr, "rpc_test.cc:%d: %s\n", line, what);
	std::abort();
}
template <class F> std::string thrown(F &&f) {
	try {
		f();
	} catch (const std::exception &e) {
		return e.what();
	}
	return "";
}
#define REJECTS(expr, rule) CHECK(thrown([&] { (void)(expr); }).find(rule) != std::string::npos)
std::string pattern(std::size_t n, char seed) {
	std::string s(n, 0);
	for (std::size_t i = 0; i < n; ++i)
		s[i] = char(seed + i * 131 % 251);
	return s;
}
std::string patch(std::string bytes, std::size_t at, unsigned char value) {
	bytes[at] = char(value);
	return bytes;
}
Frame with(Frame f, auto edit) {
	edit(f);
	return f;
}
// Encodes without encode's checks, for frames it refuses.
std::string raw(const Frame &f) {
	std::string out = magic(1) + char(f.kind) + char(f.end) + std::string(2, '\0');
	journal::put32(out, f.word);
	journal::put64(out, f.call);
	if (f.kind == Kind::call)
		for (auto v : {f.timeout_ms, f.window, f.total})
			journal::put64(out, v);
	if (f.kind == Kind::body)
		for (auto v : {f.offset, f.total})
			journal::put64(out, v);
	if (f.kind == Kind::credit)
		journal::put64(out, f.limit);
	return out + std::string(f.bytes);
}

const Frame call_frame{.kind = Kind::call, .end = true, .word = 7, .call = 1, .timeout_ms = 500, .window = 65536, .total = 4, .bytes = "ping"};
const Frame body_frame{.kind = Kind::body, .call = 1, .offset = 4, .total = 10, .bytes = "abc"};
const Frame codec_frames[] = {
	call_frame,
	body_frame,
	{.kind = Kind::body, .end = true, .call = 3, .offset = 7, .total = 10, .bytes = "xyz"},
	{.kind = Kind::body, .end = true, .call = 3, .offset = 9, .total = unknown_total},
	{.kind = Kind::credit, .call = 2, .limit = 1 << 20},
	{.kind = Kind::cancel, .word = 7, .call = 5, .bytes = "source"},
	{.kind = Kind::error, .word = 256, .call = 2, .bytes = "missing"},
	{.kind = Kind::drain, .call = 0},
};

void codec() {
	CHECK(magic(1) == "DTRP0001" && magic(12) == "DTRP0012");
	const std::size_t sizes[] = {52, 43, 43, 40, 32, 30, 31, 24};
	for (std::size_t i = 0; i < std::size(codec_frames); ++i) {
		const auto &want = codec_frames[i];
		const auto bytes = encode(want);
		const auto f = decode(bytes);
		CHECK(bytes.size() == sizes[i] && bytes.starts_with("DTRP0001") && encode(f) == bytes);
		CHECK(f.kind == want.kind && f.end == want.end && f.word == want.word && f.call == want.call);
		CHECK(f.timeout_ms == want.timeout_ms && f.window == want.window && f.offset == want.offset);
		CHECK(f.total == want.total && f.limit == want.limit && f.bytes == want.bytes);
	}
	const std::string chunk(chunk_bytes, 'c');
	CHECK(encode(with(call_frame, [&](Frame &f) { f.bytes = chunk, f.total = chunk_bytes; })).size() == 65584);
	CHECK(decode(encode(call_frame, 3), 3).word == 7);
}

// D3 rules 1-5, and the parts of rule 7 a frame alone shows, each with values.
void decoder_rules() {
	const auto call = encode(call_frame), body = encode(body_frame);
	const auto credit = encode({.kind = Kind::credit, .call = 2, .limit = 9}), drain = encode({.kind = Kind::drain});
	// 1: the magic and version.
	REJECTS(decode(call, 2), "rule 1");
	REJECTS(decode(patch(call, 0, 'X')), "rule 1");
	REJECTS(decode(drain.substr(0, 23)), "rule 1");
	// 2: kind 0 and 7, flag bits other than end, end off Call and Body, bytes 10-11.
	REJECTS(decode(patch(call, 8, 0)), "rule 2: kind");
	REJECTS(decode(patch(call, 8, 7)), "rule 2: kind");
	REJECTS(decode(patch(call, 9, 3)), "rule 2: reserved");
	REJECTS(decode(patch(call, 10, 1)), "rule 2: reserved");
	REJECTS(decode(patch(call, 11, 0x80)), "rule 2: reserved");
	REJECTS(decode(patch(credit, 9, 1)), "rule 2: end");
	REJECTS(decode(patch(drain, 9, 1)), "rule 2: end");
	// 3: word zero on Call, Cancel, Error, nonzero elsewhere; Error 7-255; Cancel outside 3, 4, 5, 7.
	REJECTS(decode(patch(call, 12, 0)), "rule 3: word");
	REJECTS(decode(patch(credit, 12, 1)), "rule 3: word");
	REJECTS(decode(patch(drain, 15, 1)), "rule 3: word");
	REJECTS(decode(raw({.kind = Kind::cancel, .call = 1})), "rule 3: word");
	for (std::uint32_t code : {7u, 100u, 255u})
		REJECTS(decode(raw({.kind = Kind::error, .word = code, .call = 2})), "rule 3: code");
	for (std::uint32_t code : {1u, 2u, 6u, 8u, 256u})
		REJECTS(decode(raw({.kind = Kind::cancel, .word = code, .call = 1})), "rule 3: code");
	for (std::uint32_t code : {1u, 6u, 256u, 70000u})
		CHECK(decode(raw({.kind = Kind::error, .word = code, .call = 2})).word == code);
	for (std::uint32_t code : {3u, 4u, 5u, 7u})
		CHECK(decode(raw({.kind = Kind::cancel, .word = code, .call = 1})).word == code);
	// 4: the length per kind.
	REJECTS(decode(credit + "x"), "rule 4");
	REJECTS(decode(credit.substr(0, 31)), "rule 4");
	REJECTS(decode(drain + "x"), "rule 4");
	REJECTS(decode(call.substr(0, 47)), "rule 4");
	REJECTS(decode(body.substr(0, 39)), "rule 4");
	const std::string text(1025, 't'), over(chunk_bytes + 1, 'b');
	REJECTS(decode(raw({.kind = Kind::cancel, .word = 4, .call = 1, .bytes = text})), "rule 4");
	CHECK(decode(raw({.kind = Kind::cancel, .word = 4, .call = 1, .bytes = text.substr(1)})).bytes.size() == 1024);
	REJECTS(decode(raw({.kind = Kind::call, .word = 7, .call = 1, .timeout_ms = 1, .window = 1, .total = unknown_total, .bytes = over})), "rule 4");
	REJECTS(decode(raw({.kind = Kind::body, .call = 1, .total = unknown_total, .bytes = over})), "rule 4");
	// 5: id 0 off Drain; a Call's timeout 0 or window 0, inline past total, end iff its known total is inline.
	REJECTS(decode(raw(with(body_frame, [](Frame &f) { f.call = 0; }))), "rule 5: call id");
	REJECTS(decode(raw({.kind = Kind::credit, .limit = 1})), "rule 5: call id");
	REJECTS(decode(raw(with(call_frame, [](Frame &f) { f.timeout_ms = 0; }))), "rule 5: timeout");
	REJECTS(decode(raw(with(call_frame, [](Frame &f) { f.window = 0; }))), "rule 5: timeout or window");
	REJECTS(decode(raw(with(call_frame, [](Frame &f) { f.total = 3; }))), "rule 5: inline past total");
	REJECTS(decode(raw(with(call_frame, [](Frame &f) { f.total = 10; }))), "rule 5: end");
	REJECTS(decode(raw(with(call_frame, [](Frame &f) { f.end = false; }))), "rule 5: end");
	CHECK(decode(encode(with(call_frame, [](Frame &f) { f.end = false, f.total = 10; }))).total == 10);
	CHECK(decode(encode(with(call_frame, [](Frame &f) { f.total = unknown_total; }))).end);
	// 7.2, 7.3 (overflow-safe: offset + n would wrap), 7.5.
	REJECTS(decode(raw(with(body_frame, [](Frame &f) { f.bytes = {}; }))), "rule 7.2");
	REJECTS(decode(raw(with(body_frame, [](Frame &f) { f.offset = 11; }))), "rule 7.3");
	REJECTS(decode(raw(with(body_frame, [](Frame &f) { f.offset = 8; }))), "rule 7.3");
	REJECTS(decode(raw({.kind = Kind::body, .call = 1, .offset = unknown_total - 1, .total = unknown_total, .bytes = "abc"})), "rule 7.3");
	REJECTS(decode(raw(with(body_frame, [](Frame &f) { f.end = true; }))), "rule 7.5");
	REJECTS(decode(raw(with(body_frame, [](Frame &f) { f.offset = 7; }))), "rule 7.5");
	CHECK(decode(encode(with(body_frame, [](Frame &f) { f.total = unknown_total; }))).total == unknown_total);
	REJECTS(encode(with(call_frame, [](Frame &f) { f.window = 0; })), "rule 5");
}

// One call at one end. `out` is this end's stream, fed as credit allows; `hold` stops consuming.
struct Peer {
	Handle h = 0;
	std::string got, out;
	std::size_t queued = 0;
	bool hold = false, ended = false, end_sent = false;
	int completions = 0;
	Status status;
	bool ok() const { return completions == 1 && status.outcome == Outcome::ok; }
	bool ended_with(Outcome o, Code code) const { return completions == 1 && status.outcome == o && status.code == std::uint32_t(code); }
};
void feed(Lane &lane, Peer &p) {
	while (!p.end_sent) {
		const std::size_t k = std::min(lane.sendable(p.h), p.out.size() - p.queued);
		if (!k && p.queued < p.out.size())
			return;
		p.end_sent = p.queued + k == p.out.size();
		lane.send(p.h, p.out.substr(p.queued, k), p.end_sent);
		p.queued += k;
	}
}
// A server answers `response` bytes once it has the whole request.
Events events(Lane &lane, Peer &p, std::size_t response = 0, bool serving = false) {
	return {.body = [&lane, &p, response, serving](std::string_view bytes, bool end) {
				p.got.append(bytes);
				p.ended = end;
				if (!p.hold)
					lane.consume(p.h, bytes.size());
				if (serving && end) {
					p.out = pattern(response, 's');
					lane.total(p.h, p.out.size());
					feed(lane, p);
				}
			},
			.credit = [&lane, &p] { feed(lane, p); },
			.done = [&p](const Status &s) { ++p.completions, p.status = s; }};
}

// Host `a` dials and calls; host `b` accepts and serves method 7. One thread; flow() moves frames.
struct Net;
struct Host {
	ByteBudget serving, calling, outgoing;
	Lane lane;
	Host(Net &net, bool dialer, std::size_t pool, LaneOptions options);
};
struct Net {
	Clock::time_point time = Clock::time_point{} + 1h;
	std::deque<std::function<void()>> posted;
	std::size_t response = 5;
	bool server_holds = false;
	std::list<Peer> callers, servers;
	std::map<Kind, int> from_a, from_b;
	std::unique_ptr<Host> a, b;
	explicit Net(std::size_t pool = 1 << 20, LaneOptions options = {.window = 128 << 10})
		: a(std::make_unique<Host>(*this, true, pool, options)), b(std::make_unique<Host>(*this, false, pool, options)) {}
	// A call of `request`, its first `inline_bytes` in the Call; `open` leaves the request unfinished.
	Peer &call(std::string request, std::size_t inline_bytes, std::size_t window = 128 << 10, MethodId method = 7, bool open = false) {
		auto &p = callers.emplace_back();
		const bool streamed = inline_bytes < request.size() || open;
		p.out = request;
		p.queued = inline_bytes;
		p.end_sent = !streamed || open;
		std::optional<std::uint64_t> total;
		if (streamed)
			total = open ? unknown_total : request.size();
		p.h = a->lane.call(method, {.deadline = time + 10s, .window = window, .total = total}, request.substr(0, inline_bytes), events(a->lane, p));
		return p;
	}
	Peer &open(std::size_t window = 128 << 10) { return call("ping", 4, window, 7, true); }
	void run() {
		while (!posted.empty()) {
			auto f = std::move(posted.front());
			posted.pop_front();
			f();
		}
	}
	bool move(Lane &from, Lane &to, std::map<Kind, int> &count) {
		bool moved = false;
		for (; auto f = from.next(); moved = true) {
			++count[decode(f->bytes).kind];
			to.receive(f->bytes);
		}
		return moved;
	}
	bool ab() { return move(a->lane, b->lane, from_a); }
	bool ba() { return move(b->lane, a->lane, from_b); }
	// Moves every queued frame both ways until both lanes are quiet.
	void flow() {
		for (bool moved = true; moved;) {
			moved = !posted.empty();
			run();
			moved |= ab();
			moved |= ba();
		}
	}
	bool zero() const {
		for (auto *h : {a.get(), b.get()})
			if (h->serving.used() || h->calling.used() || h->outgoing.used())
				return false;
		return true;
	}
};
Host::Host(Net &net, bool dialer, std::size_t pool, LaneOptions options)
	: serving(pool), calling(pool), outgoing(pool),
	  lane(
		  dialer, {serving, calling, outgoing}, options,
		  [&net, dialer](const Incoming &in) -> std::optional<Events> {
			  if (dialer || in.method != 7)
				  return std::nullopt;
			  auto &p = net.servers.emplace_back();
			  p.h = in.handle;
			  p.hold = net.server_holds;
			  return events(net.b->lane, p, net.response, true);
		  },
		  [&net](std::function<void()> f) { net.posted.push_back(std::move(f)); }, [] {}, [&net] { return net.time; }) {}

std::vector<Frame> queued(Lane &lane, std::vector<std::string> &keep) {
	while (auto f = lane.next())
		keep.push_back(std::move(f->bytes));
	std::vector<Frame> out;
	for (auto &bytes : keep)
		out.push_back(decode(bytes));
	return out;
}

// D3 rules 6-10, at the acceptor `b`, fed frames as if from `a`.
void table_rules() {
	Net net(1 << 20, {.window = 4});
	net.response = 70000;
	net.server_holds = true;
	auto from_a = [&](const Frame &f) { net.b->lane.receive(encode(f)); };
	auto id = [](Frame f, std::uint64_t call) {
		f.call = call;
		return f;
	};
	// 6: direction by the id's parity; Call ids increase; other ids are allocated.
	REJECTS(from_a({.kind = Kind::cancel, .word = 4, .call = 2}), "rule 6: direction");
	REJECTS(from_a({.kind = Kind::error, .word = 1, .call = 1}), "rule 6: direction");
	REJECTS(from_a(id(call_frame, 2)), "rule 6: direction");
	REJECTS(from_a({.kind = Kind::credit, .call = 1, .limit = 5}), "rule 6: id not allocated");
	REJECTS(from_a({.kind = Kind::credit, .call = 2, .limit = 5}), "rule 6: id not allocated");
	const Frame streamed = with(call_frame, [](Frame &f) { f.call = 3, f.end = false, f.total = 10; });
	from_a(streamed); // live, request credit 4 + 4
	REJECTS(from_a(id(call_frame, 1)), "rule 6: Call id did not increase");
	REJECTS(from_a(id(call_frame, 3)), "rule 6: Call id did not increase");
	REJECTS(from_a(id(body_frame, 5)), "rule 6: id not allocated");
	// 7: the next byte, the stream's first total, within credit, nothing after the end.
	REJECTS(from_a(with(id(body_frame, 3), [](Frame &f) { f.offset = 5, f.bytes = "ab"; })), "rule 7.1");
	REJECTS(from_a(with(id(body_frame, 3), [](Frame &f) { f.total = 11; })), "rule 7.4");
	from_a(id(body_frame, 3)); // bytes 4-6 of the credit's 8
	REJECTS(from_a(with(id(body_frame, 3), [](Frame &f) { f.offset = 7, f.end = true; })), "rule 7.6");
	from_a(id(call_frame, 5)); // whole request; its response waits for credit past 65,536
	REJECTS(from_a({.kind = Kind::body, .end = true, .call = 5, .offset = 4, .total = 4}), "rule 7.7");
	// 8: a Credit's limit increases.
	REJECTS(from_a({.kind = Kind::credit, .call = 5, .limit = 65536}), "rule 8");
	from_a({.kind = Kind::credit, .call = 5, .limit = 65537});
	// 9: frames for a retired id are counted and dropped.
	from_a({.kind = Kind::cancel, .word = 4, .call = 5});
	from_a({.kind = Kind::credit, .call = 5, .limit = 1 << 20});
	from_a(id(body_frame, 5));
	CHECK(net.b->lane.late() == 2 && net.servers.back().ended_with(Outcome::cancel, Code::cancelled));
	// 10: a Drain's id has the receiver's parity or is 0, is allocated, and comes once.
	REJECTS(from_a({.kind = Kind::drain, .call = 1}), "rule 10: Drain id");
	REJECTS(from_a({.kind = Kind::drain, .call = 2}), "rule 10: Drain id");
	from_a({.kind = Kind::drain});
	REJECTS(from_a({.kind = Kind::drain}), "rule 10: second Drain");
}

void one_frame() {
	Net net;
	auto &c = net.call("ping", 4);
	net.flow();
	CHECK(c.ok() && c.got == pattern(5, 's') && net.servers.back().got == "ping" && net.servers.back().ok());
	CHECK(net.from_a[Kind::call] == 1 && net.from_a.size() == 1 && net.from_b[Kind::body] == 1 && net.from_b.size() == 1);
	CHECK(net.zero() && !net.a->lane.live() && !net.b->lane.live());
}

// Both directions streamed past their windows; the pools stay within one window per call.
void streams() {
	Net net;
	net.response = 1 << 20;
	const auto request = pattern(300 << 10, 'q');
	auto &c = net.call(request, 1000);
	net.flow();
	CHECK(c.ok() && c.got == pattern(1 << 20, 's') && net.servers.back().got == request);
	CHECK(net.from_a[Kind::credit] >= 12 && net.from_b[Kind::credit] >= 3);
	CHECK(net.a->calling.high_water() <= 128 << 10 && net.b->serving.high_water() <= 128 << 10 && net.zero());
}

void ends() {
	Net net;
	net.response = 1 << 20;
	auto &held = net.call("x", 1); // stalls its response, then cancels it
	held.hold = true;
	auto &unknown = net.call("y", 1, 64 << 10, 9);
	auto &failed = net.open();
	net.flow();
	CHECK(unknown.ended_with(Outcome::error, Code::unknown_method));
	CHECK(held.got.size() == 128 << 10 && !held.completions);
	net.a->lane.stop(held.h, Code::cancelled, "enough");
	net.b->lane.stop(net.servers.back().h, Code::handler_failed, "boom");
	net.flow();
	CHECK(held.ended_with(Outcome::cancel, Code::cancelled) && net.servers.front().ended_with(Outcome::cancel, Code::cancelled));
	CHECK(net.servers.front().status.text == "enough" && failed.status.text == "boom");
	CHECK(failed.ended_with(Outcome::error, Code::handler_failed) && net.zero());
}

void deadlines() {
	Net net(1 << 20, {.window = 1 << 16, .max_call_deadline = 2s});
	auto &served = net.open(); // the server's deadline: arrival plus timeout_ms, clamped to 2 s
	net.flow();
	CHECK(net.b->lane.next_deadline() == net.time + 2s);
	net.time += 2s;
	net.b->lane.expire();
	net.flow();
	CHECK(served.ended_with(Outcome::error, Code::deadline_exceeded) && net.servers.back().ended_with(Outcome::error, Code::deadline_exceeded));
	auto &called = net.open(); // the caller's: a Cancel
	const auto filler = net.a->calling.renew(0, net.a->calling.limit());
	auto &waiting = net.open(64 << 10); // waits for `calling` past its deadline: timed_out
	net.flow();
	net.time += 10s;
	net.a->lane.expire();
	net.flow();
	CHECK(called.ended_with(Outcome::cancel, Code::deadline_exceeded) && net.servers.back().ended_with(Outcome::cancel, Code::deadline_exceeded));
	CHECK(waiting.status.outcome == Outcome::timed_out && waiting.completions == 1);
	net.a->calling.release(filler);
	CHECK(net.zero());
}

// D5 refusals: the serving bound, then a pool short of one chunk; a slot frees when its call ends.
void refusals() {
	Net net(1 << 20, {.window = 1 << 16, .serving = 2});
	auto &first = net.open();
	net.open();
	auto &third = net.open();
	net.flow();
	CHECK(third.ended_with(Outcome::error, Code::overloaded) && net.b->lane.live() == 2);
	net.a->lane.stop(first.h, Code::cancelled);
	auto &fourth = net.open();
	net.flow();
	CHECK(!fourth.completions && net.b->lane.live() == 2);
	Net small(32 << 10);
	auto &refused = small.open(16 << 10);
	auto &whole = small.call("ping", 4, 16 << 10);
	small.flow();
	CHECK(refused.ended_with(Outcome::error, Code::overloaded) && whole.ok() && small.zero());
}

// Mutant `drain-ignored`: calls a Drain does not cover end not_processed, and no new call starts.
void drain(Net &net) {
	auto &covered = net.open();
	net.flow();
	auto &uncovered = net.open(); // its Call crosses the Drain
	net.b->lane.drain();
	net.ba();
	CHECK(uncovered.status.outcome == Outcome::not_processed && uncovered.completions == 1 && !covered.completions);
	auto &late = net.call("ping", 4);
	CHECK(late.status.outcome == Outcome::not_processed && late.completions == 1);
	net.ab(); // the crossing Call: discarded unanswered
	CHECK(net.b->lane.live() == 1 && !net.b->lane.drained());
	net.a->lane.send(covered.h, "", true);
	net.flow();
	CHECK(covered.ok() && net.b->lane.drained() && !net.a->lane.live() && net.zero());
}
void drain_ignored() {
	Net net;
	drain(net);
}

// Mutant `no-release-on-close`: a failed connection ends every call and returns every byte.
void no_release_on_close() {
	Net net;
	net.response = 1 << 20;
	auto &live = net.call("x", 1);
	live.hold = true;
	auto &open = net.open();
	net.flow();
	const auto filler = net.a->calling.renew(0, net.a->calling.limit());
	auto &waiting = net.open();
	CHECK(net.a->calling.used() == 1 << 20 && net.b->serving.used() == 128 << 10);
	net.a->lane.close();
	net.b->lane.close();
	CHECK(live.ended_with(Outcome::connection_aborted, Code{}) && open.status.outcome == Outcome::connection_aborted);
	CHECK(waiting.status.outcome == Outcome::not_processed);
	for (auto &s : net.servers)
		CHECK(s.status.outcome == Outcome::connection_aborted);
	net.a->calling.release(filler);
	net.run();
	CHECK(net.zero());
}

// Mutant `forget-is-violation`: a response chunk crossing the Cancel is counted, not fatal.
void forget_is_violation() {
	Net net;
	net.response = 200 << 10;
	auto &c = net.call("x", 1);
	net.ab();
	net.a->lane.stop(c.h, Code::cancelled);
	net.ba();
	CHECK(net.a->lane.late() == 2 && c.completions == 1);
	net.flow();
	CHECK(net.servers.back().ended_with(Outcome::cancel, Code::cancelled) && net.zero());
}

// Mutant `grant-without-reserve`: the first window is what the pool holds, not what was asked.
void grant_without_reserve() {
	Net net(64 << 10);
	net.response = 200 << 10;
	auto &c = net.call("x", 1, 4 << 20);
	std::vector<std::string> keep;
	auto frames = queued(net.a->lane, keep);
	CHECK(frames.size() == 1 && frames[0].window == 64 << 10 && net.a->calling.used() == 64 << 10);
	for (auto &f : keep)
		net.b->lane.receive(f);
	net.flow();
	CHECK(c.ok() && net.a->calling.high_water() <= 64 << 10 && net.zero());
}

// Mutant `no-regrant`: a response of eight windows finishes only on regrants.
void no_regrant() {
	Net net;
	net.response = 1 << 20;
	auto &c = net.call("x", 1);
	net.flow();
	CHECK(c.ok() && c.got.size() == 1 << 20 && net.from_a[Kind::credit] >= 14);
}

// Mutant `grant-on-receive`: credit follows consumption, never arrival.
void grant_on_receive() {
	Net net;
	net.response = 200 << 10;
	auto &c = net.call("x", 1);
	c.hold = true;
	net.flow();
	CHECK(c.got.size() == 128 << 10 && !net.from_a[Kind::credit] && net.a->calling.used() == 128 << 10);
	net.a->lane.consume(c.h, 64 << 10);
	std::vector<std::string> keep;
	auto frames = queued(net.a->lane, keep);
	CHECK(frames.size() == 1 && frames[0].kind == Kind::credit && frames[0].limit == 192 << 10);
}

// Mutant `split-release`: the consumed bytes move into the next grant in one step, so a call
// waiting for the pool cannot take them and strand the stream.
void split_release() {
	Net net(128 << 10);
	net.response = 256 << 10;
	auto &first = net.call("x", 1);
	auto &second = net.call("y", 1); // waits for `calling`
	CHECK(net.a->calling.used() == 128 << 10);
	net.flow();
	CHECK(first.ok() && second.ok() && first.got.size() == 256 << 10 && net.zero());
}

// Mutant `id-at-call`: a call that waits for `outgoing` takes its id after the one that did not.
void id_at_call() {
	Net net;
	auto &out = net.a->outgoing;
	Charge blocker(out, out.try_acquire(out.limit(), out.limit()));
	auto &waits = net.call(pattern(1000, 'w'), 1000);
	auto &goes = net.call("", 0);
	blocker = Charge();
	net.flow();
	CHECK(waits.ok() && goes.ok() && net.zero());
}

// The pools' arithmetic, with values.
void pools() {
	ByteBudget p(10);
	CHECK(p.try_acquire(4, 6) == 6 && !p.try_acquire(5, 8));
	std::size_t woke = 0;
	std::uint64_t ticket = 0;
	CHECK(!p.acquire(5, 8, [&](std::size_t n) { woke = n; }, ticket) && ticket);
	CHECK(!p.try_acquire(1, 1));     // FIFO: the waiter is first
	CHECK(p.renew(6, 9) == 6);       // a waiter is queued: the holder keeps its own, no more
	CHECK(p.renew(6, 2) == 2 && woke == 8 && p.used() == 10); // the excess serves the waiter
	p.release(8);
	p.release(2);
	CHECK(!p.used() && p.high_water() == 10);
	CHECK(p.renew(0, 7) == 7 && p.renew(7, 12) == 10); // grows while nobody waits, up to the limit
	CHECK(!p.acquire(3, 3, [&](std::size_t n) { woke = n; }, ticket) && p.cancel(ticket) && !p.cancel(ticket));
	{
		Charge c(p, 10), d;
		d = std::move(c);
	}
	CHECK(!p.used());
}

// Threads hammer one pool: renewals never lose their own bytes, FIFO wakes, all returns to 0.
void pools_threads() {
	ByteBudget pool(1 << 20);
	std::atomic<int> wakes{0};
	std::vector<std::thread> threads;
	for (int t = 0; t < 4; ++t)
		threads.emplace_back([&, t] {
			std::mt19937 random(t);
			std::size_t held = 0;
			for (int i = 0; i < 20000; ++i) {
				const std::size_t wanted = random() % (300 << 10);
				switch (random() % 4) {
				case 0: {
					const auto n = pool.renew(held, wanted);
					CHECK(n >= std::min(wanted, held));
					held = n;
					break;
				}
				case 1:
					held += pool.try_acquire(1, wanted + 1);
					break;
				case 2: { // a wait either withdraws or its wake (on the releasing thread) returns the grant
					std::uint64_t ticket;
					if (auto n = pool.acquire(64 << 10, wanted + (64 << 10), [&](std::size_t n) { ++wakes, pool.release(n); }, ticket))
						held += n;
					else
						pool.cancel(ticket);
					break;
				}
				default:
					pool.release(std::exchange(held, 0));
				}
			}
			pool.release(held);
		});
	for (auto &t : threads)
		t.join();
	CHECK(!pool.used() && pool.high_water() <= pool.limit());
	std::printf("pools-threads: %d wakes\n", wakes.load());
}

// Two lanes on two loops over pipes, sharing one host's pools, echoing streamed bodies.
struct End {
	asio::io_context io;
	asio::executor_work_guard<asio::io_context::executor_type> work{io.get_executor()};
	std::optional<AsyncPipe> pipe;
	std::optional<Lane> lane;
	std::optional<Outgoing> writing;
	std::string wire, frame;
	unsigned char length[4];
	void pump() {
		if (writing || !(writing = lane->next()))
			return;
		wire.clear();
		journal::put32(wire, std::uint32_t(writing->bytes.size()));
		wire += writing->bytes;
		asio::async_write(*pipe, asio::buffer(wire), [this](asio::error_code e, std::size_t) {
			writing.reset();
			if (!e)
				pump();
		});
	}
	void read() {
		asio::async_read(*pipe, asio::buffer(length), [this](asio::error_code e, std::size_t) {
			if (e)
				return;
			frame.resize(length[0] | length[1] << 8 | length[2] << 16 | std::uint32_t(length[3]) << 24);
			asio::async_read(*pipe, asio::buffer(frame), [this](asio::error_code e, std::size_t) {
				if (e)
					return;
				lane->receive(frame);
				read();
			});
		});
	}
	Lane::Post post() {
		return [this](std::function<void()> f) { asio::post(io, std::move(f)); };
	}
};
void pipes() {
	std::signal(SIGPIPE, SIG_IGN);
	ByteBudget serving(1 << 20), calling(256 << 10), outgoing(128 << 10);
	const Pools pools{serving, calling, outgoing};
	End a, b;
	int ab[2], ba[2];
	CHECK(!::pipe(ab) && !::pipe(ba));
	a.pipe.emplace(a.io, ba[0], ab[1]);
	b.pipe.emplace(b.io, ab[0], ba[1]);
	// b echoes each request as its response, consuming the request only as the echo goes out.
	struct Echo {
		Handle h;
		std::uint64_t total;
		std::string pending;
		bool started = false, ended = false, end_sent = false;
		int completions = 0;
		Status status;
	};
	std::list<Peer> callers;
	std::list<Echo> echoes;
	a.lane.emplace(true, pools, LaneOptions{.window = 64 << 10}, nullptr, a.post(), [&a] { a.pump(); });
	b.lane.emplace(
		false, pools, LaneOptions{.window = 64 << 10},
		[&](const Incoming &in) -> std::optional<Events> {
			auto &e = echoes.emplace_back(Echo{.h = in.handle, .total = in.total});
			auto flush = [&b, &e] {
				while (!e.end_sent) {
					const std::size_t k = std::min(b.lane->sendable(e.h), e.pending.size());
					if (!k && !(e.ended && e.pending.empty()))
						return;
					if (!std::exchange(e.started, true))
						b.lane->total(e.h, e.total);
					e.end_sent = e.ended && k == e.pending.size();
					b.lane->send(e.h, e.pending.substr(0, k), e.end_sent);
					b.lane->consume(e.h, k);
					e.pending.erase(0, k);
				}
			};
			return Events{.body = [&e, flush](std::string_view bytes, bool end) { e.pending.append(bytes), e.ended = end, flush(); },
						  .credit = flush,
						  .done = [&e](const Status &s) { ++e.completions, e.status = s; }};
		},
		b.post(), [&b] { b.pump(); });
	constexpr int calls = 12;
	std::promise<void> all;
	int finished = 0;
	asio::post(a.io, [&] {
		for (int i = 0; i < calls; ++i) {
			auto &p = callers.emplace_back();
			p.out = pattern(i * (97 << 10) + i, char('a' + i));
			p.queued = std::min<std::size_t>(p.out.size(), 1000);
			p.end_sent = p.queued == p.out.size();
			auto ev = events(*a.lane, p);
			ev.done = [&p, &finished, &all](const Status &s) {
				++p.completions, p.status = s;
				if (++finished == calls)
					all.set_value();
			};
			p.h = a.lane->call(7, {.deadline = Clock::now() + 60s, .window = 64 << 10, .total = p.out.size()}, p.out.substr(0, p.queued), std::move(ev));
		}
	});
	a.read();
	b.read();
	std::thread ta([&] { a.io.run(); }), tb([&] { b.io.run(); });
	CHECK(all.get_future().wait_for(120s) == std::future_status::ready);
	for (auto *e : {&a, &b})
		asio::post(e->io, [e] {
			e->lane->close();
			asio::error_code ignored;
			e->pipe->close(ignored);
			e->work.reset();
		});
	ta.join();
	tb.join();
	for (auto &p : callers)
		CHECK(p.ok() && p.got == p.out);
	for (auto &e : echoes)
		CHECK(e.completions == 1 && e.status.outcome == Outcome::ok);
	CHECK(echoes.size() == calls && !serving.used() && !calling.used() && !outgoing.used());
	CHECK(calling.high_water() <= 256 << 10 && outgoing.high_water() <= 128 << 10 && serving.high_water() <= 12 * (64 << 10));
	std::printf("pipes: %d calls; high water serving %zu, calling %zu, outgoing %zu\n", calls, serving.high_water(), calling.high_water(),
				outgoing.high_water());
}

// The fuzz target's seeds: frames for its decoder, and call-table scripts of [op][u16 length][bytes].
void seed(const char *dir) {
	int n = 0;
	auto write = [&](char selector, const std::string &bytes) {
		std::ofstream(std::string(dir) + "/seed-" + std::to_string(n++), std::ios::binary) << selector << bytes;
	};
	auto rec = [](char op, const std::string &bytes) { return op + std::string{char(bytes.size()), char(bytes.size() >> 8)} + bytes; };
	auto frame = [&](const Frame &f) { return rec(0, encode(f)); };
	for (auto &f : codec_frames)
		write(0, encode(f));
	const Frame streamed = with(call_frame, [](Frame &f) { f.end = false, f.total = 10; });
	write(1, frame(streamed) + frame(with(body_frame, [](Frame &f) { f.bytes = "abcdef", f.end = true; })) + rec(1, "") +
				 frame({.kind = Kind::credit, .call = 1, .limit = 70000}) + rec(3, "") + rec(2, ""));
	write(1, frame({.kind = Kind::credit, .call = 4, .limit = 200}) + frame({.kind = Kind::body, .end = true, .call = 2, .total = 5, .bytes = "hello"}) +
				 rec(1, "") + frame({.kind = Kind::error, .word = 256, .call = 4, .bytes = "missing"}) + frame({.kind = Kind::drain, .call = 4}));
	write(1, frame(call_frame) + frame({.kind = Kind::cancel, .word = 4, .call = 1}) + frame(body_frame) + rec(4, "") + rec(2, "") + rec(2, "") + rec(2, ""));
	write(3, frame(with(call_frame, [](Frame &f) { f.call = 2; })) + frame({.kind = Kind::credit, .call = 1, .limit = 100}) +
				 frame({.kind = Kind::body, .call = 1, .total = unknown_total, .bytes = "part"}) + rec(1, "") +
				 frame({.kind = Kind::body, .end = true, .call = 1, .offset = 4, .total = unknown_total}) + frame({.kind = Kind::drain, .call = 1}));
	std::printf("%d seeds\n", n);
}
} // namespace

int main(int argc, char **argv) {
	if (argc == 3 && !std::strcmp(argv[1], "seed"))
		return seed(argv[2]), 0;
	const std::pair<const char *, void (*)()> cases[] = {
		{"codec", codec},
		{"decoder-rules", decoder_rules},
		{"table-rules", table_rules},
		{"one-frame", one_frame},
		{"streams", streams},
		{"ends", ends},
		{"deadlines", deadlines},
		{"refusals", refusals},
		{"pools", pools},
		{"pools-threads", pools_threads},
		{"pipes", pipes},
		{"forget-is-violation", forget_is_violation},
		{"grant-without-reserve", grant_without_reserve},
		{"no-release-on-close", no_release_on_close},
		{"no-regrant", no_regrant},
		{"grant-on-receive", grant_on_receive},
		{"split-release", split_release},
		{"drain-ignored", drain_ignored},
		{"id-at-call", id_at_call},
	};
	int ran = 0;
	for (auto &[name, run] : cases)
		if (argc < 2 || !std::strcmp(argv[1], name))
			run(), ++ran;
	if (!ran)
		return std::fprintf(stderr, "unknown case\n"), 2;
	std::printf("rpc: %d cases passed\n", ran);
	return 0;
}
