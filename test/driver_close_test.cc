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

// Driver::close(id) on a row that is open, fenced, half-opened (opened, never created) or
// unknown: it completes, the Driver erases the row and returns its bytes, the Worker hands its
// group back to the host (closed into its table or released), and the id recovers again in the
// same live Host. `driver_close_test [case]`; no case runs them all.
#include "consensus/driver.h"
#include "faultlab/faultfs.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <map>
#include <new>
#include <random>
#include <span>
#include <string>
#include <vector>

using namespace kronuz::consensus;
namespace j = kronuz::journal;
namespace fl = kronuz::faultlab;

// Live heap bytes, counted at the allocation and the sized free, each block carrying its size.
// A block allocated in an Uncounted scope (a test disk's own) carries a mark and never counts.
// Single-threaded: the loop is an inline scheduler. TSan's runtime defines operator new, so
// there the count is off and the bytes case only reports.
#if defined(__SANITIZE_THREAD__)
#define COUNT_BYTES 0
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define COUNT_BYTES 0
#endif
#endif
#ifndef COUNT_BYTES
#define COUNT_BYTES 1
#endif
#if COUNT_BYTES
namespace {
std::size_t live_bytes = 0;
bool uncounted = false;
constexpr std::size_t header = alignof(std::max_align_t), mark = ~(~std::size_t(0) >> 1);
}
void* operator new(std::size_t n) {
	void* raw = std::malloc(n + header);
	if (!raw) { throw std::bad_alloc(); }
	*static_cast<std::size_t*>(raw) = uncounted ? n | mark : n;
	if (!uncounted) { live_bytes += n; }
	return static_cast<char*>(raw) + header;
}
void operator delete(void* p) noexcept {
	if (!p) { return; }
	void* raw = static_cast<char*>(p) - header;
	const auto size = *static_cast<std::size_t*>(raw);
	if (!(size & mark)) { live_bytes -= size; }
	std::free(raw);
}
void operator delete(void* p, std::size_t) noexcept { operator delete(p); }
#else
namespace { std::size_t live_bytes = 0; bool uncounted = false; }
#endif
namespace {
struct Uncounted {
	bool was = uncounted;
	Uncounted() { uncounted = true; }
	~Uncounted() { uncounted = was; }
};
}

namespace {

int checks = 0, failures = 0;
void check(bool value, const std::string& what) {
	++checks;
	if (!value) { ++failures; if (failures < 40) { std::cerr << "FAIL: " << what << '\n'; } }
}

// The host's directory on `disk`, made on first use.
std::shared_ptr<j::IO> host_io(fl::filesystem& disk) {
	const auto names = disk.list();
	if (std::find(names.begin(), names.end(), "host") == names.end()) { disk.make_directory("host"); }
	return std::make_shared<fl::io>(disk, "host");
}
// A crash keeps any sector-boundary prefix of an unsynced append, as a real device may.
const fl::crash_policy sector_cut{.size = fl::size_pick::sector};

// The power cut as the process meets it: a call into the host's directory that fails because the
// power is off throws PowerCut, which is no std::exception, so no step() catches it and it may end
// the Driver's loop by a throw. File calls fail with faultfs's EIO.
struct PowerCut {};
class CutIO final : public j::IO {
public:
	explicit CutIO(fl::filesystem& d) : disk(d), backend(host_io(d)) {}
	j::FilesystemClass directory_filesystem() const override { return backend->directory_filesystem(); }
	j::DirectoryTrust directory_trust() const override { return backend->directory_trust(); }
	std::unique_ptr<j::OwnerLock> acquire_owner(bool create) override { return cut([&] { return backend->acquire_owner(create); }); }
	std::unique_ptr<j::OwnerLock> acquire_directory_owner() override { return cut([&] { return backend->acquire_directory_owner(); }); }
	std::unique_ptr<j::File> open_existing(std::string_view n) override { return cut([&] { return backend->open_existing(n); }); }
	std::unique_ptr<j::File> open_existing_read_only(std::string_view n) override { return cut([&] { return backend->open_existing_read_only(n); }); }
	std::unique_ptr<j::File> open_reclaim_candidate_read_only(std::string_view n) override { return cut([&] { return backend->open_reclaim_candidate_read_only(n); }); }
	void open_existing_read_only_into(std::string_view n, j::File& d) override { cut([&] { return backend->open_existing_read_only_into(n, d); }); }
	bool open_reclaim_candidate_read_only_into(std::string_view n, j::File& d) override { return cut([&] { return backend->open_reclaim_candidate_read_only_into(n, d); }); }
	std::unique_ptr<j::File> create_exclusive(std::string_view n) override { return cut([&] { return backend->create_exclusive(n); }); }
	void replace(std::string_view s, std::string_view d) override { cut([&] { return backend->replace(s, d); }); }
	void link(std::string_view s, std::string_view d) override { cut([&] { return backend->link(s, d); }); }
	void remove(std::string_view n) override { cut([&] { return backend->remove(n); }); }
	std::unique_ptr<j::DirectoryCursor> scan_directory() override { return cut([&] { return backend->scan_directory(); }); }
	std::optional<j::EntryFootprint> entry_footprint(std::string_view n) override { return cut([&] { return backend->entry_footprint(n); }); }
	std::optional<j::EntryInspection> inspect_entry(std::string_view n) override { return cut([&] { return backend->inspect_entry(n); }); }
	void order_files_and_directory(std::span<j::File* const> files) override { cut([&] { return backend->order_files_and_directory(files); }); }
	void sync_directory() override { cut([&] { return backend->sync_directory(); }); }
protected:
	std::unique_ptr<j::OwnerLock> lock_owner(const j::OwnerLockOptions& o) override { return cut([&] { return backend->acquire_owner(o); }); }
private:
	template <class F> auto cut(F call) const -> decltype(call()) {
		try { return call(); } catch (const std::system_error&) { if (disk.powered_off()) { throw PowerCut{}; } throw; }
	}
	fl::filesystem& disk;
	std::shared_ptr<j::IO> backend;
};

constexpr Timing compact{20, 40, 100, 200, 60000};
const j::TurnLogOptions turn_options{16 * 1024, 1024, 1, 4, 4096};
const j::AdmissionLimits admission{{256u << 20, 8192}, {256 * 1024, 3}, {64u << 20, 4}, 32, 3};
const j::Identity host_identity{'H', 'O', 'S', 'T'};

Limits limits() { Limits l; l.command_bytes = 1024; l.rpc_bytes = 4096; l.log_bytes = 64 * 1024; l.log_entries = 64; l.control_entries = 4; l.rpc_entries = 8; return l; }
GroupOptions options(std::vector<NodeId> voters, NodeId local = 1) {
	GroupOptions o;
	o.configuration.local = local; o.configuration.voters = std::move(voters);
	o.configuration.cluster[0] = 'C'; o.configuration.configuration[0] = 'G';
	o.admission = admission; o.limits = limits(); o.timing = compact; o.scheduling = WorkerLimits{16, 128};
	return o;
}

struct Sink {
	std::size_t fenced = 0;
	std::map<GroupId, Index> committed;  // each group's last committed index
	std::map<GroupId, std::vector<std::string>> commands;  // each group's committed commands, in order
	std::function<void(GroupId)> on_fenced;  // a Sink may call the Driver's plain calls
	std::function<void(GroupId)> on_committed;
	void operator()(GroupId, Send) {}
	void operator()(GroupId g, Committed c) {
		if (on_committed) { on_committed(g); }
		if (c.entries.empty()) { return; }
		committed[g] = c.entries.back().index;
		for (auto& e : c.entries) { if (e.kind == EntryKind::Command) { commands[g].push_back(std::move(e.payload)); } }
	}
	void operator()(GroupId, RoleChanged) {}
	void operator()(GroupId, ActivateInstall) {}
	void operator()(GroupId, CheckpointPublished) {}
	void operator()(GroupId, RepairCompleted) {}
	void operator()(GroupId g, Fenced) { ++fenced; if (on_fenced) { on_fenced(g); } }
	void operator()(GroupId, SnapshotNeeded) {}
	void operator()(GroupId, SnapshotTransmit) {}
	void operator()(GroupId, SnapshotReleased) {}
};
static_assert(sink<Sink>);

// A flat in-memory directory whose own allocations never count: a host's files are on disk,
// not in its heap. No power-loss model, and every operation is O(log names).
class HeapIO final : public j::IO {
	struct Data { std::string bytes; };
	class File final : public j::File {
	public:
		explicit File(std::shared_ptr<Data> data) : data_(std::move(data)) {}
		std::uint64_t size() override { return data_->bytes.size(); }
		std::size_t read_at(std::uint64_t offset, std::span<char> bytes) override {
			if (offset >= data_->bytes.size()) { return 0; }
			const auto n = std::min<std::size_t>(bytes.size(), data_->bytes.size() - offset);
			std::copy_n(data_->bytes.data() + offset, n, bytes.data()); return n;
		}
		std::size_t write_at(std::uint64_t offset, std::string_view bytes) override {
			Uncounted scope;
			if (data_->bytes.size() < offset + bytes.size()) { data_->bytes.resize(offset + bytes.size(), '\0'); }
			data_->bytes.replace(offset, bytes.size(), bytes); return bytes.size();
		}
		void truncate(std::uint64_t size) override { Uncounted scope; data_->bytes.resize(size, '\0'); }
		void sync() override {}
	private:
		std::shared_ptr<Data> data_;
	};
	class Cursor final : public j::DirectoryCursor {
	public:
		explicit Cursor(std::vector<std::string> names) : names_(std::move(names)) {}
		std::optional<std::string> next() override { Uncounted scope; if (at_ == names_.size()) { return std::nullopt; } return names_[at_++]; }
	private:
		std::vector<std::string> names_; std::size_t at_ = 0;
	};
	std::map<std::string, std::shared_ptr<Data>, std::less<>> names_;
	std::unique_ptr<j::File> open(std::string_view name, bool required) {
		Uncounted scope;
		auto found = names_.find(name);
		if (found == names_.end()) { if (required) { throw std::runtime_error("no such file"); } return nullptr; }
		return std::make_unique<File>(found->second);
	}
public:
	~HeapIO() override { Uncounted scope; names_.clear(); }
	std::unique_ptr<j::OwnerLock> acquire_owner(bool) override { Uncounted scope; return std::make_unique<j::OwnerLock>(); }
	std::unique_ptr<j::File> open_existing(std::string_view name) override { return open(name, true); }
	std::unique_ptr<j::File> open_existing_read_only(std::string_view name) override { return open(name, true); }
	std::unique_ptr<j::File> open_reclaim_candidate_read_only(std::string_view name) override { return open(name, false); }
	void open_existing_read_only_into(std::string_view, j::File&) override { throw std::logic_error("reusable file open unsupported"); }
	bool open_reclaim_candidate_read_only_into(std::string_view, j::File&) override { throw std::logic_error("reusable candidate open unsupported"); }
	std::unique_ptr<j::File> create_exclusive(std::string_view name) override {
		Uncounted scope;
		auto [at, fresh] = names_.try_emplace(std::string(name), std::make_shared<Data>());
		if (!fresh) { throw std::runtime_error("file exists"); }
		return std::make_unique<File>(at->second);
	}
	void replace(std::string_view source, std::string_view destination) override {
		Uncounted scope;
		auto found = names_.find(source);
		if (found == names_.end()) { throw std::runtime_error("no such file"); }
		auto data = found->second; names_.erase(found); names_[std::string(destination)] = std::move(data);
	}
	void remove(std::string_view name) override { Uncounted scope; if (auto found = names_.find(name); found != names_.end()) { names_.erase(found); } }
	std::unique_ptr<j::DirectoryCursor> scan_directory() override {
		Uncounted scope;
		std::vector<std::string> names;
		for (const auto& [name, data] : names_) { names.push_back(name); }
		return std::make_unique<Cursor>(std::move(names));
	}
	std::optional<j::EntryFootprint> entry_footprint(std::string_view name) override {
		auto found = names_.find(name);
		if (found == names_.end()) { return std::nullopt; }
		j::EntryFootprint footprint; footprint.kind = j::EntryKind::Regular; footprint.logical_bytes = found->second->bytes.size();
		return footprint;
	}
	void sync_directory() override {}
protected:
	std::unique_ptr<j::OwnerLock> lock_owner(const j::OwnerLockOptions&) override { Uncounted scope; return std::make_unique<j::OwnerLock>(); }
};

// One host and one loop on an inline_scheduler; the Worker's IO and the turns go through
// InlineCompletion, or through a HeldCompletion the case completes by hand.
struct Loop {
	fl::filesystem& disk;
	task::inline_scheduler scheduler;
	j::InlineCompletion inline_io;
	j::HeldCompletion held;
	std::unique_ptr<j::Host> host;
	Sink sink;
	std::unique_ptr<Driver<Sink>> driver;
	task::cancel_source stop, driver_stop;  // the turn driver's run, the Driver's
	bool turns_done = false, driver_done = false;
	explicit Loop(fl::filesystem& d, bool hold = false, std::shared_ptr<j::IO> storage = {}) : disk(d) {
		if (!storage) { storage = host_io(disk); }
		host = std::make_unique<j::Host>(std::move(storage), host_identity, j::HostOptions{1, turn_options});
		auto io = hold ? j::completion_ref::of(held) : j::completion_ref::of(inline_io);
		driver = std::make_unique<Driver<Sink>>(Seams{task::scheduler_ref::of(scheduler), io, *host, 0, 7}, sink);
		auto ref = task::scheduler_ref::of(scheduler);
		auto turns = hold ? j::completion_ref::of(held) : j::completion_ref::of(inline_io);
		task::spawn(ref, host->driver(0).run(turns, stop.token()), [this](std::exception_ptr) { turns_done = true; });
		task::spawn(ref, driver->run(driver_stop.token()), [this](std::exception_ptr) { driver_done = true; });
		run();
	}
	~Loop() { finish(); }
	void run() {
		std::size_t budget = 10'000'000;
		scheduler.run_until([&] { return --budget == 0; });
		if (!budget) { check(false, "the loop spins"); std::exit(1); }
	}
	// Runs what is queued within `steps` turns of the scheduler: false if it is still busy.
	bool run_bounded(std::size_t steps) {
		std::size_t budget = steps;
		scheduler.run_until([&] { return --budget == 0; });
		return budget != 0;
	}
	void finish() {
		if (!driver) { return; }
		shutdown();
		release();
		check(turns_done && driver_done, "the turn driver's and the Driver's runs end at shutdown");
		driver.reset(); host.reset();
	}
	// Stops both runs, the turn driver's first, as one shared token would.
	void shutdown() { stop.request(task::cancel_reason::shutdown); driver_stop.request(task::cancel_reason::shutdown); }
	void release() {
		run();
		for (int n = 0; held.held(); ++n) {
			if (n == 100000) { check(false, "held IO never runs out"); std::exit(1); }
			held.complete(held.tickets().front()); run();
		}
	}
	template <class T> struct Started {
		bool done = false;
		std::exception_ptr error;
		std::optional<T> value;
	};
	template <class T> std::shared_ptr<Started<T>> start(task::task<T> work) {
		auto state = std::make_shared<Started<T>>();
		task::spawn(task::scheduler_ref::of(scheduler), std::move(work), [state](std::exception_ptr e, std::optional<T> v) { state->done = true; state->error = e; state->value = std::move(v); });
		run();
		return state;
	}
	std::shared_ptr<Started<bool>> start(task::task<void> work) {
		auto state = std::make_shared<Started<bool>>();
		auto wrapped = [](task::task<void> inner) -> task::task<bool> { co_await std::move(inner); co_return true; };
		task::spawn(task::scheduler_ref::of(scheduler), wrapped(std::move(work)), [state](std::exception_ptr e, std::optional<bool> v) { state->done = true; state->error = e; state->value = v; });
		run();
		return state;
	}
	bool settle(task::task<void> work, std::exception_ptr& error) {
		auto state = start(std::move(work));
		release();
		error = state->error;
		return state->done;
	}
	// A lone voter, created and leading.
	void lone(GroupId g) {
		driver->open(g, options({1}));
		std::exception_ptr error;
		check(settle(driver->create(g, j::Identity{'L', static_cast<char>(g)}), error) && !error, "create");
	}
	// close(g) to completion: whether it completed, and how.
	bool close(GroupId g, std::exception_ptr& error) { return settle(driver->close(g), error); }
	// Applies everything committed to g.
	void apply(GroupId g) {
		for (Index done = 0; sink.committed[g] > done;) { done = sink.committed[g]; driver->applied(g, Applied{done}); release(); }
	}
	// A proposal placed and committed.
	std::optional<Placement> propose(GroupId g, const std::string& command) {
		auto call = start(driver->propose(g, command));
		release();
		check(call->done && !call->error && call->value, "placed " + command + ": " + what(call->error));
		return call->value;
	}
	// Recovers g in this live Host: the application image its checkpoint restores (empty
	// without one); its commands after the image reach the Sink as they commit.
	std::string recover(GroupId g, std::exception_ptr& error, GroupOptions o) {
		std::string image;
		driver->open(g, std::move(o));
		sink.commands.erase(g); sink.committed.erase(g);
		auto restore = [&image](j::ArtifactReader& reader) {
			image.assign(static_cast<std::size_t>(reader.descriptor().length), '\0');
			for (std::size_t at = 0; at < image.size();) { at += reader.read_at(at, std::span<char>(image.data() + at, image.size() - at)); }
		};
		if (!settle(driver->recover(g, restore), error)) { check(false, "recover completes"); }
		release();
		return image;
	}
	static std::string what(std::exception_ptr error);
};

std::string Loop::what(std::exception_ptr error) {
	if (!error) { return "no error"; }
	try { std::rethrow_exception(error); } catch (const std::exception& e) { return e.what(); } catch (...) {}
	return "unknown";
}
std::string what(std::exception_ptr error) { return Loop::what(error); }
template <class E> bool is(std::exception_ptr error) {
	if (!error) { return false; }
	try { std::rethrow_exception(error); } catch (const E&) { return true; } catch (...) {}
	return false;
}

// A1: a group fenced by its application, in a live process, closes; the id opens again.
void fenced_row() {
	constexpr GroupId g = 11;
	fl::filesystem disk; Loop loop(disk);
	loop.lone(g);
	auto read = loop.start(loop.driver->read(g));
	loop.driver->application_failed(g, 1, "the application failed");
	loop.run();
	check(read->done && is<fenced>(read->error), "a waiter on the fenced group completes with fenced");
	check(loop.sink.fenced == 1, "the Sink heard Fenced");
	check(loop.driver->groups() == 1, "the fenced row is held");
	std::exception_ptr error;
	check(loop.close(g, error) && !error, "close completes on a fenced row: " + what(error));
	check(loop.driver->groups() == 0 && !loop.driver->state(g), "the fenced row is erased");
	auto refused = loop.start(loop.driver->read(g));
	check(refused->done && is<closed>(refused->error), "a call on the erased id completes with closed");
	loop.driver->open(g, options({1}));
	check(loop.driver->groups() == 1, "the id opens again");
	check(loop.close(g, error) && !error && loop.driver->groups() == 0, "and closes again");
}

// A1: a group fenced by its application with a proposal's IO in flight: close still ends.
void fenced_in_flight() {
	constexpr GroupId g = 12;
	fl::filesystem disk; Loop loop(disk, true);
	loop.lone(g);
	auto proposal = loop.start(loop.driver->propose(g, "staged"));
	check(!proposal->done && loop.held.held() > 0, "the proposal's record waits in a held turn");
	loop.driver->application_failed(g, 1, "the application failed");
	loop.run();
	auto closing = loop.start(loop.driver->close(g));
	loop.release();
	check(closing->done && !closing->error, "close completes with a record staged on a fenced row: " + what(closing->error));
	check(proposal->done && is<fenced>(proposal->error), "the staged proposal's caller completes with fenced: " + what(proposal->error));
	check(loop.driver->groups() == 0, "the row is erased");
}

// A2: open succeeded and the caller then failed: no create, no records.
void half_opened_row() {
	constexpr GroupId g = 21;
	fl::filesystem disk; Loop loop(disk);
	loop.driver->open(g, options({1}));
	check(loop.driver->groups() == 1, "the row is held");
	std::exception_ptr error;
	check(loop.close(g, error) && !error, "close completes on a row never created: " + what(error));
	check(loop.driver->groups() == 0 && !loop.driver->state(g), "the row is erased");
	loop.driver->open(g, options({1}));
	check(loop.settle(loop.driver->create(g, j::Identity{'H', 'O'}), error) && !error, "the id opens and creates again: " + what(error));
	check(loop.driver->state(g) && loop.driver->state(g)->role == Role::Leader, "and leads");
}

// A2: a create that failed (the group cancelled before it ran) leaves a row that closes.
void failed_create() {
	constexpr GroupId g = 22;
	fl::filesystem disk; Loop loop(disk);
	loop.driver->open(g, options({1}));
	task::cancel_source cancel; cancel.request();
	std::exception_ptr error;
	check(loop.settle(loop.driver->create(g, j::Identity{'F', 'C'}, cancel.token()), error) && is<task::cancelled>(error), "a cancelled create completes with task::cancelled");
	check(loop.close(g, error) && !error && loop.driver->groups() == 0, "close frees the row: " + what(error));
}

// An id the Driver never held: close completes at once.
void unknown_row() {
	fl::filesystem disk; Loop loop(disk);
	std::exception_ptr error;
	check(loop.close(31, error) && !error && loop.driver->groups() == 0, "close of an unknown id completes: " + what(error));
	loop.lone(32);
	check(loop.close(32, error) && !error, "close of an open row completes: " + what(error));
	check(loop.close(32, error) && !error, "and again, once erased");
	loop.driver->open(32, options({1}));
	check(loop.driver->groups() == 1, "the id opens again");
	bool threw = false;
	try { loop.driver->open(32, options({1})); } catch (const std::logic_error&) { threw = true; }
	check(threw, "an open row refuses a second open");
}

// A4: a close with a call waiting and IO in flight completes it and then the row, once the IO lands.
void close_in_flight() {
	constexpr GroupId g = 41;
	fl::filesystem disk; Loop loop(disk, true);
	loop.lone(g);
	auto staged = loop.start(loop.driver->propose(g, "staged"));
	auto read = loop.start(loop.driver->read(g));
	check(loop.held.held() > 0 && !staged->done && !read->done, "a record staged, a read waiting");
	auto closing = loop.start(loop.driver->close(g));
	check(read->done && is<closed>(read->error), "the read completes with closed");
	check(!closing->done && loop.driver->groups() == 1, "close waits for the staged record");
	loop.release();
	check(staged->done && !staged->error, "the staged proposal completes with its placement");
	check(closing->done && !closing->error, "close completes once it is durable");
	check(loop.driver->groups() == 0 && loop.driver->in_flight() == 0, "the row is erased with no IO in flight");
}

// A group the Sink queues again in its own closing step is erased off the ready list: the Sink's
// Fenced callback calls applied(), which marks the group ready while it closes in that step.
void erase_queued() {
	constexpr GroupId g = 51, h = 52;
	fl::filesystem disk; Loop loop(disk);
	loop.lone(g); loop.lone(h);
	std::size_t calls = 0;
	loop.sink.on_fenced = [&](GroupId id) { ++calls; loop.driver->applied(id, Applied{1}); };
	// close is queued ahead of the fence, so one step both fences the group and closes it.
	bool closing_done = false; std::exception_ptr closing_error;
	task::spawn(task::scheduler_ref::of(loop.scheduler), loop.driver->close(g), [&](std::exception_ptr e) { closing_done = true; closing_error = e; });
	loop.driver->application_failed(g, 1, "the application failed");
	loop.run();
	check(calls == 1, "the Sink queued the group again in its closing step");
	check(closing_done && !closing_error, "close completes: " + what(closing_error));
	check(loop.driver->groups() == 1 && !loop.driver->state(g) && loop.driver->state(h), "only the closed row is erased");
	std::exception_ptr error;
	check(loop.close(h, error) && !error && loop.driver->groups() == 0, "and the ready list still serves the other row");
	loop.driver->open(g, options({1}));
	check(loop.driver->groups() == 1, "the id opens again");
}

// A checkpoint's capture of the application: `image`, through `through` in `term`.
struct Capture {
	std::string image; std::size_t at = 0; Index through; Term term;
	CaptureMetadata metadata() const { return {0, through, term, 0}; }
	std::size_t read(std::span<char> out) {
		auto n = std::min(out.size(), image.size() - at);
		std::copy_n(image.data() + at, n, out.data()); at += n; return n;
	}
};

// A6: after shutdown a group awaiting its turn barrier is not watched again, so the loop does
// not wake itself while another group's IO keeps it running: it idles until that IO lands,
// then ends. The turn driver and the Driver stop at once, as in Xapiand's shutdown; the turn
// driver still writes what is staged.
void shutdown_idles() {
	constexpr GroupId g = 61, h = 62;
	fl::filesystem disk; Loop loop(disk, true);
	loop.lone(h);
	auto placed = loop.start(loop.driver->propose(h, "k"));
	loop.release();
	check(placed->done && placed->value, "placed");
	if (!placed->value) { return; }
	for (Index done = 0; loop.sink.committed[h] > done;) { done = loop.sink.committed[h]; loop.driver->applied(h, Applied{done}); loop.release(); }
	loop.lone(g);
	auto checkpoint = loop.start(loop.driver->checkpoint(h, 1 << 20, Capture{std::string(1000, 'y'), 0, placed->value->index, placed->value->term}));
	const auto io = loop.held.held();
	auto proposal = loop.start(loop.driver->propose(g, "staged"));
	check(!checkpoint->done && io > 0, "another group's IO is held");
	check(!proposal->done && loop.held.held() > io && loop.driver->in_flight() > 1, "a record waits for its turn barrier");
	auto& turns = loop.host->driver(0);
	const auto durable = turns.sequence(g);
	check(turns.staged(g), "the proposal's record is staged");
	loop.shutdown();
	if (!loop.run_bounded(20'000)) { check(false, "the loop idles after shutdown with the barrier pending"); std::cout << checks << " driver close checks, " << failures << " failures\n"; std::exit(1); }
	check(loop.driver->in_flight() > 0 && !loop.driver_done, "the loop waits for the IO");
	loop.release();
	check(loop.driver_done && loop.driver->in_flight() == 0, "the loop ends once the IO lands");
	check(loop.turns_done && !turns.staged(g) && turns.sequence(g) > durable, "the record staged before shutdown is durable: sequence " + std::to_string(turns.sequence(g)) + " after " + std::to_string(durable));
	check(proposal->done && is<task::cancelled>(proposal->error), "the proposal completes with task::cancelled: " + what(proposal->error));
	check(checkpoint->done && is<task::cancelled>(checkpoint->error), "the checkpoint completes with task::cancelled: " + what(checkpoint->error));
}

// A6: a step after the turn driver returned stages a record (a higher-term VoteRequest to a
// follower) while another group's IO holds the loop. run() does not wait for that record's
// barrier, which no turn driver will write: it ends once the IO lands, and the record stays staged.
void shutdown_ends() {
	constexpr GroupId g = 71, h = 72;
	fl::filesystem disk; Loop loop(disk, true);
	loop.lone(h);
	auto placed = loop.start(loop.driver->propose(h, "k"));
	loop.release();
	check(placed->done && placed->value, "placed");
	if (!placed->value) { return; }
	for (Index done = 0; loop.sink.committed[h] > done;) { done = loop.sink.committed[h]; loop.driver->applied(h, Applied{done}); loop.release(); }
	loop.driver->open(g, options({1, 2}));  // a follower: its peer never answers
	std::exception_ptr error;
	check(loop.settle(loop.driver->create(g, j::Identity{'T', 'V'}), error) && !error, "the two-voter group is created: " + what(error));
	auto checkpoint = loop.start(loop.driver->checkpoint(h, 1 << 20, Capture{std::string(1000, 'y'), 0, placed->value->index, placed->value->term}));
	check(!checkpoint->done && loop.held.held() > 0, "another group's IO is held");
	loop.shutdown();
	check(loop.run_bounded(20'000) && loop.turns_done && !loop.driver_done, "the turn driver returns; the Driver waits for the IO");
	auto& turns = loop.host->driver(0);
	const auto durable = turns.sequence(g);
	loop.driver->receive(g, Receive{2, VoteRequest{5, 100, 5}});
	check(loop.run_bounded(20'000), "the loop idles after the step");
	check(loop.driver->state(g) && loop.driver->state(g)->term == 5 && turns.staged(g), "the step staged the new term after the turn driver returned");
	loop.release();
	for (int n = 0; n < 20 && !loop.driver_done; ++n) { loop.scheduler.advance(50'000'000); loop.run_bounded(200'000); }
	if (!loop.driver_done) { check(false, "the Driver's run ends once the IO lands: in_flight " + std::to_string(loop.driver->in_flight())); std::cout << checks << " driver close checks, " << failures << " failures\n"; std::exit(1); }
	check(loop.driver->in_flight() == 0, "no IO in flight");
	check(turns.staged(g) && turns.sequence(g) == durable, "the record stays staged, reported not durable");
}

// The Host's per-loop range table keeps an expired entry per view a closed row held until it
// doubles since its last sweep (journal host.h, Ranges): spare ids from `first`, each opened
// and closed once, grow it to that size, and the first open whose sweep returns the rows'
// entries ends it. What is left after is what the rows keep.
void sweep_ranges(Loop& loop, GroupId first, std::size_t rows) {
	std::exception_ptr error;
	for (GroupId spare = first; spare < first + 4 * rows + 64; ++spare) {  // past twice the most rows it held
		const auto before = live_bytes;
		loop.driver->open(spare, options({1}));
		if (!loop.close(spare, error) || error) { check(false, "the sweeping row closes: " + what(error)); return; }
		if (live_bytes + rows * 64 < before) { return; }
	}
	check(!COUNT_BYTES, "the range table sweeps");
}

// A3: 10,000 rows opened and closed leave the Driver's heap where it was.
void bytes() {
	constexpr GroupId rows = 10'000;
	fl::filesystem disk; Loop loop(disk);
	std::exception_ptr error;
	// One warm round, so one-time allocations do not count.
	loop.driver->open(1, options({1}));
	check(loop.close(1, error) && !error, "warm close");
	const auto before = live_bytes;
	for (GroupId g = 1; g <= rows; ++g) { loop.driver->open(g, options({1})); }
	const auto opened = live_bytes;
	check(loop.driver->groups() == rows, "10,000 rows held");
	for (GroupId g = 1; g <= rows; ++g) {
		auto closing = loop.start(loop.driver->close(g));
		if (!closing->done || closing->error) { check(false, "close " + std::to_string(g) + " completes: " + what(closing->error)); break; }
	}
	check(loop.driver->groups() == 0, "no row held after the closes");
	const auto unpurged = live_bytes;
	sweep_ranges(loop, rows + 1, rows);
	const auto after = live_bytes;
	std::printf("per row: %zu bytes held open, %zu left after close before the Host's range sweep, %zu after\n", (opened - before) / std::size_t(rows),
		unpurged > before ? (unpurged - before) / std::size_t(rows) : std::size_t(0), after > before ? (after - before) / std::size_t(rows) : std::size_t(0));
	// What stays is the row table's bucket array, which an unordered_map never shrinks: a pointer a row.
	check(!COUNT_BYTES || after <= before + rows * sizeof(void*) * 2, "closed rows return their bytes: " + std::to_string(after) + " against " + std::to_string(before));
}

// Heap the simulated disk itself holds: not the process's (a real host's files are on disk).
std::size_t disk_bytes(const fl::filesystem& disk) { return disk.used(); }

// Report only: created rows (the rows Xapiand counts) opened, closed and left, with the share the
// journal keeps per structure. `proposals` more records are written per row before the close.
void created_pass(GroupId rows, int proposals) {
	fl::filesystem disk; Loop loop(disk);
	std::exception_ptr error;
	loop.lone(1); check(loop.close(1, error) && !error, "warm close");
	const auto base = live_bytes, base_disk = disk_bytes(disk);
	for (GroupId g = 2; g < 2 + rows; ++g) {
		loop.lone(g);
		for (int n = 0; n < proposals; ++n) { std::exception_ptr e; std::optional<Placement> placed; auto call = loop.start(loop.driver->propose(g, "p")); loop.release(); (void)placed; (void)e; check(call->done && !call->error, "proposal placed"); }
	}
	const auto opened = live_bytes, opened_disk = disk_bytes(disk);
	std::uint64_t records = 0;
	for (GroupId g = 2; g < 2 + rows; ++g) { records += loop.host->driver_of(g).sequence(g); }
	for (GroupId g = 2; g < 2 + rows; ++g) { check(loop.close(g, error) && !error, "close"); }
	loop.driver->open(1, options({1})); check(loop.close(1, error) && !error, "the purging row closes");
	const auto closed = live_bytes, closed_disk = disk_bytes(disk);
	for (GroupId g = 2; g < 2 + rows; ++g) { loop.host->driver_of(g).forget(g); }
	const auto forgotten = live_bytes, forgotten_disk = disk_bytes(disk);
	// The process's heap: the counted bytes less what the simulated disk holds.
	auto process = [](std::size_t total, std::size_t disk_part) { return total > disk_part ? total - disk_part : std::size_t(0); };
	const auto from = process(base, base_disk);
	auto per = [&](std::size_t now) { return now > from ? (now - from) / std::size_t(rows) : std::size_t(0); };
	std::printf("created rows=%u proposals=%d records/row=%.1f: process heap per row: open %zu, closed %zu, after the log forgets them %zu; simulated disk per row: %zu\n",
		unsigned(rows), proposals, double(records) / double(rows), per(process(opened, opened_disk)), per(process(closed, closed_disk)),
		per(process(forgotten, forgotten_disk)), (closed_disk - base_disk) / std::size_t(rows));
}
void created_bytes() { created_pass(2000, 0); created_pass(2000, 10); }

// A row's application image: its applied commands, each ended by a newline.
std::string join(const std::vector<std::string>& commands) {
	std::string image;
	for (const auto& c : commands) { image += c; image += '\n'; }
	return image;
}
std::vector<std::string> split(const std::string& image) {
	std::vector<std::string> commands;
	for (std::size_t at = 0, end; (end = image.find('\n', at)) != std::string::npos; at = end + 1) { commands.push_back(image.substr(at, end - at)); }
	return commands;
}
bool prefix(const std::vector<std::string>& shorter, const std::vector<std::string>& longer) {
	return shorter.size() <= longer.size() && std::equal(shorter.begin(), shorter.end(), longer.begin());
}
// A checkpoint of g's applied commands at its last committed entry, the leader's term.
task::task<CheckpointPublished> checkpoint_of(Loop& loop, GroupId g, const std::vector<std::string>& applied) {
	const auto state = loop.driver->state(g);
	return loop.driver->checkpoint(g, 1 << 20, Capture{join(applied), 0, loop.sink.committed[g], state ? state->term : 0});
}
// The commands a recovered row holds: its image's, then those committed after it.
std::vector<std::string> recovered(Loop& loop, GroupId g, const std::string& image) {
	auto state = split(image);
	for (const auto& c : loop.sink.commands[g]) { state.push_back(c); }
	return state;
}

// A1: a row closes and recovers through the Driver in the same live Host: clean with its
// checkpoint last (closed into the host's table), clean with a record staged at close and
// fenced, with records committed and with one staged (each released). Each recovery holds
// every acknowledged record and nothing unattempted.
void reopen_rows() {
	constexpr GroupId g = 81;
	fl::filesystem disk; Loop loop(disk, true);
	auto& turns = loop.host->driver(0);
	std::vector<std::string> acknowledged, attempted;
	std::exception_ptr error;
	auto place = [&](const std::string& c) { attempted.push_back(c); if (loop.propose(g, c)) { acknowledged.push_back(c); } };
	auto reopen = [&](const std::string& how, bool table) {
		check(loop.driver->groups() == 0, how + ": the row is erased");
		check((loop.host->status(g) == j::GroupStatus::closed) == table, how + (table ? ": closed into the host's table" : ": released, not closed into the table"));
		check(table || turns.parked(g), how + ": the released group is parked");
		const auto image = loop.recover(g, error, options({1}));
		check(!error, how + ": recovers in the live Host: " + what(error));
		loop.apply(g);
		const auto state = recovered(loop, g, image);
		check(prefix(acknowledged, state) && prefix(state, attempted), how + ": recovers every acknowledged record and nothing unattempted: " + join(state));
		check(loop.driver->state(g) && loop.driver->state(g)->role == Role::Leader, how + ": and leads");
		attempted = acknowledged = state;
	};
	loop.lone(g);
	// Clean, committed: the checkpoint covers every record.
	place("a"); place("b"); loop.apply(g);
	auto published = loop.start(checkpoint_of(loop, g, acknowledged));
	loop.release();
	check(published->done && !published->error, "the checkpoint publishes: " + what(published->error));
	check(loop.close(g, error) && !error, "clean close: " + what(error));
	reopen("clean, committed", true);
	// Clean, staged: a record staged at close, after the checkpoint.
	place("c"); loop.apply(g);
	attempted.push_back("d");
	auto staged = loop.start(loop.driver->propose(g, "d"));
	check(!staged->done && turns.staged(g), "d is staged");
	auto closing = loop.start(loop.driver->close(g));
	loop.release();
	check(staged->done && !staged->error, "the staged record is placed: " + what(staged->error));
	if (staged->done && !staged->error) { acknowledged.push_back("d"); }
	check(closing->done && !closing->error, "close with a staged record: " + what(closing->error));
	reopen("clean, staged", false);
	// Fenced, committed.
	place("e"); loop.apply(g);
	loop.driver->application_failed(g, loop.sink.committed[g], "the application failed"); loop.release();
	check(loop.close(g, error) && !error, "fenced close: " + what(error));
	reopen("fenced, committed", false);
	// Fenced, staged: the staged record's caller hears fenced; its record may be durable.
	place("f"); loop.apply(g);
	attempted.push_back("g");
	staged = loop.start(loop.driver->propose(g, "g"));
	check(!staged->done && turns.staged(g), "g is staged");
	loop.driver->application_failed(g, loop.sink.committed[g], "the application failed"); loop.run();
	closing = loop.start(loop.driver->close(g));
	loop.release();
	check(closing->done && !closing->error, "fenced close with a staged record: " + what(closing->error));
	reopen("fenced, staged", false);
	// And the recovered row still checkpoints and closes into the table.
	place("h"); loop.apply(g);
	published = loop.start(checkpoint_of(loop, g, acknowledged));
	loop.release();
	check(published->done && !published->error && loop.close(g, error) && !error, "a recovered row checkpoints and closes: " + what(error));
	reopen("clean again", true);
}

// A2: 10,000 created rows, each with a record and a checkpoint covering it, opened together and
// closed: the process heap per row (HeapIO's files left out) returns to its pre-open value,
// the Driver's and the journal's alike. Then 10,000 rows closed with a record after their
// checkpoint, which the host releases: each is parked, and what that keeps is reported.
void closed_bytes() {
	const char* scale = std::getenv("CLOSE_BYTES_ROWS");  // another count, for timing
	const GroupId rows = scale ? std::stoull(scale) : 10'000;
	fl::filesystem unused; Loop loop(unused, false, std::make_shared<HeapIO>());
	std::exception_ptr error;
	auto process = [] { return live_bytes; };
	auto open = [&](GroupId g, bool covered) {
		loop.lone(g);
		loop.propose(g, "p"); loop.apply(g);
		auto published = loop.start(checkpoint_of(loop, g, {"p"}));
		loop.run();
		if (!published->done || published->error) { check(false, "checkpoint " + std::to_string(g) + ": " + what(published->error)); }
		if (!covered) { loop.propose(g, "q"); }
	};
	// The test's Sink keeps what it heard per row: dropped with the row, it is not the library's.
	auto close = [&](GroupId g) {
		if (!loop.close(g, error) || error) { check(false, "close " + std::to_string(g) + ": " + what(error)); }
		loop.sink.committed.erase(g); loop.sink.commands.erase(g);
	};
	open(1, true); close(1);  // one warm round
	auto per = [&](std::size_t now, std::size_t from) { return double(now) / double(rows) - double(from) / double(rows); };
	// A pass opens `rows` covered rows from `first` together, then closes them: the heap open,
	// after close, and after the Host's range table sweeps.
	auto pass = [&](GroupId first, GroupId spare) {
		const auto from = process();
		for (GroupId g = first; g < first + rows; ++g) { open(g, true); }
		const auto opened = process();
		for (GroupId g = first; g < first + rows; ++g) { close(g); }
		const auto unswept = process();
		sweep_ranges(loop, spare, rows);
		const auto closed = process();
		std::size_t table = 0;
		for (GroupId g = first; g < first + rows; ++g) { table += loop.host->status(g) == j::GroupStatus::closed; }
		check(table == rows && loop.driver->groups() == 0, "every covered row closes into the host's table: " + std::to_string(table));
		std::printf("closed_bytes: %u covered rows from %u, process heap per row: %.1f B open, %.1f B after close before the Host's range sweep, %.1f B after\n",
			unsigned(rows), unsigned(first), per(opened, from), per(unswept, from), per(closed, from));
		return closed;
	};
	const auto before = process();
	const auto first = pass(2, 10 * rows);
	// What the first pass keeps is capacity: the Driver's row table's bucket array, which an
	// unordered_map sizes to its most rows and never shrinks. A second pass adds nothing.
	check(!COUNT_BYTES || first <= before + rows * sizeof(void*) * 3, "closed rows return the heap: " + std::to_string(first) + " B against " + std::to_string(before) + " B before");
	const auto second = pass(2 + rows, 11 * rows);
	check(!COUNT_BYTES || second <= first + rows, "a second pass of closed rows keeps nothing more: " + std::to_string(second) + " B against " + std::to_string(first) + " B");
	const auto released_before = process();
	for (GroupId g = 2 + 2 * rows; g < 2 + 3 * rows; ++g) { open(g, false); }
	for (GroupId g = 2 + 2 * rows; g < 2 + 3 * rows; ++g) { close(g); }
	sweep_ranges(loop, 20 * rows, rows);
	const auto released = process();
	std::size_t parked = 0;
	for (GroupId g = 2 + 2 * rows; g < 2 + 3 * rows; ++g) { parked += loop.host->driver(0).parked(g); }
	check(parked == rows, "every row with a record after its checkpoint is released and parked: " + std::to_string(parked));
	std::printf("closed_bytes: %u released rows, process heap per row after close: %.1f B\n", unsigned(rows), per(released, released_before));
}

// A3: power cuts at every IO of a close-then-recover cycle, per seed. Rounds place a record and
// then fence and close the row, close it with records after its checkpoint, checkpoint and
// close it into the table, or go on; each close recovers the row in the live Host. After the
// cut, a new Host recovers every acknowledged record and nothing unattempted: none lost, none
// that a table close dropped replayed again.
std::uint64_t crash_tables = 0, crash_releases = 0;  // the closes a sweep's runs completed, by kind
bool close_crash_run(std::uint64_t seed, std::uint64_t cut) {
	constexpr GroupId g = 91;
	fl::filesystem disk;
	std::vector<std::string> acknowledged, attempted;
	std::mt19937_64 random(seed);
	bool reached = false;
	std::function<bool()> dead = [] { return false; };  // the cut-th operation has run
	const auto at = "seed " + std::to_string(seed) + " cut " + std::to_string(cut);
	std::vector<std::function<bool()>> calls;  // whether each call the run started completed
	auto track = [&](auto call) { calls.push_back([call] { return call->done; }); return call; };
	{
		Loop loop(disk, false, std::make_shared<CutIO>(disk));
		loop.lone(g);
		std::exception_ptr error;
		const auto base = disk.mutations();
		dead = [&disk, base, cut] { return disk.mutations() - base >= cut; };
		disk.power_off_after(cut - 1);
		auto ok = [&](std::shared_ptr<Loop::Started<bool>> call) { loop.release(); return call->done && !call->error; };
		bool alive = true;
		for (unsigned round = 0; round < 6 && alive; ++round) {
			const auto c = "r" + std::to_string(round);
			attempted.push_back(c);
			auto placed = track(loop.start(loop.driver->propose(g, c))); loop.release();
			if (!placed->done || placed->error) { alive = false; break; }
			acknowledged = attempted;
			const auto action = random() % 4;
			if (action == 3) { continue; }
			if (action == 0) { loop.driver->application_failed(g, loop.sink.committed[g], "fenced by the sweep"); loop.release(); }
			if (action == 2) {
				loop.apply(g);
				auto published = track(loop.start(checkpoint_of(loop, g, acknowledged))); loop.release();
				if (!published->done || published->error) { alive = false; break; }
			}
			if (!ok(track(loop.start(loop.driver->close(g))))) { alive = false; break; }
			++(loop.host->status(g) == j::GroupStatus::closed ? crash_tables : crash_releases);
			const auto image = loop.recover(g, error, options({1}));
			loop.apply(g);
			// Without power nothing commits after the recover, so the state shows only before the cut.
			if (error || dead()) { alive = false; break; }
			const auto state = recovered(loop, g, image);
			check(state == attempted, "a recover in the live Host holds every attempted record: " + at + " " + join(state));
		}
		reached = alive && !dead();
		if (!reached && !dead()) { check(false, "the run failed before its power cut: " + at); }
	}
	// The cut may end the Driver's loop by a throw: its callers still complete, none suspended.
	check(std::all_of(calls.begin(), calls.end(), [](const auto& done) { return done(); }), "every call completes once the loop ends: " + at);
	disk.crash(seed * 7919 + cut, sector_cut);
	Loop again(disk);
	if (again.host->status(g) != j::GroupStatus::none) {
		std::exception_ptr error;
		const auto image = again.recover(g, error, options({1}));
		check(!error, "recovery after the cut: " + at + ": " + what(error));
		again.apply(g);
		const auto state = recovered(again, g, image);
		check(prefix(acknowledged, state) && prefix(state, attempted), "recovery keeps every acknowledged record and nothing unattempted: " + at + " recovered " + join(state) + "acknowledged " + join(acknowledged));
	} else { check(acknowledged.empty(), "a row with acknowledged records is in the host's table: " + at); }
	return reached;
}
void close_crash() {
	const char* count = std::getenv("CLOSE_CRASH_SEEDS");
	const std::uint64_t seeds = count ? std::stoull(count) : 8;
	std::uint64_t runs = 0;
	for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
		for (std::uint64_t cut = 1; !close_crash_run(seed, cut); ++cut) { ++runs; if (failures) { return; } }
	}
	std::printf("close_crash: %u seeds, %u power cuts; %u closes into the table, %u releases\n", unsigned(seeds), unsigned(runs), unsigned(crash_tables), unsigned(crash_releases));
	check(crash_tables && crash_releases, "the sweep closes rows both ways");
}

// F2: a close whose table commit fails after the shard file was replaced (its directory sync
// throws) fences the Store, which releases the group. The table may hold the group closed, so
// recover and create refuse the id in this live Host with close_in_doubt (the host's mark).
// After a later
// directory sync and a power cut, the restarted host resolves it from the disk: closed, and
// it recovers every acknowledged record. Without the refusal, a record acknowledged after the
// failed close is lost.
void failed_close() {
	constexpr GroupId g = 71;
	fl::filesystem disk;
	std::vector<std::string> acknowledged, attempted;
	{
		Loop loop(disk);
		std::exception_ptr error;
		loop.lone(g);
		attempted.push_back("a");
		if (loop.propose(g, "a")) { acknowledged.push_back("a"); }
		loop.apply(g);
		auto published = loop.start(checkpoint_of(loop, g, acknowledged));
		loop.release();
		check(published->done && !published->error, "the checkpoint publishes: " + what(published->error));
		disk.add_fault({fl::op::sync_directory});  // the next directory sync fails: the table commit's
		check(loop.close(g, error) && !error, "close completes when its table commit fails: " + what(error));
		check(loop.driver->groups() == 0, "the row is erased");
		check(loop.host->driver(0).parked(g), "the fenced Store released the group: parked");
		loop.recover(g, error, options({1}));
		check(is<close_in_doubt>(error), "a live recover of the id refuses with close_in_doubt: " + what(error));
		if (!error) {  // the reopen the refusal stops: a record acknowledged past the closed base
			loop.apply(g);
			attempted.push_back("x");
			if (loop.propose(g, "x")) { acknowledged.push_back("x"); }
			loop.apply(g);
		} else {
			check(loop.close(g, error) && !error && loop.driver->groups() == 0, "the refused row closes: " + what(error));
			loop.driver->open(g, options({1}));
			check(loop.settle(loop.driver->create(g, j::Identity{'F', 'C'}), error) && is<close_in_doubt>(error), "a live create of the id refuses with close_in_doubt: " + what(error));
			check(loop.close(g, error) && !error && loop.driver->groups() == 0, "the refused row closes: " + what(error));
		}
		fl::io(disk, "host").sync_directory();  // any later directory sync: another group's create, a segment
		loop.release();
	}
	disk.crash(1, sector_cut);
	Loop again(disk);
	check(again.host->status(g) == j::GroupStatus::closed, "the restarted host holds the group closed, as its file does");
	std::exception_ptr error;
	const auto image = again.recover(g, error, options({1}));
	check(!error, "the restarted host recovers the id: " + what(error));
	again.apply(g);
	const auto state = recovered(again, g, image);
	check(prefix(acknowledged, state) && prefix(state, attempted), "it recovers every acknowledged record and nothing unattempted: acknowledged " + join(acknowledged) + ", recovered " + join(state));
}

// A7 (D128): the failed close's doubt is the Host's, not the Driver's. A second Driver over the
// same live Host is refused the id with close_in_doubt, and nothing acknowledged is lost at the
// restart.
void second_driver() {
	constexpr GroupId g = 71;
	fl::filesystem disk;
	std::vector<std::string> acknowledged, attempted;
	{
		Loop loop(disk);
		std::exception_ptr error;
		loop.lone(g);
		attempted.push_back("a");
		if (loop.propose(g, "a")) { acknowledged.push_back("a"); }
		loop.apply(g);
		auto published = loop.start(checkpoint_of(loop, g, acknowledged));
		loop.release();
		disk.add_fault({fl::op::sync_directory});  // the next directory sync fails: the table commit's
		check(loop.close(g, error) && !error, "the close's table commit fails: " + what(error));
		loop.driver_stop.request(task::cancel_reason::shutdown);
		loop.release();
		check(loop.driver_done && loop.driver->groups() == 0, "the first Driver's run ends, no row left");
		loop.driver.reset();
		loop.driver_stop = task::cancel_source{}; loop.driver_done = false;
		loop.driver = std::make_unique<Driver<Sink>>(Seams{task::scheduler_ref::of(loop.scheduler), j::completion_ref::of(loop.inline_io), *loop.host, 0, 7}, loop.sink);
		task::spawn(task::scheduler_ref::of(loop.scheduler), loop.driver->run(loop.driver_stop.token()), [&loop](std::exception_ptr) { loop.driver_done = true; });
		loop.run();
		loop.recover(g, error, options({1}));
		check(is<close_in_doubt>(error), "a second Driver over the same live Host refuses the id with close_in_doubt: " + what(error));
		if (!error) {
			loop.apply(g);
			attempted.push_back("x");
			if (loop.propose(g, "x")) { acknowledged.push_back("x"); }
			loop.apply(g);
		}
		check(loop.close(g, error), "the row closes: " + what(error));
		fl::io(disk, "host").sync_directory();
		loop.release();
	}
	disk.crash(1, sector_cut);
	Loop again(disk);
	std::exception_ptr error;
	const auto image = again.recover(g, error, options({1}));
	check(!error, "the restarted host recovers the id: " + what(error));
	again.apply(g);
	const auto state = recovered(again, g, image);
	check(prefix(acknowledged, state) && prefix(state, attempted), "every acknowledged record survives: acknowledged " + join(acknowledged) + ", recovered " + join(state));
}

// A throw that ends the Driver's loop under the blocking entry (a Sink's, which no step
// catches) reaches the entry's caller, not as the work's task::cancelled.
void entry_throw() {
	constexpr GroupId g = 111;
	fl::filesystem disk; task::blocking_scheduler scheduler; j::InlineCompletion io; Sink sink;
	j::Host host(host_io(disk), host_identity, j::HostOptions{1, turn_options});
	Driver<Sink> driver(Seams{task::scheduler_ref::of(scheduler), j::completion_ref::of(io), host, 0, 7}, sink);
	kronuz::consensus::run(driver, scheduler, [](Driver<Sink>& d, GroupId id) -> task::task<void> { d.open(id, options({1})); co_await d.create(id, j::Identity{'E', 'T'}); }(driver, g));
	driver.applied(g, Applied{sink.committed[g]});  // the next Committed is delivered once this one is applied
	sink.on_committed = [](GroupId) { throw 42; };
	int thrown = 0;
	try { kronuz::consensus::run(driver, scheduler, [](Driver<Sink>& d, GroupId id) -> task::task<void> { co_await d.committed(id, co_await d.propose(id, "x")); co_await d.read(id); }(driver, g)); }
	catch (int value) { thrown = value; } catch (...) {}
	check(thrown == 42, "the loop's throw reaches the blocking entry's caller");
}

// A5: a checkpoint whose peak exceeds the replacement pool fails at once with
// checkpoint_exceeds_pool, and the calls behind it go on; one at exactly the peak publishes.
void pool_bound() {
	constexpr std::uint64_t cap = 4096;
	const std::uint64_t peak = 2 * j::detail::artifact_header_size + checkpoint_detail::maximum_size(limits()) + cap;
	auto run = [&](j::StorageResources pool, bool fits) {
		constexpr GroupId g = 101;
		fl::filesystem disk; Loop loop(disk);
		auto o = options({1}); o.admission.replacement_pool = pool;
		loop.driver->open(g, std::move(o));
		std::exception_ptr error;
		check(loop.settle(loop.driver->create(g, j::Identity{'P', 'B'}), error) && !error, "create: " + what(error));
		loop.propose(g, "a"); loop.apply(g);
		const auto state = loop.driver->state(g);
		auto published = loop.start(loop.driver->checkpoint(g, cap, Capture{"a\n", 0, loop.sink.committed[g], state ? state->term : 0}));
		auto later = loop.start(loop.driver->propose(g, "b"));
		loop.release();
		const auto label = "pool " + std::to_string(pool.logical_bytes) + " B, " + std::to_string(pool.entries) + " entries, peak " + std::to_string(peak) + " B: ";
		if (fits) { check(published->done && !published->error, label + "the checkpoint publishes: " + what(published->error)); }
		else { check(published->done && is<checkpoint_exceeds_pool>(published->error), label + "the checkpoint fails with checkpoint_exceeds_pool: " + (published->done ? what(published->error) : std::string("still waiting"))); }
		check(later->done && !later->error, label + "a later proposal is placed: " + (later->done ? what(later->error) : std::string("still waiting")));
	};
	run({peak, 2}, true);
	run({peak - 1, 2}, false);
	run({peak, 1}, false);
}

const std::map<std::string, void (*)()> cases{
	{"created_bytes", &created_bytes},
	{"erase_queued", &erase_queued},
	{"shutdown_idles", &shutdown_idles},
	{"shutdown_ends", &shutdown_ends},
	{"fenced_row", &fenced_row},
	{"fenced_in_flight", &fenced_in_flight},
	{"half_opened_row", &half_opened_row},
	{"failed_create", &failed_create},
	{"unknown_row", &unknown_row},
	{"close_in_flight", &close_in_flight},
	{"bytes", &bytes},
	{"reopen_rows", &reopen_rows},
	{"closed_bytes", &closed_bytes},
	{"close_crash", &close_crash},
	{"pool_bound", &pool_bound},
	{"failed_close", &failed_close},
	{"second_driver", &second_driver},
	{"entry_throw", &entry_throw},
};

}  // namespace

int main(int argc, char** argv) {
	if (argc > 1) {
		auto found = cases.find(argv[1]);
		if (found == cases.end()) { std::cerr << "unknown case " << argv[1] << '\n'; return 2; }
		std::cerr << "case " << argv[1] << '\n';
		found->second();
	} else {
		// A report and the two long cases run by name, each its own ctest entry.
		for (const auto& [name, run] : cases) {
			if (name == "created_bytes" || name == "closed_bytes" || name == "close_crash") { continue; }
			std::cerr << "case " << name << '\n'; run();
		}
	}
	std::cout << checks << " driver close checks, " << failures << " failures\n";
	return failures ? 1 : 0;
}
