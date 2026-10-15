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

// D11's turn driver: single-voter groups on one core share a turn log; each turn carries
// the Persist batches the groups staged and releases their Persisted after one barrier.
// Power is lost after every I/O operation of a run (journal's sector model): every entry a
// group acknowledged (delivered as Committed) survives, and the groups carry on.
#include "consensus/core.h"
#include "consensus/turn_journal.h"
#include "journal/memory_io.h"
#include <iostream>
#include <map>
#include <random>

using namespace kronuz::consensus;
namespace jt = kronuz::journal::testing;

namespace {
int checks = 0, failures = 0;
void check(bool value, const std::string& what) {
	++checks;
	if (!value) { ++failures; if (failures < 20) { std::cerr << "FAIL: " << what << '\n'; } }
}
constexpr Timing compact{20, 40, 100, 200, 60000};
const kronuz::journal::Identity identity{'T', 'U', 'R', 'N', 'S'};
const kronuz::journal::TurnLogOptions options{16 * 1024, 1024, 1, 4, 4096};

FixedConfiguration config(std::uint64_t group) {
	FixedConfiguration c; c.local = 1; c.voters = {1}; c.cluster[0] = 'C'; c.configuration[0] = static_cast<char>('A' + group); return c;
}

struct Node {
	std::map<std::uint64_t, std::unique_ptr<Core>> cores;
	std::map<std::uint64_t, std::map<Index, std::string>> acknowledged;  // committed commands, by index
	std::uint64_t now = 0;
	void act(TurnJournal& journal, std::uint64_t group, const Actions& actions) {
		for (const auto& action : actions) {
			if (auto p = std::get_if<Persist>(&action)) { journal.stage(group, *p); }
			else if (auto c = std::get_if<Committed>(&action)) {
				for (const auto& e : c->entries) { if (e.kind == EntryKind::Command) { acknowledged[group][e.index] = e.payload; } }
				cores.at(group)->step(Applied{c->entries.back().index});
			}
		}
	}
	// One round: every core ticks, leaders propose, then one turn persists what was staged.
	void round(TurnJournal& journal, std::mt19937_64& random) {
		now += 10;
		for (auto& [group, core] : cores) {
			act(journal, group, core->step(Tick{now, 100 + 7 * group}));
			if (core->role() == Role::Leader && !core->busy() && random() % 2) {
				act(journal, group, core->step(Propose{random(), "c" + std::to_string(group) + "-" + std::to_string(random() % 100000)}));
			}
		}
		while (!journal.idle()) {
			for (auto [group, token] : journal.commit()) { act(journal, group, cores.at(group)->step(Persisted{token})); }
		}
	}
};

// Opens the node's groups: recovers each from the journal, or creates it.
void open(Node& node, TurnJournal& journal, std::uint64_t groups) {
	node.cores.clear();
	std::vector<std::uint64_t> created;
	for (std::uint64_t g = 1; g <= groups; ++g) {
		if (!journal.recover(g, config(g), Limits{})) { journal.create(g, config(g)); created.push_back(g); }
	}
	if (!journal.idle()) { journal.commit(); }
	for (std::uint64_t g = 1; g <= groups; ++g) {
		auto state = journal.recover(g, config(g), Limits{});
		if (!state) { RecoveredState fresh; fresh.configuration = config(g); state = fresh; }
		node.cores[g] = std::make_unique<Core>(config(g), std::move(*state), Limits{}, compact);
		node.cores[g]->step(Tick{node.now, 100 + 7 * g});
		node.act(journal, g, node.cores[g]->step(Start{}));
	}
	while (!journal.idle()) { for (auto [group, token] : journal.commit()) { node.act(journal, group, node.cores.at(group)->step(Persisted{token})); } }
}

std::uint64_t run(std::uint64_t seed, std::uint64_t cut, unsigned rounds, std::uint64_t groups) {
	jt::Disk disk; jt::MemoryIO io(disk); Node node; std::mt19937_64 random(seed);
	disk.random.seed(seed * 31 + cut);
	try {
		disk.cut = cut;
		TurnJournal journal(io, identity, options);
		open(node, journal, groups);
		for (unsigned r = 0; r < rounds; ++r) { node.round(journal, random); }
	} catch (const jt::PowerLoss&) {}
	const auto operations = disk.operations;
	disk.power_loss();
	const auto at = "seed " + std::to_string(seed) + " cut " + std::to_string(cut);
	try {
		TurnJournal journal(io, identity, options);
		check(!journal.damaged(), "power loss alone damages nothing: " + at);
		for (const auto& [group, entries] : node.acknowledged) {
			auto state = journal.recover(group, config(group), Limits{});
			bool kept = state.has_value();
			for (const auto& [index, payload] : entries) {
				if (!kept) { break; }
				if (index <= state->base_index) { continue; }
				const auto offset = index - state->base_index - 1;
				kept = offset < state->entries.size() && state->entries[offset].payload == payload;
			}
			check(kept, "group " + std::to_string(group) + " keeps every acknowledged entry: " + at);
		}
		Node again; again.now = node.now;
		open(again, journal, groups);
		for (unsigned r = 0; r < 40; ++r) { again.round(journal, random); }
		std::size_t committed = 0; for (const auto& [_, entries] : again.acknowledged) { committed += entries.size(); }
		check(committed > 0, "the groups carry on after recovery: " + at);
	} catch (const std::exception& error) { check(false, at + ": " + error.what()); }
	return operations;
}
}  // namespace

int main(int argc, char** argv) {
	const std::uint64_t seeds = argc > 1 ? std::stoull(argv[1]) : 4;
	std::uint64_t points = 0;
	for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
		const auto total = run(seed, std::numeric_limits<std::uint64_t>::max(), 60, 6);
		for (std::uint64_t cut = 1; cut <= total; ++cut, ++points) { run(seed, cut, 60, 6); }
	}
	std::cout << points << " crash points, " << checks << " turn journal checks, " << failures << " failures\n";
	return failures ? 1 : 0;
}
