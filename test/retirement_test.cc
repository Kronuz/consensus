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

#include "consensus/core.h"
#include <iostream>
using namespace kronuz::consensus;
namespace {
unsigned checks = 0;
void require(bool value, const char* message) {
    ++checks;
    if (!value) { throw std::runtime_error(message); }
}
void retirement_progress() {
	FixedConfiguration fixed; fixed.local = 1; fixed.voters = {1};
	RecoveredState state; state.configuration = fixed; state.hard = {1, {}, 3};
	for (Index index = 1; index <= 3; ++index) {
		state.entries.push_back({index, 1, EntryKind::Command, std::string(1024, 'x')});
	}
	state.entries.back().kind = EntryKind::Configuration;
	auto membership = fixed_membership(state.configuration);
	membership.limits = Limits{};
	state.entries.back().payload = encode_membership(membership);
	require(state.entries.back().payload.size() > 64, "configuration fixture exceeds the lowered command limit");
	Limits limits; limits.command_bytes = 64;
	Core core(state.configuration, state, limits);
	core.begin_retirement();
	for (auto budget : {detail::RetirementBudget{0, 64}, detail::RetirementBudget{32, 0}}) {
		require(!core.retirement_step(budget) && core.retired_log_entries() == 3,
			"zero unit or byte capacity cannot retire historical payloads");
	}
	detail::RetirementBudget used{32, 64};
	require(used.consume(), "zero-byte work consumes a unit");
	require(!core.retirement_step(used) && core.retired_log_entries() == 3,
		"oversized historical work waits after a zero-byte predecessor");
	detail::RetirementBudget shared{32, 64};
	std::vector<Entry> output{{1, 1, EntryKind::Command, std::string(1024, 'x')}};
	require(detail::retire_entries(output, shared) && shared.bytes == 0, "one first oversized output makes progress");
	require(!core.retirement_step(shared) && core.retired_log_entries() == 3,
		"oversized allowance is shared across output and retained-log retirement");
	for (std::size_t remaining = 3; remaining; --remaining) {
		detail::RetirementBudget budget{32, 64};
		core.retirement_step(budget);
		require(core.retired_log_entries() == remaining - 1 && budget.bytes == 0,
			"a fresh turn retires exactly one oversized historical payload without wrapping");
		core.retirement_step(budget);
		require(core.retired_log_entries() == remaining - 1, "a helper cannot renew the same turn's allowance");
	}
	require(core.retirement_drained(), "historical retirement drains in finite turns");
}
}
int main() {
    try { retirement_progress(); }
    catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
    std::cout << checks << " retirement checks passed\n";
}
