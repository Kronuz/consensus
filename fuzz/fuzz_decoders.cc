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

// libFuzzer target for consensus's decoders (Track C, T8): control frames, memberships,
// founding and join records, storage batches, checkpoint bundles, and snapshot
// descriptors. data[0] picks the decoder; the rest is its input. Each must decode or
// throw, never crash or read out of bounds, and what decodes must re-encode to the same
// bytes where the encoding is canonical.
#include "consensus/checkpoint.h"
#include "consensus/membership.h"
#include "consensus/snapshot.h"
#include "consensus/storage.h"
#include "consensus/transport/codec.h"

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <string_view>

namespace {

using namespace kronuz::consensus;

FixedConfiguration founding() {
	FixedConfiguration c;
	c.cluster[0] = 'F'; c.configuration[0] = 'Z'; c.local = 1; c.voters = {1, 2, 3};
	return c;
}
FixedConfiguration joiner() {
	FixedConfiguration c = founding();
	c.local = 7; c.voters.clear();
	return c;
}
void require(bool value) {
	if (!value) { std::abort(); }
}

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
	if (size == 0) { return 0; }
	const std::string_view input(reinterpret_cast<const char*>(data) + 1, size - 1);
	try {
		// FUZZ_DECODER pins every input to one decoder, so each gets measured hours of its own.
		static const int pinned = std::getenv("FUZZ_DECODER") ? std::atoi(std::getenv("FUZZ_DECODER")) : -1;
		switch (pinned >= 0 ? pinned : data[0] % 7) {
		case 0: {
			auto message = transport::decode(input);
			require(transport::encode(message) == input);
			break;
		}
		case 1: {
			auto m = decode_membership(input);
			require(encode_membership(m) == input);
			break;
		}
		case 2: {
			Recovery recovery(founding()); recovery.replay(1, input);
			auto state = recovery.finish(1);
			require(encode_initialization(founding(), state.membership) == input);
			break;
		}
		case 3: {
			Recovery recovery(joiner()); recovery.replay(1, input);
			require(encode_join(joiner()) == input);
			break;
		}
		case 4: {
			Recovery recovery(founding()); recovery.replay(1, encode_initialization(founding()));
			recovery.replay(2, input);
			(void)recovery.finish(2);
			break;
		}
		case 5: {
			kronuz::journal::ArtifactDescriptor application{};
			(void)decode_checkpoint(input, founding(), 1, application);
			break;
		}
		case 6: {
			const auto c = founding();
			SnapshotPolicy policy{c.cluster, c.configuration, 7, 1 << 20};
			auto descriptor = decode_snapshot(input, policy);
			require(encode_snapshot(descriptor, policy) == input);
			break;
		}
		}
	} catch (const std::exception& error) {
		if (std::getenv("FUZZ_DEBUG")) { std::fprintf(stderr, "decoder %u: %s\n", data[0] % 7u, error.what()); }
	}
	return 0;
}
