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

#pragma once

#include "journal/artifact.h"
#include "storage.h"

namespace kronuz::consensus {
namespace checkpoint_detail {
constexpr std::uint64_t magic = 0x31504b4342544652ull;
constexpr std::uint64_t magic_v2 = 0x32504b4342544652ull;
// Version 3 records the base membership (Track C, D1); written only when there is one, so a
// fixed configuration's bundles keep their bytes. Layout: magic, flags (bit 0: an
// application format follows), the version 1 fields, and after the initialization record a
// membership length and its bytes.
constexpr std::uint64_t magic_v3 = 0x33504b4342544652ull;
inline std::size_t maximum_size(Limits limits) {
	constexpr std::size_t fixed = 400 + 8 + 24 * 64 + 16;  // with a version 3 membership
	if (limits.log_entries > (std::numeric_limits<std::size_t>::max() - fixed) / 21 ||
		limits.log_bytes > std::numeric_limits<std::size_t>::max() - fixed - limits.log_entries * 21) {
		throw std::length_error("checkpoint encoded bound overflow");
	}
	return fixed + limits.log_entries * 21 + limits.log_bytes;
}
} // namespace checkpoint_detail
} // namespace kronuz::consensus
