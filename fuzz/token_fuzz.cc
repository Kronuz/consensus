// libFuzzer target for the join token v2 (consensus/transport/admission_join.h): any text decodes
// to a token or a JoinRefused, never a crash. A decoded token re-encodes to the same text up to
// hex case; a refusal is version exactly when the text has version 1's shape or another
// version's magic.
#include "consensus/transport/admission_join.h"

#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <string>

using namespace kronuz::consensus::transport;

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data, std::size_t size) {
	const std::string_view text(reinterpret_cast<const char *>(data), size);
	const bool digits = size >= 8 && std::all_of(text.begin() + 4, text.begin() + 8, [](char c) { return c >= '0' && c <= '9'; });
	const bool versioned = text.starts_with("DTJT") && (size == 132 || (digits && !text.starts_with("DTJT0002")));
	try {
		const auto token = decode_join_token(text);
		std::string lower(text);
		for (auto &c : lower)
			c = char(std::tolower(static_cast<unsigned char>(c)));
		if (versioned || encode_join_token(token) != lower || decode_join_token(encode_join_token(token)) != token)
			std::abort();
	} catch (const JoinRefused &e) {
		if ((e.fault == JoinFault::version) != versioned || (e.fault != JoinFault::version && e.fault != JoinFault::malformed))
			std::abort();
	}
	return 0;
}
