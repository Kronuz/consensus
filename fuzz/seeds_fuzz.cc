// libFuzzer target for the multicast seed datagram (consensus/transport/seeds_codec.h): any bytes
// decode to an announcement or a fault, never a crash; a decoded one is valid and re-encodes to
// the same bytes, and a fault is the one its size and magic call for.
#include "consensus/transport/seeds_codec.h"

#include <cstdint>
#include <cstdlib>
#include <string_view>

using namespace kronuz::consensus::transport;

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data, std::size_t size) {
	const std::string_view bytes(reinterpret_cast<const char *>(data), size);
	Announcement a;
	const auto fault = decode_announcement(bytes, a);
	if (size > seed_datagram_limit && fault != SeedFault::oversized)
		std::abort();
	if (fault == SeedFault::version && bytes.substr(0, 8) == "DTSD0001")
		std::abort();
	if (fault == SeedFault::none && (announcement_fault(a) || encode_announcement(a) != bytes))
		std::abort();
	return 0;
}
