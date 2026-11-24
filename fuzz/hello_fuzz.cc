// libFuzzer target for the node hello, DTHL0003 (consensus/transport/node_hello.h): any bytes
// decode to a hello or a Corruption, never a crash; a decoded hello is well formed and
// re-encodes to the same bytes.
#include "consensus/transport/node_hello.h"

#include <cstdint>
#include <cstdlib>

using namespace kronuz::consensus::transport;

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data, std::size_t size) {
	const std::string_view bytes(reinterpret_cast<const char *>(data), size);
	try {
		const auto hello = decode_node_hello(bytes);
		if (node_hello_fault(hello) || encode_node_hello(hello) != bytes || (hello.reason && !hello.refused))
			std::abort();
	} catch (const kronuz::journal::Corruption &) {
	}
	return 0;
}
