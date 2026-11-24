// libFuzzer target for the DTJN0002 exchange's frames (consensus/transport/admission_join.h): the
// first byte picks the offer, the request or the answer, and the rest decodes to that frame or a
// JoinRefused (malformed), never a crash. A decoded frame re-encodes to the same bytes, and its
// DER (CAs, CSR, certificate) parses or is refused as the member and the joiner do.
#include "consensus/transport/admission_join.h"

#include <cstdint>
#include <cstdlib>

using namespace kronuz::consensus::transport;

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data, std::size_t size) {
	if (!size)
		return 0;
	const std::string_view bytes(reinterpret_cast<const char *>(data) + 1, size - 1);
	try {
		switch (data[0] % 3) {
		case 0: {
			const auto offer = decode_join_offer(bytes);
			if (bytes.size() <= join_frame_limit && encode_join_offer(offer) != bytes)
				std::abort();
			for (const auto &ca : offer.cas)
				if (auto c = join_detail::certificate_of(ca))
					(void)key_pin(c.get());
			break;
		}
		case 1: {
			const auto request = decode_join_request(bytes);
			if (bytes.size() <= join_frame_limit && encode_join_request(request) != bytes)
				std::abort();
			if (auto key = join_detail::signed_key(request.csr))
				(void)key_pin(key.get());
			break;
		}
		default: {
			const auto answer = decode_join_answer(bytes);
			if (encode_join_answer(answer) != bytes)
				std::abort();
			(void)join_detail::certificate_of(answer.certificate);
		}
		}
	} catch (const JoinRefused &e) {
		if (e.fault != JoinFault::malformed)
			std::abort();
	}
	return 0;
}
