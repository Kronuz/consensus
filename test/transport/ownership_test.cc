#include "consensus/transport/snapshot_write.h"
#include <cassert>
#include <iostream>

using namespace kronuz::consensus::transport;
using namespace kronuz::consensus;
struct Channel {
	std::optional<TransferOutput> current;
	std::size_t consumed = 0;
	bool session_open(const TrustedSession &session) const { return current && current->session == session; }
	std::optional<TransferOutput> outbound() { return current; }
	void consume_output(Token token, std::size_t size) {
		assert(current && current->token == token && current->bytes.size() == size);
		consumed += size;
		current.reset();
	}
};
int main() {
	for (unsigned change = 0; change != 5; ++change) {
		auto budget = std::make_shared<ByteBudget>(32);
		TrustedSession session{2, fresh_connection_identity(), fresh_connection_identity()};
		std::string original("snapshot");
		TransferOutput output{session, 9, {original.data(), original.size()}};
		Channel channel{output};
		{
			SnapshotWrite flight(output, budget);
			auto owned = flight.take_frame();
			// Retire the borrowed allocation while the original write still owns
			// its independent bytes, exactly as timeout/disconnect would do.
			original.assign(1024, 'x');
			assert(owned.bytes() == "snapshot" && budget->used() == 8);
			std::string replacement("snapshot");
			channel.current->bytes = {replacement.data(), replacement.size()};
			switch (change) {
			case 1:
				channel.current.reset();
				break;
			case 2:
				channel.current->session.remote = fresh_connection_identity();
				break;
			case 3:
				++channel.current->token;
				break;
			case 4:
				channel.current->bytes = channel.current->bytes.subspan(1);
				break;
			}
			assert(flight.consume_if_current(channel) == (change == 0));
			assert(channel.consumed == (change == 0 ? 8 : 0));
			try {
				(void)flight.consume_if_current(channel);
				assert(false);
			} catch (const std::logic_error &) {
			}
			assert(budget->used() == 8);
		}
		assert(budget->used() == 0 && budget->high_water() == 8);
		{
			ByteCredit credit(budget, 16);
			assert(budget->used() == 16 && budget->high_water() == 16);
		}
		assert(budget->used() == 0 && budget->high_water() == 16);
	}
	std::cout << "snapshot writes: owned bytes, session/token/offset correlation and credit passed\n";
}
