// peers.h compiles for any host with worker() and channel(): Peers is explicitly
// instantiated over a minimal one. Detent's transport tests run it against its node.
#include "consensus/transport/peers.h"

#include <cstdio>
#include <cstdlib>

namespace {
struct MinimalHost {
	kronuz::consensus::Worker& worker() { std::abort(); }
	kronuz::consensus::SnapshotChannel& channel() { std::abort(); }
};
}  // namespace

static_assert(kronuz::consensus::transport::PeerHost<MinimalHost>);
template class kronuz::consensus::transport::Peers<MinimalHost>;

int main() {
	std::printf("peers: ok\n");
	return EXIT_SUCCESS;
}
