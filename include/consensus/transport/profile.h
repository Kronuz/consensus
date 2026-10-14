#pragma once
#include "hello.h"
#include <openssl/evp.h>
#include <string_view>

namespace kronuz::consensus::transport {
// What two voters must agree on before they talk: the application's own profile bytes
// (its semantic limits, opaque here), the cluster and configuration identities, the voters,
// and the consensus limits, hashed with SHA-256 under the tag DTPROF01.
inline Fingerprint profile_fingerprint(std::string_view application,
									   const kronuz::consensus::FixedConfiguration &configuration,
									   const kronuz::consensus::Limits &consensus) {
	using kronuz::journal::put64;
	auto voters = configuration.voters;
	std::sort(voters.begin(), voters.end());
	if (voters.empty() || voters.size() > consensus.voters || voters.front() == 0 ||
		std::adjacent_find(voters.begin(), voters.end()) != voters.end() ||
		!std::binary_search(voters.begin(), voters.end(), configuration.local))
		throw std::invalid_argument("invalid profile membership");
	std::string bytes("DTPROF01", 8);
	bytes += application;
	for (auto id : {configuration.cluster, configuration.configuration})
		bytes.append(id.data(), id.size());
	put64(bytes, voters.size());
	for (auto voter : voters)
		put64(bytes, voter);
	for (auto limit : {consensus.voters, consensus.command_bytes, consensus.log_bytes, consensus.log_entries,
					   consensus.control_entries, consensus.rpc_bytes, consensus.rpc_entries, consensus.reads,
					   consensus.uncommitted_entries})
		put64(bytes, limit);
	Fingerprint result{};
	unsigned length = 0;
	if (EVP_Digest(bytes.data(), bytes.size(), result.data(), &length, EVP_sha256(), nullptr) != 1 || length != result.size()) {
		throw std::runtime_error("profile fingerprint");
	}
	return result;
}
} // namespace kronuz::consensus::transport
