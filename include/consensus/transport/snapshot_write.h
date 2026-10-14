#pragma once
#include "session.h"
#include <consensus/snapshot_channel.h>

namespace kronuz::consensus::transport {
// SnapshotChannel exposes a borrowed frame. This copy remains owned even when
// timeout, fencing or disconnect destroys that original while a write waits.
class SnapshotWrite {
  public:
	SnapshotWrite(const kronuz::consensus::TransferOutput &output, std::shared_ptr<ByteBudget> budget)
		: session_(output.session), token_(output.token), size_(output.bytes.size()),
		  frame_(std::move(budget), size_) {
		if (!token_ || !size_)
			throw std::invalid_argument("invalid snapshot write flight");
		frame_.assign({output.bytes.data(), size_});
	}
	SnapshotWrite(const SnapshotWrite &) = delete;
	const Frame &frame() const noexcept { return frame_; }
	Frame take_frame() {
		if (taken_)
			throw std::logic_error("snapshot write already started");
		taken_ = true;
		return std::move(frame_);
	}
	// Call only after the original complete write has been reaped successfully.
	// Old completion never consumes a replacement connection's selected frame.
	template <class Channel> bool consume_if_current(Channel &channel) {
		if (!taken_ || settled_)
			throw std::logic_error("snapshot write settlement state");
		settled_ = true;
		if (!channel.session_open(session_))
			return false;
		auto current = channel.outbound();
		if (!current || current->session != session_ || current->token != token_ ||
			current->bytes.size() != size_)
			return false;
		channel.consume_output(token_, size_);
		return true;
	}

  private:
	kronuz::consensus::TrustedSession session_;
	kronuz::consensus::Token token_;
	std::size_t size_;
	Frame frame_;
	bool taken_ = false, settled_ = false;
};
} // namespace kronuz::consensus::transport
