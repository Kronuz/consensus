#pragma once
#include <asio.hpp>
#include <unistd.h>

// Test transport with the same asynchronous byte-stream contract as a socket.
// Duplex pipes exercise TLS readiness and cancellation without a network bind.
class AsyncPipe {
  public:
	using executor_type = asio::any_io_executor;
	using lowest_layer_type = AsyncPipe;
	AsyncPipe(asio::io_context &io, int input, int output) : input_(io, input), output_(io, output) {}
	AsyncPipe(AsyncPipe &&) = default;
	executor_type get_executor() noexcept { return input_.get_executor(); }
	AsyncPipe &lowest_layer() noexcept { return *this; }
	const AsyncPipe &lowest_layer() const noexcept { return *this; }
	template <class Buffers, class Token> auto async_read_some(const Buffers &buffers, Token &&token) {
		return input_.async_read_some(buffers, std::forward<Token>(token));
	}
	template <class Buffers, class Token> auto async_write_some(const Buffers &buffers, Token &&token) {
		return output_.async_write_some(buffers, std::forward<Token>(token));
	}
	void close(asio::error_code &error) {
		input_.close(error);
		asio::error_code other;
		output_.close(other);
		if (!error)
			error = other;
	}
	static std::pair<AsyncPipe, AsyncPipe> pair(asio::io_context &io) {
		int first[2], second[2];
		if (::pipe(first) != 0)
			throw std::runtime_error("test pipe allocation");
		if (::pipe(second) != 0) {
			::close(first[0]);
			::close(first[1]);
			throw std::runtime_error("test pipe allocation");
		}
		return {AsyncPipe(io, first[0], second[1]), AsyncPipe(io, second[0], first[1])};
	}

  private:
	asio::posix::stream_descriptor input_, output_;
};
