// Probe only (probe/detent/fetch-stall, never lands): groups_test's loop, socket and kernel probes.
#pragma once
#include "rpc_hosts.h"
#include <algorithm>
#include <cstring>
#include <fstream>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <unistd.h>
#ifdef __APPLE__
#include <cxxabi.h>
#include <dlfcn.h>
#include <mach/mach.h>
#include <mach/mach_vm.h>
#include <sys/mbuf.h>
#include <sys/sysctl.h>
#endif
// Each loopback TCP socket of this process on `ports`: local and peer port, bytes unread
// (FIONREAD), bytes in its send buffer, and on macOS the windows, cwnd, RTO and retransmits.
std::string sockets(std::initializer_list<std::uint16_t> ports) {
	std::string r;
	for (int fd = 3; fd != 1024; ++fd) {
		sockaddr_in local{}, peer{};
		socklen_t a = sizeof local, b = sizeof peer;
		if (::getsockname(fd, reinterpret_cast<sockaddr *>(&local), &a) != 0 || local.sin_family != AF_INET ||
			::getpeername(fd, reinterpret_cast<sockaddr *>(&peer), &b) != 0)
			continue;
		const auto lp = ntohs(local.sin_port), pp = ntohs(peer.sin_port);
		if (std::find(ports.begin(), ports.end(), lp) == ports.end() && std::find(ports.begin(), ports.end(), pp) == ports.end())
			continue;
		int unread = -1, unsent = -1;
		::ioctl(fd, FIONREAD, &unread);
#if defined(SO_NWRITE)
		socklen_t n = sizeof unsent;
		::getsockopt(fd, SOL_SOCKET, SO_NWRITE, &unsent, &n);
#elif defined(SIOCOUTQ)
		::ioctl(fd, SIOCOUTQ, &unsent);
#endif
		char line[256];
		std::snprintf(line, sizeof line, " [%u>%u unread %d sndq %d", lp, pp, unread, unsent);
		r += line;
#if defined(TCP_CONNECTION_INFO)
		tcp_connection_info i{};
		socklen_t m = sizeof i;
		if (::getsockopt(fd, IPPROTO_TCP, TCP_CONNECTION_INFO, &i, &m) == 0) {
			std::snprintf(line, sizeof line, " sndwnd %u rcvwnd %u cwnd %u rto %u srtt %u rexmit %llu", i.tcpi_snd_wnd, i.tcpi_rcv_wnd,
						  i.tcpi_snd_cwnd, i.tcpi_rto, i.tcpi_srtt, (unsigned long long)i.tcpi_txretransmitpackets);
			r += line;
		}
#endif
		r += "]";
	}
	return r;
}
// Kernel stacks of a loop stuck in an uninterruptible kernel wait (macOS on a CI runner, or
// GROUPS_PROBE_SPINDUMP naming the file): the first such sample starts `sudo -n spindump` on this
// process for 3 s in the background; a failed run prints the report's threads in write, a socket
// call or the sampler. Kernel frames print raw, `kernel + 0xN` from the loaded kernel's base: a
// runner's kernel exports too few symbols to name them (nearest-export names are wrong), so they are
// symbolicated offline against the Kernel Debug Kit of the printed build and UUID (CI downloads none).
class Spindump {
  public:
	void start() {
#ifdef __APPLE__
		const char *named = std::getenv("GROUPS_PROBE_SPINDUMP"), *ci = std::getenv("GITHUB_ACTIONS");
		if (started_ || !((named && *named) || (ci && std::strcmp(ci, "true") == 0)))
			return;
		started_ = true;
		path_ = named && *named ? named : "spindump-fetch-" + std::to_string(::getpid()) + ".txt";
		// .build: the OS build; .text: the on-disk kernel's unslid __TEXT address.
		const std::string command = "(sudo -n spindump " + std::to_string(::getpid()) + " 3 10 -o '" + path_ + "' > /dev/null 2>&1; sw_vers -buildVersion > '" +
									path_ + ".build' 2> /dev/null; otool -l /System/Library/Kernels/kernel 2> /dev/null | awk '/segname __TEXT$/{f=1} f&&/vmaddr/{print $2; exit}' > '" +
									path_ + ".text'; touch '" + path_ + ".done') &";
		(void)std::system(command.c_str());
#endif
	}
	// Waits up to 30 s for the report, then prints the build, the kernel's UUID, base and slide, and its threads that match.
	void print() const {
		if (!started_)
			return;
		for (int i = 0; i != 300 && !std::filesystem::exists(path_ + ".done"); ++i)
			std::this_thread::sleep_for(100ms);
		std::ifstream in(path_);
		if (!in) {
			std::printf("probe: spindump %s: no report (sudo -n spindump failed)\n", path_.c_str());
			return;
		}
		std::string line, block, image;
		std::vector<std::string> lines;
		while (std::getline(in, line)) {
			if (image.empty() && line.find("kernel (") != std::string::npos && line.find("/System/Library/Kernels/kernel") != std::string::npos)
				image = line;
			lines.push_back(std::move(line));
		}
		const auto uuid = image.find('<') == std::string::npos ? std::string("unknown") : image.substr(image.find('<') + 1, image.find('>') - image.find('<') - 1);
		const auto base = image.find("0x") == std::string::npos ? 0ull : std::strtoull(image.c_str() + image.find("0x"), nullptr, 16);
		const auto unslid = std::strtoull(first_line(path_ + ".text").c_str(), nullptr, 16);
		char slide[40] = "unknown";
		if (base && unslid)
			std::snprintf(slide, sizeof slide, "0x%llx", base - unslid);
		std::printf("probe: spindump %s: kernel frames are raw (kernel + 0xN, N from the loaded base); symbolicate offline against the "
					"Kernel Debug Kit for the OS build and UUID below (CI does not download it)\n"
					"probe: kernel build %s uuid %s base 0x%llx slide %s unslid __TEXT 0x%llx\n",
					path_.c_str(), first_line(path_ + ".build").c_str(), uuid.c_str(), base, slide, unslid);
		int printed = 0;
		const auto flush = [&] {
			for (const char *mark : {"write", "flush", "getsockopt", "ioctl", "getsockname", "getpeername", "sockets("})
				if (block.find(mark) != std::string::npos && printed < 600) {
					std::fputs(block.c_str(), stdout);
					printed += int(std::count(block.begin(), block.end(), '\n'));
					break;
				}
			block.clear();
		};
		for (const auto &l : lines) {
			if (l.rfind("  Thread 0x", 0) == 0)
				flush();
			if (!block.empty() || l.rfind("  Thread 0x", 0) == 0)
				block += raw(l) + "\n";
		}
		flush();
	}

  private:
	static std::string first_line(const std::string &path) {
		std::ifstream in(path);
		std::string r;
		std::getline(in, r);
		return r;
	}
	// "??? (kernel + N) [0xADDRESS]" as "kernel + 0xN [0xADDRESS]".
	static std::string raw(const std::string &line) {
		static const std::string mark = "??? (kernel + ";
		const auto at = line.find(mark);
		if (at == std::string::npos)
			return line;
		char *end = nullptr;
		const auto offset = std::strtoull(line.c_str() + at + mark.size(), &end, 10);
		if (*end != ')')
			return line;
		char hex[40];
		std::snprintf(hex, sizeof hex, "kernel + 0x%llx", offset);
		return line.substr(0, at) + hex + (end + 1);
	}
	bool started_ = false;
	std::string path_;
};
// Each loop of the hosts, every sample: how long the stamp posted to it last has waited (0 once
// it ran), its thread's CPU ms since the last sample, its run state on macOS (R runnable, W
// waiting, U uninterruptible, S stopped) and, once a stamp waits 250 ms, the thread's stack.
// A stuck loop that waits is blocked in a call; one with CPU and a moving stack runs a long
// handler; one runnable with a stack that does not move is a thread the host does not run.
class LoopProbe {
  public:
	explicit LoopProbe(std::initializer_list<Host *> hosts) {
		for (auto *h : hosts)
			for (std::uint32_t k = 0; k != h->loops.size(); ++k)
				loops_.push_back({h->node, k, h->loops[k].get()});
	}
	std::string sample() {
		std::string r;
		for (auto &l : loops_) {
			const auto now = steady::now();
			const std::uint64_t age = *l.ran ? 0 : std::uint64_t(std::chrono::duration_cast<std::chrono::milliseconds>(now - l.posted).count());
			if (*l.ran) {
				*l.ran = false;
				l.posted = now;
				asio::post(l.loop->io, [ran = l.ran] { *ran = true; });
			}
			char state = '?';
			const auto cpu = cpu_us(*l.loop, state);
			char line[96];
			std::snprintf(line, sizeof line, " {%llu.%u age %llu cpu %llu %c", (unsigned long long)l.node, l.k, (unsigned long long)age,
						  (unsigned long long)((cpu - std::min(cpu, l.cpu)) / 1000), state);
			l.cpu = cpu;
			r += line;
			std::string where;
			if (age >= 250) {
				if (state == 'U')
					spindump_.start();
				where = stack(*l.loop);
				r += where;
			}
			if (age > l.worst) {
				l.worst = age;
				l.at = std::string(1, state) + where;
			}
			r += "}";
		}
		return r;
	}
	const Spindump &spindump() const noexcept { return spindump_; }
	// Each loop's longest stall seen (a posted stamp's wait), its state and stack then.
	std::string summary() const {
		std::string r;
		for (auto &l : loops_) {
			char line[64];
			std::snprintf(line, sizeof line, " {%llu.%u worst %llu ms ", (unsigned long long)l.node, l.k, (unsigned long long)l.worst);
			r += line + l.at + "}";
		}
		return r;
	}

  private:
	struct Probed {
		NodeId node;
		std::uint32_t k;
		Loop *loop;
		std::shared_ptr<std::atomic<bool>> ran = std::make_shared<std::atomic<bool>>(true);
		steady::time_point posted{};
		std::uint64_t cpu = 0, worst = 0;
		std::string at;
	};
	// The thread's user and system time in microseconds, and its run state.
	static std::uint64_t cpu_us(Loop &loop, char &state) {
#ifdef __APPLE__
		thread_basic_info_data_t info{};
		mach_msg_type_number_t n = THREAD_BASIC_INFO_COUNT;
		if (thread_info(pthread_mach_thread_np(loop.thread.native_handle()), THREAD_BASIC_INFO, thread_info_t(&info), &n) !=
			KERN_SUCCESS)
			return 0;
		state = "?RSWUH"[info.run_state >= 1 && info.run_state <= 5 ? info.run_state : 0];
		return std::uint64_t(info.user_time.seconds + info.system_time.seconds) * 1000000 +
			   std::uint64_t(info.user_time.microseconds + info.system_time.microseconds);
#else
		clockid_t clock;
		timespec t{};
		if (pthread_getcpuclockid(loop.thread.native_handle(), &clock) != 0 || clock_gettime(clock, &t) != 0)
			return 0;
		return std::uint64_t(t.tv_sec) * 1000000 + std::uint64_t(t.tv_nsec) / 1000;
#endif
	}
	// The thread's program counter and up to 7 return addresses by its frame pointers, read while
	// it is suspended and named once it resumes (dladdr may take a lock the thread holds).
	static std::string stack(Loop &loop) {
		std::string r;
#ifdef __APPLE__
		const auto thread = pthread_mach_thread_np(loop.thread.native_handle());
		std::uintptr_t pcs[8] = {}, pc = 0, fp = 0;
		std::size_t n = 0;
		if (thread_suspend(thread) != KERN_SUCCESS)
			return r;
#if defined(__x86_64__)
		x86_thread_state64_t s{};
		mach_msg_type_number_t count = x86_THREAD_STATE64_COUNT;
		if (thread_get_state(thread, x86_THREAD_STATE64, thread_state_t(&s), &count) == KERN_SUCCESS)
			pc = s.__rip, fp = s.__rbp;
#elif defined(__arm64__)
		arm_thread_state64_t s{};
		mach_msg_type_number_t count = ARM_THREAD_STATE64_COUNT;
		if (thread_get_state(thread, ARM_THREAD_STATE64, thread_state_t(&s), &count) == KERN_SUCCESS)
			pc = arm_thread_state64_get_pc(s), fp = arm_thread_state64_get_fp(s);
#endif
		pcs[n++] = pc;
		while (fp && n != 8) {
			std::uintptr_t frame[2] = {};
			mach_vm_size_t got = 0;
			if (mach_vm_read_overwrite(mach_task_self(), fp, sizeof frame, mach_vm_address_t(frame), &got) != KERN_SUCCESS ||
				got != sizeof frame || frame[0] <= fp)
				break;
			pcs[n++] = frame[1];
			fp = frame[0];
		}
		thread_resume(thread);
		for (std::size_t i = 0; i != n && pcs[i]; ++i) {
			Dl_info info{};
			std::string name = "?";
			std::uintptr_t base = 0;
			if (dladdr(reinterpret_cast<void *>(pcs[i]), &info) && info.dli_sname) {
				int status = 0;
				char *plain = abi::__cxa_demangle(info.dli_sname, nullptr, nullptr, &status);
				name = plain ? plain : info.dli_sname;
				std::free(plain);
				base = reinterpret_cast<std::uintptr_t>(info.dli_saddr);
			} else if (info.dli_fname) {
				name = std::filesystem::path(info.dli_fname).filename().string();
				base = reinterpret_cast<std::uintptr_t>(info.dli_fbase);
			}
			if (name.size() > 72)
				name.resize(72);
			char at[24];
			std::snprintf(at, sizeof at, "+0x%llx", (unsigned long long)(pcs[i] - base));
			r += (i ? " < " : " pc ") + name + at;
		}
#else
		(void)loop;
#endif
		return r;
	}
	std::vector<Probed> loops_;
	Spindump spindump_;
};
// The kernel's network memory and pages each sample, counters as deltas (macOS): mbuf
// allocations that waited or drained the protocols, the 16 KB clusters active of those created
// and their failed allocations, free pages, threads waiting for one, page-ins, swap-ins and
// decompressions. A write that sleeps in the kernel waits for one of these.
class KernelProbe {
  public:
	KernelProbe() { (void)sample(); }
	std::string sample() {
		std::string r;
#ifdef __APPLE__
		mbstat m{};
		std::size_t n = sizeof m;
		if (::sysctlbyname("kern.ipc.mbstat", &m, &n, nullptr, 0) != 0)
			return r;
		alignas(mb_stat_t) unsigned char buffer[4096];
		n = sizeof buffer;
		const mb_class_stat_t *jumbo = nullptr;
		if (::sysctlbyname("kern.ipc.mb_stat", buffer, &n, nullptr, 0) == 0) {
			const auto *s = reinterpret_cast<const mb_stat_t *>(buffer);
			for (std::uint32_t i = 0; i != s->mbs_cnt && offsetof(mb_stat_t, mbs_class) + (i + 1) * sizeof(mb_class_stat_t) <= n; ++i)
				if (std::strcmp(s->mbs_class[i].mbcl_cname, "16kcl") == 0)
					jumbo = &s->mbs_class[i];
		}
		unsigned wanted = 0;
		n = sizeof wanted;
		(void)::sysctlbyname("vm.page_free_wanted", &wanted, &n, nullptr, 0);
		vm_statistics64_data_t vm{};
		mach_msg_type_number_t count = HOST_VM_INFO64_COUNT;
		(void)host_statistics64(mach_host_self(), HOST_VM_INFO64, host_info64_t(&vm), &count);
		const Counters now{m.m_wait, m.m_drain, jumbo ? jumbo->mbcl_fail_cnt : 0, vm.pageins, vm.swapins, vm.decompressions};
		char line[256];
		std::snprintf(line, sizeof line,
					  " {kern wait +%llu drain +%llu 16kcl %u/%u fail +%llu; vm free %llu MiB wanted %u pageins +%llu swapins +%llu decomp +%llu}",
					  delta(now.wait, last_.wait), delta(now.drain, last_.drain), jumbo ? jumbo->mbcl_active : 0, jumbo ? jumbo->mbcl_total : 0,
					  delta(now.fail, last_.fail), (unsigned long long)vm.free_count * vm_page_size >> 20, wanted, delta(now.pageins, last_.pageins),
					  delta(now.swapins, last_.swapins), delta(now.decompressions, last_.decompressions));
		r = line;
		last_ = now;
#endif
		return r;
	}

  private:
	struct Counters {
		std::uint64_t wait = 0, drain = 0, fail = 0, pageins = 0, swapins = 0, decompressions = 0;
	};
	static unsigned long long delta(std::uint64_t now, std::uint64_t then) { return now - std::min(now, then); }
	Counters last_;
};
