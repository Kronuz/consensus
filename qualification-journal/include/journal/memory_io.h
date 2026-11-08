/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

// An in-memory filesystem for power-loss tests: each unsynced 512-byte sector of a file
// persists or not at power loss, and namespace changes persist only after a directory sync.
// Disk::cut makes the I/O operation of that number, and every later one, throw PowerLoss;
// power_loss() then leaves the state a crash would. Test support, not a storage backend.

#pragma once

#include "completion/io.h"
#include <algorithm>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

namespace kronuz::journal::testing {
namespace j = kronuz::journal;

struct PowerLoss {};
constexpr std::size_t sector = 512;

// Inodes hold durable bytes and pending sector writes; names map to inodes, with the
// durable namespace changing only at sync_directory.
struct Disk {
	struct Inode { std::string durable; std::map<std::uint64_t, std::string> pending; std::uint64_t size = 0; };
	std::map<std::uint64_t, Inode> inodes; std::map<std::string, std::uint64_t> names, durable_names;
	std::uint64_t next_inode = 1, operations = 0, cut = std::numeric_limits<std::uint64_t>::max();
	std::mt19937_64 random{0};
	// From the cut on, every operation fails: a disk without power stays without it, even
	// when a caller turns the first failure into an error and carries on.
	void tick() { if (++operations >= cut) { throw PowerLoss{}; } }
	bool dead() const noexcept { return operations >= cut; }
	std::string content(const Inode& inode) const {
		std::string bytes = inode.durable; bytes.resize(inode.size, '\0');
		for (const auto& [s, data] : inode.pending) {
			const std::size_t at = static_cast<std::size_t>(s) * sector;
			if (at < bytes.size()) { auto count = std::min(sector, bytes.size() - at); bytes.replace(at, count, data.substr(0, count)); }
		}
		return bytes;
	}
	// The state after power loss: the durable namespace, and each pending sector kept or not.
	void power_loss() {
		std::map<std::uint64_t, Inode> survived;
		for (const auto& [name, id] : durable_names) {
			auto& inode = inodes.at(id); auto& after = survived[id];
			after.durable = inode.durable; after.size = std::max<std::uint64_t>(inode.durable.size(), 0);
			for (const auto& [s, data] : inode.pending) {
				if (random() % 2) { continue; }
				auto end = s * sector + data.size();
				if (after.durable.size() < end) { after.durable.resize(end, '\0'); }
				after.durable.replace(s * sector, data.size(), data);
			}
			after.size = after.durable.size();
		}
		inodes = std::move(survived); names = durable_names;
		cut = std::numeric_limits<std::uint64_t>::max();
	}
};

class MemoryFile final : public j::File {
public:
	MemoryFile(Disk& disk, std::uint64_t inode) : disk_(disk), inode_(inode) {}
	std::uint64_t size() override { return node().size; }
	std::size_t read_at(std::uint64_t offset, std::span<char> bytes) override {
		auto content = disk_.content(node());
		if (offset >= content.size()) { return 0; }
		auto count = std::min<std::size_t>(bytes.size(), content.size() - offset);
		std::copy_n(content.data() + offset, count, bytes.data()); return count;
	}
	std::size_t write_at(std::uint64_t offset, std::string_view bytes) override {
		disk_.tick();
		auto& inode = node(); auto content = disk_.content(inode);
		if (content.size() < offset + bytes.size()) { content.resize(offset + bytes.size(), '\0'); }
		content.replace(offset, bytes.size(), bytes);
		inode.size = content.size();
		for (auto s = offset / sector; s * sector < offset + bytes.size(); ++s) { inode.pending[s] = content.substr(s * sector, sector); }
		return bytes.size();
	}
	void truncate(std::uint64_t size) override { disk_.tick(); sync(); node().durable.resize(size, '\0'); node().size = size; }
	void sync() override {
		disk_.tick();
		auto& inode = node(); inode.durable = disk_.content(inode); inode.pending.clear();
	}
private:
	Disk::Inode& node() { return disk_.inodes.at(inode_); }
	Disk& disk_; std::uint64_t inode_;
};
class MemoryCursor final : public j::DirectoryCursor {
public:
	explicit MemoryCursor(std::vector<std::string> names) : names_(std::move(names)) {}
	std::optional<std::string> next() override { if (at_ == names_.size()) { return std::nullopt; } return names_[at_++]; }
private:
	std::vector<std::string> names_; std::size_t at_ = 0;
};
// One directory of the disk: names under `prefix`, so several directories share one disk
// and one power cut. sync_directory makes only this directory's namespace durable.
class MemoryIO final : public j::IO {
public:
	explicit MemoryIO(Disk& disk, std::string prefix = {}) : disk_(disk), prefix_(std::move(prefix)) {}
	std::unique_ptr<j::OwnerLock> acquire_owner(bool) override { return std::make_unique<j::OwnerLock>(); }
	std::unique_ptr<j::File> open_existing(std::string_view name) override {
		auto found = disk_.names.find(path(name));
		if (found == disk_.names.end()) { throw std::runtime_error("no such file"); }
		return std::make_unique<MemoryFile>(disk_, found->second);
	}
	std::unique_ptr<j::File> create_exclusive(std::string_view name) override {
		disk_.tick();
		if (disk_.names.contains(path(name))) { throw std::runtime_error("file exists"); }
		auto id = disk_.next_inode++; disk_.inodes[id]; disk_.names[path(name)] = id;
		return std::make_unique<MemoryFile>(disk_, id);
	}
	void replace(std::string_view source, std::string_view destination) override {
		disk_.tick();
		auto id = disk_.names.at(path(source)); disk_.names.erase(path(source)); disk_.names[path(destination)] = id;
	}
	void remove(std::string_view name) override { disk_.tick(); disk_.names.erase(path(name)); }
	std::unique_ptr<j::DirectoryCursor> scan_directory() override {
		std::vector<std::string> names;
		for (const auto& [name, _] : disk_.names) { if (mine(name)) { names.push_back(name.substr(prefix_.size())); } }
		return std::make_unique<MemoryCursor>(std::move(names));
	}
	std::optional<j::EntryFootprint> entry_footprint(std::string_view name) override {
		auto found = disk_.names.find(path(name));
		if (found == disk_.names.end()) { return std::nullopt; }
		j::EntryFootprint footprint; footprint.kind = j::EntryKind::Regular; footprint.logical_bytes = disk_.inodes.at(found->second).size;
		return footprint;
	}
	void sync_directory() override {
		disk_.tick();
		std::erase_if(disk_.durable_names, [&](const auto& entry) { return mine(entry.first); });
		for (const auto& [name, id] : disk_.names) { if (mine(name)) { disk_.durable_names[name] = id; } }
	}
private:
	std::string path(std::string_view name) const { return prefix_ + std::string(name); }
	// A name of this directory, not of a nested one.
	bool mine(const std::string& name) const { return name.starts_with(prefix_) && name.find('/', prefix_.size()) == std::string::npos; }
	Disk& disk_;
	std::string prefix_;
};


}  // namespace kronuz::journal::testing
