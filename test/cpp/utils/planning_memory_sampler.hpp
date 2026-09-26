#pragma once

#include <dlfcn.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#if defined(__GLIBC__)
#include <malloc.h>
#endif

namespace sirius::test {
struct allocator_reading {
  uint64_t system_bytes        = 0;
  uint64_t duckdb_bytes        = 0;
  uint64_t system_active_bytes = 0;
  uint64_t duckdb_active_bytes = 0;
  bool valid                   = false;
  uint64_t total() const noexcept { return system_bytes + duckdb_bytes; }
};

// Statistics describe allocator-observed bytes. Neither allocator gives a historical
// process peak; tcaches, allocator metadata and sampling gaps limit the interpretation.
class allocator_reader {
 public:
  virtual ~allocator_reader()               = default;
  virtual allocator_reading read() noexcept = 0;
};
class process_allocator_reader final : public allocator_reader {
 public:
  using mallctl_fn = int (*)(char const*, void*, size_t*, void*, size_t);
  explicit process_allocator_reader(std::string const& system_allocator) : xml_(1024 * 1024, '\0')
  {
    duckdb_ = reinterpret_cast<mallctl_fn>(dlsym(RTLD_DEFAULT, "duckdb_je_mallctl"));
    if (system_allocator == "jemalloc") {
      system_ = reinterpret_cast<mallctl_fn>(dlsym(RTLD_DEFAULT, "mallctl"));
      if (!system_) throw std::runtime_error("process jemalloc mallctl is unavailable");
      Dl_info malloc_owner{}, control_owner{};
      if (!dladdr(dlsym(RTLD_DEFAULT, "malloc"), &malloc_owner) ||
          !dladdr(reinterpret_cast<void*>(system_), &control_owner) ||
          malloc_owner.dli_fbase != control_owner.dli_fbase) {
        throw std::runtime_error("mallctl does not belong to the process malloc provider");
      }
      if (system_ == duckdb_) duckdb_ = nullptr;  // Never sum the same allocator twice.
    } else if (system_allocator == "glibc") {
#if defined(__GLIBC__)
      Dl_info owner{};
      if (!dladdr(dlsym(RTLD_DEFAULT, "malloc"), &owner) || !owner.dli_fname ||
          !std::strstr(owner.dli_fname, "libc.so")) {
        throw std::runtime_error("glibc statistics cannot describe the process malloc provider");
      }
      stream_ = fmemopen(xml_.data(), xml_.size(), "w+");
      if (!stream_) throw std::runtime_error("cannot create allocator statistics buffer");
      setvbuf(stream_, nullptr, _IONBF, 0);
#else
      throw std::runtime_error("glibc allocator statistics are unavailable");
#endif
    } else {
      throw std::invalid_argument("SCAN_PLANNING_ALLOCATOR must be glibc or jemalloc");
    }
    // Initialize allocator statistics and stdio before any measured window.
    if (!read().valid) {
      if (stream_) std::fclose(stream_);
      stream_ = nullptr;
      throw std::runtime_error("allocator statistics are unavailable or incomplete");
    }
  }
  ~process_allocator_reader() override
  {
    if (stream_) std::fclose(stream_);
  }
  bool has_duckdb_statistics() const noexcept { return duckdb_ != nullptr; }
  allocator_reading read() noexcept override
  {
    allocator_reading result;
    bool ok = false;
    if (system_) {
      ok = jemalloc_read(system_, result.system_bytes, result.system_active_bytes);
    } else {
#if defined(__GLIBC__)
      std::rewind(stream_);
      std::clearerr(stream_);
      if (malloc_info(0, stream_) == 0 && std::fflush(stream_) == 0) {
        auto length = std::ftell(stream_);
        if (length > 0 && static_cast<size_t>(length) < xml_.size()) {
          xml_[length]     = '\0';
          uint64_t current = 0, fast = 0, rest = 0, mapped = 0;
          ok = last_attribute("<system type=\"current\"", "size=\"", current) &&
               (!std::strstr(xml_.data(), "<total type=\"fast\"") ||
                last_attribute("<total type=\"fast\"", "size=\"", fast)) &&
               last_attribute("<total type=\"rest\"", "size=\"", rest) &&
               last_attribute("<total type=\"mmap\"", "size=\"", mapped) &&
               current >= fast + rest && std::strstr(xml_.data(), "</malloc>");
          if (ok) {
            // glibc arena in-use estimate plus its mmap allocations; not requested bytes.
            result.system_bytes        = current - fast - rest + mapped;
            result.system_active_bytes = current + mapped;
          }
        }
      }
#endif
    }
    if (duckdb_) ok = jemalloc_read(duckdb_, result.duckdb_bytes, result.duckdb_active_bytes) && ok;
    result.valid = ok;
    return result;
  }

 private:
  static bool jemalloc_read(mallctl_fn control, uint64_t& allocated, uint64_t& active) noexcept
  {
    uint64_t epoch    = 1;
    size_t epoch_size = sizeof(epoch), allocated_value = 0, active_value = 0;
    size_t length = sizeof(size_t);
    if (control("epoch", &epoch, &epoch_size, &epoch, sizeof(epoch)) != 0 ||
        control("stats.allocated", &allocated_value, &length, nullptr, 0) != 0)
      return false;
    length = sizeof(size_t);
    if (control("stats.active", &active_value, &length, nullptr, 0) != 0) return false;
    allocated = allocated_value;
    active    = active_value;
    return true;
  }
  bool last_attribute(char const* element, char const* attribute, uint64_t& value) noexcept
  {
    auto next        = std::strstr(xml_.data(), element);
    char const* last = nullptr;
    while (next) {
      last = next;
      next = std::strstr(next + 1, element);
    }
    if (!last) return false;
    auto end   = std::strchr(last, '>');
    auto found = std::strstr(last, attribute);
    if (!found || !end || found >= end) return false;
    char* parsed_end = nullptr;
    value            = std::strtoull(found + std::strlen(attribute), &parsed_end, 10);
    return parsed_end && parsed_end < end && *parsed_end == '"';
  }
  std::vector<char> xml_;
  FILE* stream_      = nullptr;
  mallctl_fn system_ = nullptr;
  mallctl_fn duckdb_ = nullptr;
};

struct memory_result {
  allocator_reading before, peak, after;
  uint64_t samples        = 0;
  uint64_t maximum_gap_us = 0;
  bool valid              = true;
  uint64_t peak_added_bytes() const noexcept { return peak.total() - before.total(); }
  int64_t retained_delta_bytes() const noexcept
  {
    return static_cast<int64_t>(after.total()) - static_cast<int64_t>(before.total());
  }
};

class planning_memory_sampler {
 public:
  // A zero interval selects boundary-only sampling for deterministic observer tests.
  planning_memory_sampler(allocator_reader& reader, std::chrono::microseconds interval)
    : reader_(reader), interval_(interval)
  {
    if (interval_.count() > 0) {
      worker_ = std::thread([this] {
        std::unique_lock<std::mutex> lock(mutex_);
        (void)reader_.read();  // Warm statistics on the sampling thread before the baseline.
        ready_ = true;
        condition_.notify_all();
        while (!quit_) {
          condition_.wait(lock, [this] { return quit_ || running_; });
          if (quit_) break;
          sample_locked();
          condition_.wait_for(lock, interval_, [this] { return quit_ || !running_; });
        }
      });
      std::unique_lock<std::mutex> lock(mutex_);
      condition_.wait(lock, [this] { return ready_; });
    }
  }
  ~planning_memory_sampler()
  {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      quit_    = true;
      running_ = false;
    }
    condition_.notify_all();
    if (worker_.joinable()) worker_.join();
  }
  void start() noexcept
  {
    std::lock_guard<std::mutex> lock(mutex_);
    result_        = {};
    result_.before = reader_.read();
    result_.peak = result_.after = result_.before;
    result_.valid                = result_.before.valid;
    result_.samples              = 1;
    last_                        = std::chrono::steady_clock::now();
    running_                     = true;
    condition_.notify_all();
  }
  void record_peak() noexcept
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (running_) sample_locked();
  }
  memory_result stop() noexcept
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (running_) sample_locked();
    running_ = false;
    condition_.notify_all();
    return result_;
  }

 private:
  void sample_locked() noexcept
  {
    auto reading = reader_.read();
    auto now     = std::chrono::steady_clock::now();
    auto gap     = std::chrono::duration_cast<std::chrono::microseconds>(now - last_).count();
    result_.maximum_gap_us = std::max(result_.maximum_gap_us, static_cast<uint64_t>(gap));
    last_                  = now;
    ++result_.samples;
    result_.valid = result_.valid && reading.valid;
    if (reading.valid) {
      if (reading.total() > result_.peak.total()) result_.peak = reading;
      result_.after = reading;
    }
  }
  allocator_reader& reader_;
  std::chrono::microseconds interval_;
  std::mutex mutex_;
  std::condition_variable condition_;
  std::thread worker_;
  bool ready_ = false, quit_ = false, running_ = false;
  memory_result result_;
  std::chrono::steady_clock::time_point last_;
};
}  // namespace sirius::test
