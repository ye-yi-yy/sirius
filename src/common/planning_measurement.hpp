#pragma once

#include <array>
#include <chrono>
#include <cstdint>

namespace sirius::measurement {
// Enabled only in a dedicated benchmark build. No timers or counters run in normal builds.
enum class phase : unsigned {
  optimizer_hook,
  capture_logical,
  finalize,
  capture_physical,
  candidate_build,
  comparison,
  publish,
  execute_rebuild,
  lifecycle_lock_wait,
  native_walk,
  count
};
constexpr auto phase_count = static_cast<unsigned>(phase::count);
char const* phase_name(phase value) noexcept;
struct phase_result {
  uint64_t nanoseconds = 0;
  uint64_t calls       = 0;
};
struct observation {
  std::array<phase_result, phase_count> phases{};
  // Used by memory and I/O fixtures. No allocation, logging or sampling in latency runs.
  void (*boundary)(void*, phase, bool) noexcept = nullptr;
  void* payload                                 = nullptr;
};
enum class io_request { open, read, metadata };
struct datasource_io_result {
  uint64_t opens = 0, read_calls = 0, requested_bytes = 0, metadata_requests = 0;
};
#ifdef SIRIUS_ENABLE_PLANNING_MEASUREMENTS
// Process-wide request counters include reads issued by worker threads. One window at a time.
void begin_datasource_io() noexcept;
datasource_io_result end_datasource_io() noexcept;
void record_datasource_io(io_request request, uint64_t bytes = 0) noexcept;
#else
inline void begin_datasource_io() noexcept {}
inline datasource_io_result end_datasource_io() noexcept { return {}; }
inline void record_datasource_io(io_request, uint64_t = 0) noexcept {}
#endif

#ifdef SIRIUS_ENABLE_PLANNING_MEASUREMENTS
extern thread_local observation* active_observation;
class observation_scope {
 public:
  explicit observation_scope(observation& value) noexcept;
  ~observation_scope();
  observation_scope(observation_scope const&) = delete;

 private:
  observation* previous_;
};
class phase_scope {
 public:
  explicit phase_scope(phase value) noexcept;
  ~phase_scope() { finish(); }
  void finish() noexcept;
  phase_scope(phase_scope const&) = delete;

 private:
  observation* observer_;
  phase value_;
  std::chrono::steady_clock::time_point begin_;
};
#else
class observation_scope {
 public:
  explicit observation_scope(observation&) noexcept {}
};
class phase_scope {
 public:
  explicit phase_scope(phase) noexcept {}
  void finish() noexcept {}
};
#endif
}  // namespace sirius::measurement
