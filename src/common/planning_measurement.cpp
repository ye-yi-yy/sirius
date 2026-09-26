#include "common/planning_measurement.hpp"

#include <atomic>

namespace sirius::measurement {
char const* phase_name(phase value) noexcept
{
  constexpr char const* names[] = {"optimizer_hook",
                                   "capture_logical",
                                   "finalize",
                                   "capture_physical",
                                   "candidate_build",
                                   "comparison",
                                   "publish",
                                   "execute_rebuild",
                                   "lifecycle_lock_wait",
                                   "native_walk"};
  return names[static_cast<unsigned>(value)];
}
#ifdef SIRIUS_ENABLE_PLANNING_MEASUREMENTS
namespace {
std::atomic<bool> io_enabled{false};
std::atomic<uint64_t> io_opens{0}, io_reads{0}, io_bytes{0}, io_metadata{0};
}  // namespace
void begin_datasource_io() noexcept
{
  io_opens = io_reads = io_bytes = io_metadata = 0;
  io_enabled.store(true);
}
datasource_io_result end_datasource_io() noexcept
{
  io_enabled.store(false);
  return {io_opens.load(), io_reads.load(), io_bytes.load(), io_metadata.load()};
}
void record_datasource_io(io_request request, uint64_t bytes) noexcept
{
  if (!io_enabled.load()) return;
  switch (request) {
    case io_request::open: ++io_opens; break;
    case io_request::read:
      ++io_reads;
      io_bytes += bytes;
      break;
    case io_request::metadata: ++io_metadata; break;
  }
}
thread_local observation* active_observation = nullptr;
observation_scope::observation_scope(observation& value) noexcept : previous_(active_observation)
{
  active_observation = &value;
}
observation_scope::~observation_scope() { active_observation = previous_; }
phase_scope::phase_scope(phase value) noexcept : observer_(active_observation), value_(value)
{
  if (!observer_) return;
  if (observer_->boundary) observer_->boundary(observer_->payload, value_, true);
  begin_ = std::chrono::steady_clock::now();
}
void phase_scope::finish() noexcept
{
  if (!observer_) return;
  auto const end = std::chrono::steady_clock::now();
  auto& result   = observer_->phases[static_cast<unsigned>(value_)];
  result.nanoseconds += std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin_).count();
  ++result.calls;
  if (observer_->boundary) observer_->boundary(observer_->payload, value_, false);
  observer_ = nullptr;
}
#endif
}  // namespace sirius::measurement
