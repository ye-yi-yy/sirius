#include <catch.hpp>
#include <common/planning_measurement.hpp>
#include <duckdb/common/multi_file/multi_file_list.hpp>
#include <unistd.h>
#include <utils/planning_file_system.hpp>
#include <utils/planning_memory_sampler.hpp>

#include <filesystem>
#include <fstream>

namespace {
class scripted_allocator : public sirius::test::allocator_reader {
 public:
  sirius::test::allocator_reading current{100, 50, 0, 0, true};
  sirius::test::allocator_reading read() noexcept override { return current; }
};
}  // namespace

TEST_CASE("Planning memory observations preserve transient peaks and signed retention",
          "[planning_observers]")
{
  scripted_allocator reader;
  sirius::test::planning_memory_sampler sampler(reader, std::chrono::microseconds(0));
  sampler.start();
  reader.current = {400, 100, 0, 0, true};
  sampler.record_peak();
  reader.current = {50, 25, 0, 0, true};
  auto result    = sampler.stop();
  REQUIRE(result.valid);
  CHECK(result.before.total() == 150);
  CHECK(result.peak.total() == 500);
  CHECK(result.after.total() == 75);
  CHECK(result.peak_added_bytes() == 350);
  CHECK(result.retained_delta_bytes() == -75);
  CHECK(result.samples == 3);
  // Window reset must not retain a prior attempt's peak.
  sampler.start();
  result = sampler.stop();
  CHECK(result.peak.total() == 75);
  reader.current.valid = false;
  sampler.start();
  CHECK_FALSE(sampler.stop().valid);
}

TEST_CASE("Planning phase observers restore nested windows after exceptions",
          "[planning_observers]")
{
#ifdef SIRIUS_ENABLE_PLANNING_MEASUREMENTS
  using namespace sirius::measurement;
  observation outer, inner;
  {
    observation_scope observing(outer);
    phase_scope hook(phase::optimizer_hook);
    try {
      observation_scope nested(inner);
      phase_scope build(phase::candidate_build);
      throw std::runtime_error("test failure");
    } catch (std::runtime_error const&) {
    }
    CHECK(active_observation == &outer);
  }
  CHECK(active_observation == nullptr);
  CHECK(outer.phases[static_cast<unsigned>(phase::optimizer_hook)].calls == 1);
  CHECK(inner.phases[static_cast<unsigned>(phase::candidate_build)].calls == 1);
#else
  sirius::measurement::observation observation;
  sirius::measurement::observation_scope observing(observation);
  sirius::measurement::phase_scope phase(sirius::measurement::phase::finalize);
  phase.finish();
  CHECK(observation.phases[static_cast<unsigned>(sirius::measurement::phase::finalize)].calls == 0);
#endif
}

TEST_CASE("Planning filesystem counts reads on existing handles without expanding globs early",
          "[planning_observers]")
{
  struct temporary_directory {
    std::filesystem::path path =
      std::filesystem::temp_directory_path() / ("planning_observer_" + std::to_string(getpid()));
    temporary_directory()
    {
      if (!std::filesystem::create_directory(path))
        throw std::runtime_error("test directory exists");
    }
    ~temporary_directory() { std::filesystem::remove_all(path); }
  } directory;
  std::filesystem::create_directory(directory.path / "nested");
  auto path = directory.path / "file.txt";
  {
    std::ofstream file(path);
    file << "abcdef";
  }
  sirius::test::planning_file_system fs;
  auto handle = fs.OpenFile(path.string(), duckdb::FileFlags::FILE_FLAGS_READ, nullptr);
  // Negative control: adding reads after opening the handle must remain visible.
  fs.start();
  char data[4]{};
  handle->Read(data, 2, 1);
  CHECK(std::string(data, 2) == "bc");
  handle->Read(data, 3);
  auto result = fs.stop();
  CHECK(result.opens == 0);
  CHECK(result.read_calls == 2);
  CHECK(result.bytes_read == 5);
  fs.start();
  auto files = fs.Glob((directory.path / "*.txt").string(),
                       duckdb::FileGlobInput(duckdb::FileGlobOptions::ALLOW_EMPTY),
                       nullptr);
  result     = fs.stop();
  CHECK(result.directory_lists == 0);
  CHECK(result.files_enumerated == 0);
  fs.start();
  auto expanded = files->GetAllFiles();
  result        = fs.stop();
  CHECK(expanded.size() == 1);
  CHECK(result.directory_lists > 0);
  CHECK(result.files_enumerated == 1);
  CHECK(result.directory_entries == 2);
  fs.start();
  CHECK(handle->GetFileSize() == 6);
  CHECK(fs.stop().metadata_requests > 0);
}

TEST_CASE("Allocator statistics backend exposes available components", "[planning_observers]")
{
#if defined(__GLIBC__)
  sirius::test::process_allocator_reader reader("glibc");
  auto reading = reader.read();
  REQUIRE(reading.valid);
  CHECK(reading.system_bytes > 0);
  if (reader.has_duckdb_statistics()) CHECK(reading.duckdb_active_bytes >= reading.duckdb_bytes);
#endif
}

TEST_CASE("Datasource request observations include worker threads", "[planning_observers]")
{
#ifdef SIRIUS_ENABLE_PLANNING_MEASUREMENTS
  using namespace sirius::measurement;
  begin_datasource_io();
  std::thread worker([] { record_datasource_io(io_request::read, 4096); });
  worker.join();
  auto result = end_datasource_io();
  CHECK(result.read_calls == 1);
  CHECK(result.requested_bytes == 4096);
  record_datasource_io(io_request::read, 1);
  CHECK(end_datasource_io().requested_bytes == 4096);
#endif
}
