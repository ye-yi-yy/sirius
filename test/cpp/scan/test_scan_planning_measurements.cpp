#include <catch.hpp>
#include <common/planning_measurement.hpp>
#include <duckdb.hpp>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/main/prepared_statement_data.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <op/scan/duckdb_native_gpu_ingestible.hpp>
#include <op/scan/table_scan/bound_read_view.hpp>
#include <sirius_context.hpp>
#include <unistd.h>
#include <utils/gpu_execution_fixture.hpp>
#include <utils/planning_file_system.hpp>
#include <utils/planning_memory_sampler.hpp>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

namespace {
using clock_type = std::chrono::steady_clock;
std::string env(std::string const& name, std::string fallback = {})
{
  auto const* value = std::getenv(name.c_str());
  return value ? value : std::move(fallback);
}

void marker(std::string const& value)
{
  auto line = "SCAN_PLANNING_MARK " + value + "\n";
  (void)::write(STDERR_FILENO, line.data(), line.size());
}

double percentile(std::vector<double> values, double fraction)
{
  std::sort(values.begin(), values.end());
  auto index = static_cast<std::size_t>(std::ceil(values.size() * fraction));
  index      = std::clamp<std::size_t>(index, 1, values.size()) - 1;
  return values[index];
}

void require_ok(duckdb::unique_ptr<duckdb::QueryResult> const& result, std::string const& label)
{
  REQUIRE(result);
  INFO(label << ": " << (result->HasError() ? result->GetError() : ""));
  REQUIRE_FALSE(result->HasError());
}

duckdb::LogicalGet& first_get(duckdb::LogicalOperator& node)
{
  if (node.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
    return node.Cast<duckdb::LogicalGet>();
  }
  for (auto& child : node.children) {
    try {
      return first_get(*child);
    } catch (std::out_of_range const&) {
    }
  }
  throw std::out_of_range("no LogicalGet");
}

duckdb::DataTable& table_storage(duckdb::Connection& connection, std::string const& table)
{
  auto& context = *connection.context;
  auto& catalog = duckdb::Catalog::GetCatalog(context, "");
  duckdb::CatalogTransaction transaction(catalog, context);
  auto& schema = catalog.GetSchema(transaction, "main");
  auto entry   = schema.GetEntry(transaction, duckdb::CatalogType::TABLE_ENTRY, table);
  REQUIRE(entry);
  return entry->Cast<duckdb::DuckTableEntry>().GetStorage();
}

std::unique_ptr<sirius::op::scan::duckdb_native_ingestible_table_info> native_info(
  duckdb::Connection& connection, duckdb::DataTable& storage)
{
  using namespace sirius::op::scan;
  auto info        = std::make_unique<duckdb_native_ingestible_table_info>();
  info->storage    = &storage;
  info->context    = connection.context.get();
  info->db_path    = storage.GetAttached().GetStorageManager().GetDBPath();
  info->table_name = "scan_bench_native";
  projected_column column;
  column.storage_idx = duckdb::StorageIndex(0);
  info->projected_cols.push_back(column);
  info->column_ids.push_back(duckdb::ColumnIndex(0));
  info->names.push_back("i");
  auto type = sirius::logical_type::make(sirius::type_id::BIGINT);
  info->projected_types.push_back(type);
  info->returned_types.push_back(type);
  info->output_types.push_back(type);
  return info;
}

class measurement_runtime_guard {
 public:
  measurement_runtime_guard()
  {
    auto const* value = std::getenv("SIRIUS_DISABLE");
    if (value) previous_ = value;
    unsetenv("SIRIUS_DISABLE");
  }
  ~measurement_runtime_guard()
  {
    if (previous_)
      setenv("SIRIUS_DISABLE", previous_->c_str(), 1);
    else
      unsetenv("SIRIUS_DISABLE");
  }

 private:
  std::optional<std::string> previous_;
};

class scan_planning_fixture : private measurement_runtime_guard,
                              public sirius::test::GpuExecutionFixture {
 public:
  scan_planning_fixture()
    : GpuExecutionFixture(env("SCAN_PLANNING_MODE", "latency") == "io"
                            ? duckdb::make_uniq<duckdb::VirtualFileSystem>(
                                duckdb::make_uniq<sirius::test::planning_file_system>())
                            : nullptr)
  {
  }
};

struct added_inventory_file {
  std::filesystem::path path;
  bool created = false;
  ~added_inventory_file()
  {
    if (created) {
      std::error_code error;
      std::filesystem::remove(path, error);
    }
  }
  void create(std::string const& source, std::string const& destination)
  {
    path = destination;
    std::filesystem::create_hard_link(source, path);
    created = true;
  }
};

struct measurement_boundaries {
  sirius::test::planning_memory_sampler* sampler  = nullptr;
  sirius::test::planning_file_system* file_system = nullptr;
  bool rebuild_only                               = false;
  bool started = false, completed = false;
  sirius::test::memory_result result;
  sirius::test::planning_io_result io;
  sirius::measurement::datasource_io_result datasource_io;
  static void observe(void* payload, sirius::measurement::phase value, bool entering) noexcept
  {
    auto& self = *static_cast<measurement_boundaries*>(payload);
    if (self.rebuild_only && value == sirius::measurement::phase::execute_rebuild) {
      if (entering) {
        if (self.sampler) self.sampler->start();
        if (self.file_system) {
          self.file_system->start();
          sirius::measurement::begin_datasource_io();
        }
        self.started = true;
      } else {
        if (self.file_system) {
          self.datasource_io = sirius::measurement::end_datasource_io();
          self.io            = self.file_system->stop();
        }
        if (self.sampler) self.result = self.sampler->stop();
        self.completed = true;
      }
    } else if (self.sampler) {
      self.sampler->record_peak();
    }
  }
};
}  // namespace

TEST_CASE_METHOD(scan_planning_fixture,
                 "Scan planning latency and memory usage",
                 "[.][scan_planning_measurements][isolated_context][gpu_execution]")
{
  auto scenario               = env("SCAN_PLANNING_SCENARIO", "small");
  auto const attempts         = std::max(1, std::stoi(env("SCAN_PLANNING_ATTEMPTS", "25")));
  auto const trace            = env("SCAN_PLANNING_TRACE") == "1";
  auto const measurement_mode = env("SCAN_PLANNING_MODE", "latency");
  auto const operation        = env("SCAN_PLANNING_OPERATION", "fresh");
  auto const memory           = measurement_mode == "memory";
  REQUIRE((measurement_mode == "latency" || memory || measurement_mode == "io"));
  REQUIRE((operation == "fresh" || operation == "rebind" || operation == "rebuild" ||
           operation == "sharing" || operation == "mismatch" || operation == "exception"));
#ifndef SIRIUS_ENABLE_PLANNING_MEASUREMENTS
  FAIL("Configure SIRIUS_ENABLE_PLANNING_MEASUREMENTS=ON for this hidden benchmark");
#endif
  std::size_t scan_count = 1;
  std::string query;

  if (scenario == "small") {
    auto const requested_scans = std::stoi(env("SCAN_PLANNING_SCANS", "4"));
    REQUIRE(requested_scans > 0);
    scan_count = static_cast<std::size_t>(requested_scans);
    std::vector<std::string> tables;
    for (std::size_t i = 0; i < scan_count; ++i) {
      tables.push_back("scan_bench_" + std::to_string(i));
      require_ok(con->Query("CREATE OR REPLACE TABLE " + tables.back() +
                            " AS SELECT range AS id FROM range(16)"),
                 "create small table");
    }
    require_ok(con->Query("CHECKPOINT"), "checkpoint small tables");
    for (auto const& table : tables) {
      require_ok(con->Query("CALL pin_table(format='duckdb', name='" + table + "', tier='gpu')"),
                 "pin small table");
    }
    query = "SELECT sum(";
    for (std::size_t i = 0; i < scan_count; ++i) {
      if (i) query += "+";
      query += "s" + std::to_string(i) + ".id";
    }
    query += ") FROM " + tables[0] + " s0";
    for (std::size_t i = 1; i < scan_count; ++i) {
      query += " JOIN " + tables[i] + " s" + std::to_string(i) + " USING(id)";
    }
  } else if (scenario == "glob") {
    auto pattern = env("SCAN_PLANNING_GLOB");
    REQUIRE_FALSE(pattern.empty());
    query = "SELECT sum(id) FROM read_parquet('" + pattern + "')";
  } else if (scenario == "iceberg") {
    require_ok(con->Query("LOAD iceberg"), "load iceberg");
    require_ok(con->Query("SET unsafe_enable_version_guessing=true"), "iceberg version guessing");
    auto path = env("SCAN_PLANNING_ICEBERG");
    REQUIRE_FALSE(path.empty());
    query =
      "SELECT sum(count) FROM iceberg_scan('" + path + "', snapshot_from_id=9400000000000002)";
  } else if (scenario == "native") {
    require_ok(con->Query("CREATE OR REPLACE TABLE scan_bench_native AS "
                          "SELECT range::BIGINT AS i FROM range(300000)"),
               "create native table");
    require_ok(con->Query("CHECKPOINT"), "checkpoint native table");
  } else {
    FAIL("unknown SCAN_PLANNING_SCENARIO: " << scenario);
  }

  require_ok(con->Query("SET threads=1"), "keep planning observations on the calling thread");
  require_ok(con->Query("SET gpu_execution=true"), "enable GPU execution");
  require_ok(con->Query("SET enable_duckdb_fallback=false"), "disable fallback");
  require_ok(con->Query("SET sirius_test_inject_pin_registry_change=false"), "clear epoch hook");
  require_ok(con->Query("SET sirius_test_inject_transparent_gpu_error=''"), "clear error hook");
  require_ok(con->Query("SET sirius_test_inject_read_view_mismatch='off'"), "clear view hook");
  require_ok(con->Query("SET sirius_test_pause_native_after_prepare_ms=0"),
             "clear pre-window pause");
  require_ok(con->Query("SET sirius_test_pause_native_decode_ms=0"), "clear native pause");
  require_ok(con->Query("SET sirius_test_inject_native_walk_failure=''"), "clear walk hook");
  require_ok(con->Query("SET sirius_test_inject_native_decode_failure=''"), "clear decode hook");
  require_ok(con->Query("SET sirius_test_mark_runtime_unavailable_before_window=false"),
             "clear unavailable hook");

  if (scenario == "native") {
    REQUIRE(measurement_mode == "latency");
    require_ok(con->Query("BEGIN TRANSACTION READ ONLY"), "begin native measurement snapshot");
    auto& storage = table_storage(*con, "scan_bench_native");
    auto context  = con->context->registered_state->Get<duckdb::SiriusContext>("sirius_state");
    REQUIRE(context);
    std::vector<double> lock_us;
    std::vector<double> walk_us;
    for (int attempt = 0; attempt < attempts; ++attempt) {
      sirius::op::scan::duckdb_native_gpu_ingestible ingestible(native_info(*con, storage));
      duckdb::SiriusContext::StandaloneQueryScope window(
        *context, *con->context, "scan_bench_native_preparation");
      auto begin = clock_type::now();
      context->get_scan_manager().acquire_checkpoint_key(storage.GetAttached());
      auto locked = clock_type::now();
      ingestible.ensure_metadata_prepared();
      auto walked = clock_type::now();
      window.finish();
      lock_us.push_back(std::chrono::duration<double, std::micro>(locked - begin).count());
      walk_us.push_back(std::chrono::duration<double, std::micro>(walked - locked).count());
      std::cout << "{\"record\":\"native_preparation\",\"attempt\":" << attempt
                << ",\"checkpoint_acquire_available\":true,\"checkpoint_acquire_us\":"
                << lock_us.back() << ",\"native_walk_us\":" << walk_us.back() << "}\n";
    }
    require_ok(con->Query("ROLLBACK"), "end native measurement snapshot");
    std::cout << "SCAN_PLANNING_NATIVE attempts=" << attempts
              << " checkpoint_acquire_median_us=" << percentile(lock_us, 0.5)
              << " checkpoint_acquire_p95_us=" << percentile(lock_us, 0.95)
              << " walk_median_us=" << percentile(walk_us, 0.5)
              << " walk_p95_us=" << percentile(walk_us, 0.95) << std::endl;
    return;
  }

  if (env("SCAN_PLANNING_CAPTURE_ONLY") == "1") {
    require_ok(con->Query("BEGIN"), "begin capture benchmark");
    auto plan = con->ExtractPlan(query);
    REQUIRE(plan);
    auto& get = first_get(*plan);
    std::vector<double> capture_us;
    for (int attempt = 0; attempt < attempts; ++attempt) {
      auto begin = clock_type::now();
      auto view  = sirius::op::scan::capture_bound_read_view(get, *con->context);
      auto end   = clock_type::now();
      REQUIRE(view.identity);
      capture_us.push_back(std::chrono::duration<double, std::micro>(end - begin).count());
    }
    std::cout << "SCAN_PLANNING_CAPTURE scenario=" << scenario << " attempts=" << attempts
              << " median_us=" << percentile(capture_us, 0.5)
              << " p95_us=" << percentile(capture_us, 0.95) << std::endl;
    require_ok(con->Query("ROLLBACK"), "end capture benchmark");
    return;
  }

  if (env("SCAN_PLANNING_METRICS") == "1") {
    require_ok(con->Query("BEGIN"), "begin metrics capture");
    auto plan = con->ExtractPlan(query);
    REQUIRE(plan);
    auto const views = sirius::op::scan::capture_bound_read_views(*plan, *con->context);
    REQUIRE(views.size() == scan_count);
    std::size_t view_index = 0;
    for (auto const& captured : views) {
      auto const& capacity = captured.view.metrics;
      std::cout << "SCAN_PLANNING_CAPTURE_SIDE source=separate_logical_capture scan="
                << view_index++ << " F=" << capacity.file_count
                << " L=" << capacity.canonical_capacity << " E=" << capacity.evidence_capacity
                << " C=" << capacity.transient_path_capacity
                << " I=" << capacity.sort_index_capacity << std::endl;
    }
    sirius::op::scan::read_view_capture_metrics metric;
    for (auto const& captured : views) {
      metric.file_count += captured.view.metrics.file_count;
      metric.canonical_capacity += captured.view.metrics.canonical_capacity;
      metric.evidence_capacity += captured.view.metrics.evidence_capacity;
      metric.transient_path_capacity += captured.view.metrics.transient_path_capacity;
      metric.sort_index_capacity += captured.view.metrics.sort_index_capacity;
    }
    auto const fresh_bound = 3 * metric.canonical_capacity + 2 * metric.evidence_capacity +
                             metric.transient_path_capacity + metric.sort_index_capacity +
                             256 * 1024;
    auto const rebind_bound = 5 * (metric.canonical_capacity + metric.evidence_capacity) +
                              metric.transient_path_capacity + metric.sort_index_capacity +
                              256 * 1024;
    auto const steady_bound =
      metric.canonical_capacity + 2 * metric.evidence_capacity + views.size() * 256 * 1024;
    // Capacity limits are reported separately from allocator-observed samples.
    if (scenario == "glob") {
      CHECK(metric.file_count ==
            static_cast<std::size_t>(std::stoull(env("SCAN_PLANNING_EXPECTED_FILES", "10000"))));
    }
    std::cout << "SCAN_PLANNING_CAPTURE_METRICS actual_peak_verified=false scenario=" << scenario
              << " scans=" << views.size() << " F=" << metric.file_count
              << " L=" << metric.canonical_capacity << " E=" << metric.evidence_capacity
              << " C=" << metric.transient_path_capacity << " I=" << metric.sort_index_capacity
              << " fresh_bound=" << fresh_bound << " rebind_bound=" << rebind_bound
              << " steady_bound=" << steady_bound << std::endl;
    require_ok(con->Query("ROLLBACK"), "end metrics capture");
    return;  // Collect capacities separately; do not warm a subsequent timing window.
  }

  sirius::test::planning_file_system* file_system = nullptr;
  if (measurement_mode == "io") {
    auto& fs  = *duckdb::DBConfig::GetConfig(*con->context).file_system;
    auto* vfs = dynamic_cast<duckdb::VirtualFileSystem*>(&fs);
    REQUIRE(vfs);
    file_system = dynamic_cast<sirius::test::planning_file_system*>(&vfs->GetDefaultFileSystem());
    REQUIRE(file_system);
  }
  std::unique_ptr<sirius::test::process_allocator_reader> allocator;
  std::unique_ptr<sirius::test::planning_memory_sampler> sampler;
  auto const interval_us = std::stoll(env("SCAN_PLANNING_SAMPLE_US", "100"));
  REQUIRE(interval_us > 0);
  auto allocator_name = env("SCAN_PLANNING_ALLOCATOR", "glibc");
  if (memory) {
    allocator = std::make_unique<sirius::test::process_allocator_reader>(allocator_name);
    sampler   = std::make_unique<sirius::test::planning_memory_sampler>(
      *allocator, std::chrono::microseconds(interval_us));
  }
  std::vector<double> api_us;
  api_us.reserve(attempts);
  for (int attempt = 0; attempt < attempts; ++attempt) {
    bool const execute = operation == "rebuild" || operation == "exception";
    bool const rebind  = operation == "rebind";
    require_ok(con->Query(std::string("SET sirius_test_inject_pin_registry_change=") +
                          (operation == "rebuild" ? "true" : "false")),
               "configure rebuild");
    require_ok(con->Query(std::string("SET sirius_test_inject_read_view_mismatch='") +
                          (operation == "mismatch" ? "finalize" : "off") + "'"),
               "configure mismatch");
    require_ok(con->Query(std::string("SET sirius_test_inject_transparent_gpu_error='") +
                          (execute ? "scan planning measurement" : "") + "'"),
               "configure cleanup");

    added_inventory_file added_file;
    duckdb::unique_ptr<duckdb::PreparedStatement> prepared;
    duckdb::shared_ptr<duckdb::PreparedStatementData> old_generation;
    duckdb::unique_ptr<duckdb::PendingQueryResult> pending;
    duckdb::unique_ptr<duckdb::QueryResult> result;
    // Preserve the fresh origin as well as the immediate window baseline: subtracting
    // the latter during rebind would exclude the already-live old generation.
    auto const origin = allocator ? allocator->read() : sirius::test::allocator_reading{};
    if (execute || rebind) {
      prepared = con->Prepare(query);
      REQUIRE(prepared);
      INFO((prepared->HasError() ? prepared->GetError() : ""));
      REQUIRE_FALSE(prepared->HasError());
      if (rebind) {
        if (scenario == "glob") {
          auto const source      = env("SCAN_PLANNING_SOURCE");
          auto const destination = env("SCAN_PLANNING_ADD_PATH");
          REQUIRE_FALSE(source.empty());
          REQUIRE_FALSE(destination.empty());
          added_file.create(source, destination);
        }
        prepared->data->properties.always_require_rebind = true;
        old_generation                                   = prepared->data;
      } else {
        // DuckDB rebind and executor initialization finish outside the rebuild window.
        pending = prepared->PendingQuery();
        REQUIRE(pending);
        REQUIRE_FALSE(pending->HasError());
      }
    }
    auto const before_stats = sirius::test::get_transparent_execution_stats(*con);
    sirius::measurement::observation observation;
    measurement_boundaries boundaries{sampler.get(), file_system, operation == "rebuild"};
    observation.boundary = (memory || file_system) ? &measurement_boundaries::observe : nullptr;
    observation.payload  = &boundaries;
    sirius::test::memory_result memory_result;
    if (trace) marker("BEGIN " + operation + " " + scenario + " " + std::to_string(attempt));
    if (sampler && !boundaries.rebuild_only) sampler->start();
    if (file_system && !boundaries.rebuild_only) {
      file_system->start();
      sirius::measurement::begin_datasource_io();
    }
    auto begin = clock_type::now();
    {
      sirius::measurement::observation_scope observing(observation);
      if (execute)
        result = pending->Execute();
      else if (rebind)
        pending = prepared->PendingQuery();
      else if (scenario == "iceberg" && operation != "mismatch")
        pending = con->PendingQuery(query);
      else
        prepared = con->Prepare(query);
    }
    auto end           = clock_type::now();
    auto datasource_io = boundaries.datasource_io;
    auto io_result     = boundaries.io;
    if (file_system && !boundaries.rebuild_only) {
      datasource_io = sirius::measurement::end_datasource_io();
      io_result     = file_system->stop();
    }
    if ((sampler || file_system) && boundaries.rebuild_only) {
      REQUIRE(boundaries.started);
      REQUIRE(boundaries.completed);
    }
    if (sampler) memory_result = boundaries.rebuild_only ? boundaries.result : sampler->stop();
    if (trace) marker("END " + operation + " " + scenario + " " + std::to_string(attempt));
    auto const after_stats = sirius::test::get_transparent_execution_stats(*con);
    if (execute) {
      REQUIRE(result);
      REQUIRE(result->HasError());
      REQUIRE(
        result->GetError().find("injected transparent GPU failure: scan planning measurement") !=
        std::string::npos);
      REQUIRE(after_stats.successful_rebinds == before_stats.successful_rebinds);
      REQUIRE(after_stats.execution_rebuilds - before_stats.execution_rebuilds ==
              (operation == "rebuild" ? 1 : 0));
    } else if (rebind) {
      REQUIRE(pending);
      INFO((pending->HasError() ? pending->GetError() : ""));
      REQUIRE_FALSE(pending->HasError());
      REQUIRE(after_stats.successful_rebinds == before_stats.successful_rebinds + 1);
      REQUIRE(after_stats.executions == before_stats.executions);
      REQUIRE(old_generation);
      REQUIRE(old_generation == prepared->data);
    } else if (scenario == "iceberg" && operation != "mismatch") {
      REQUIRE(pending);
      INFO((pending->HasError() ? pending->GetError() : ""));
      REQUIRE_FALSE(pending->HasError());
      REQUIRE(after_stats.successful_rebinds == before_stats.successful_rebinds + 1);
      REQUIRE(
        observation.phases[static_cast<unsigned>(sirius::measurement::phase::publish)].calls == 1);
    } else {
      REQUIRE(prepared);
      if (operation == "mismatch") {
        REQUIRE(prepared->HasError());
        REQUIRE(after_stats.read_view_mismatches == before_stats.read_view_mismatches + 1);
      } else {
        INFO((prepared->HasError() ? prepared->GetError() : ""));
        REQUIRE_FALSE(prepared->HasError());
        REQUIRE(after_stats.successful_rebinds == before_stats.successful_rebinds + 1);
        REQUIRE(
          observation.phases[static_cast<unsigned>(sirius::measurement::phase::publish)].calls ==
          1);
      }
    }
    auto elapsed = std::chrono::duration<double, std::micro>(end - begin).count();
    api_us.push_back(elapsed);
    std::cout << "{\"record\":\"planning_attempt\",\"mode\":\"" << measurement_mode
              << "\",\"operation\":\"" << operation << "\",\"scenario\":\"" << scenario
              << "\",\"scans\":" << scan_count << ",\"attempt\":" << attempt
              << ",\"timing_valid\":" << (measurement_mode == "latency" ? "true" : "false")
              << ",\"api_total_us\":" << elapsed << ",\"phases\":{";
    for (unsigned i = 0; i < sirius::measurement::phase_count; ++i) {
      if (i) std::cout << ',';
      std::cout << '"'
                << sirius::measurement::phase_name(static_cast<sirius::measurement::phase>(i))
                << "\":{\"calls\":" << observation.phases[i].calls
                << ",\"inclusive_us\":" << observation.phases[i].nanoseconds / 1000.0 << '}';
    }
    std::cout << "}}\n";
    if (sampler) {
      REQUIRE(memory_result.valid);
      REQUIRE(origin.valid);
      if (boundaries.rebuild_only) {
        REQUIRE(boundaries.started);
        REQUIRE(boundaries.completed);
      }
      std::cout << "{\"record\":\"planning_memory\",\"operation\":\"" << operation
                << "\",\"attempt\":" << attempt << ",\"backend\":\"" << allocator_name
                << "\",\"duckdb_jemalloc_observed\":"
                << (allocator->has_duckdb_statistics() ? "true" : "false")
                << ",\"peak_kind\":\"sampled_allocator_observed\",\"exact_peak_verified\":false"
                << ",\"live_before_bytes\":" << memory_result.before.total()
                << ",\"peak_live_bytes\":" << memory_result.peak.total()
                << ",\"live_after_bytes\":" << memory_result.after.total()
                << ",\"peak_added_bytes\":" << memory_result.peak_added_bytes()
                << ",\"retained_delta_bytes\":" << memory_result.retained_delta_bytes()
                << ",\"fresh_origin_bytes\":" << origin.total()
                << ",\"peak_from_fresh_origin_bytes\":"
                << static_cast<int64_t>(memory_result.peak.total()) -
                     static_cast<int64_t>(origin.total())
                << ",\"retained_from_fresh_origin_bytes\":"
                << static_cast<int64_t>(memory_result.after.total()) -
                     static_cast<int64_t>(origin.total())
                << ",\"system_at_peak_bytes\":" << memory_result.peak.system_bytes
                << ",\"duckdb_at_peak_bytes\":" << memory_result.peak.duckdb_bytes
                << (allocator_name == "glibc" ? ",\"system_heap_at_peak_bytes\":"
                                              : ",\"system_active_at_peak_bytes\":")
                << memory_result.peak.system_active_bytes
                << ",\"duckdb_active_at_peak_bytes\":" << memory_result.peak.duckdb_active_bytes
                << ",\"sampling_sleep_us\":" << interval_us
                << ",\"samples\":" << memory_result.samples
                << ",\"maximum_sample_gap_us\":" << memory_result.maximum_gap_us << "}\n";
    }
    if (file_system) {
      std::cout << "{\"record\":\"planning_io\",\"operation\":\"" << operation
                << "\",\"attempt\":" << attempt
                << ",\"coverage\":\"duckdb_local_filesystem_api\",\"opens\":" << io_result.opens
                << ",\"read_calls\":" << io_result.read_calls
                << ",\"bytes_read\":" << io_result.bytes_read
                << ",\"metadata_requests\":" << io_result.metadata_requests
                << ",\"directory_lists\":" << io_result.directory_lists
                << ",\"files_enumerated\":" << io_result.files_enumerated
                << ",\"directory_entries\":" << io_result.directory_entries
                << ",\"sirius_datasource_opens\":" << datasource_io.opens
                << ",\"sirius_datasource_read_calls\":" << datasource_io.read_calls
                << ",\"sirius_datasource_requested_bytes\":" << datasource_io.requested_bytes
                << ",\"sirius_datasource_metadata_requests\":" << datasource_io.metadata_requests
                << "}\n";
    }
    // Retained memory above is read while PREPARE (and both generations for a
    // pending rebind) is alive. Close only after taking the endpoint reading.
    pending.reset();
    result.reset();
    prepared.reset();
    old_generation.reset();
    if (added_file.created) {
      REQUIRE(std::filesystem::remove(added_file.path));
      added_file.created = false;
    }
  }
  require_ok(con->Query("SET sirius_test_inject_pin_registry_change=false"), "clear rebuild");
  require_ok(con->Query("SET sirius_test_inject_transparent_gpu_error=''"), "clear error");
  require_ok(con->Query("SET sirius_test_inject_read_view_mismatch='off'"), "clear mismatch");
  std::cout << "SCAN_PLANNING_RESULT mode=" << measurement_mode << " operation=" << operation
            << " scenario=" << scenario << " scans=" << scan_count << " attempts=" << attempts
            << " api_median_us=" << percentile(api_us, 0.5)
            << " api_p95_us=" << percentile(api_us, 0.95) << std::endl;
}
