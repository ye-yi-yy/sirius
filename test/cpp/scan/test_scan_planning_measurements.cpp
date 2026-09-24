#include <catch.hpp>
#include <duckdb.hpp>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <op/scan/duckdb_native_gpu_ingestible.hpp>
#include <op/scan/table_scan/bound_read_view.hpp>
#include <sirius_context.hpp>
#include <unistd.h>
#include <utils/gpu_execution_fixture.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace {
using clock_type = std::chrono::steady_clock;
std::string env(std::string const& name, std::string fallback = {})
{
  auto const* value = std::getenv(name.c_str());
  return value ? value : std::move(fallback);
}

long rss_kib()
{
  std::ifstream input("/proc/self/status");
  std::string key;
  while (input >> key) {
    if (key == "VmRSS:") {
      long value = 0;
      input >> value;
      return value;
    }
    std::string rest;
    std::getline(input, rest);
  }
  return 0;
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
}  // namespace

TEST_CASE_METHOD(sirius::test::GpuExecutionFixture,
                 "Scan planning latency and memory usage",
                 "[.][scan_planning_measurements][integration][gpu_execution]")
{
  auto scenario          = env("SCAN_PLANNING_SCENARIO", "small");
  auto const attempts    = std::max(1, std::stoi(env("SCAN_PLANNING_ATTEMPTS", "25")));
  auto const trace       = env("SCAN_PLANNING_TRACE") == "1";
  auto const memory      = env("SCAN_PLANNING_MEMORY") == "1";
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
    }
    require_ok(con->Query("ROLLBACK"), "end native measurement snapshot");
    std::cout << "SCAN_PLANNING_NATIVE attempts=" << attempts
              << " lock_median_us=" << percentile(lock_us, 0.5)
              << " lock_p95_us=" << percentile(lock_us, 0.95)
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
    auto const steady_bound = metric.canonical_capacity + 2 * metric.evidence_capacity + 256 * 1024;
    auto const fresh_model  = 3 * metric.canonical_capacity + 2 * metric.evidence_capacity +
                             metric.transient_path_capacity + metric.sort_index_capacity;
    auto const rebind_model = 4 * metric.canonical_capacity + 4 * metric.evidence_capacity +
                              metric.transient_path_capacity + metric.sort_index_capacity;
    auto const steady_model = metric.canonical_capacity + 2 * metric.evidence_capacity;
    // These formulas estimate capacity; they do not observe overlapping live allocations.
    // Report them for investigation without treating the inequality as peak-memory acceptance.
    if (scenario == "glob") {
      CHECK(metric.file_count ==
            static_cast<std::size_t>(std::stoull(env("SCAN_PLANNING_EXPECTED_FILES", "10000"))));
    }
    std::cout << "SCAN_PLANNING_CAPACITY_MODEL actual_peak_verified=false scenario=" << scenario
              << " scans=" << views.size() << " F=" << metric.file_count
              << " L=" << metric.canonical_capacity << " E=" << metric.evidence_capacity
              << " C=" << metric.transient_path_capacity << " I=" << metric.sort_index_capacity
              << " fresh_model=" << fresh_model << " fresh_bound=" << fresh_bound
              << " rebind_model=" << rebind_model << " rebind_bound=" << rebind_bound
              << " steady_model=" << steady_model << " steady_bound=" << steady_bound << std::endl;
    require_ok(con->Query("ROLLBACK"), "end metrics capture");
  }

  std::vector<double> prepare_us;
  std::vector<double> execute_us;
  // Compare each sampled peak with its own attempt's baseline before taking the maximum.
  // Process RSS samples do not establish peak added live allocations.
  long process_peak_kib   = rss_kib();
  long prepare_growth_kib = 0;
  long execute_growth_kib = 0;
  long retained_kib       = 0;

  for (int attempt = 0; attempt < attempts; ++attempt) {
    require_ok(con->Query("SET sirius_test_inject_pin_registry_change=true"), "force rebuild");
    require_ok(
      con->Query("SET sirius_test_inject_transparent_gpu_error='scan planning measurement'"),
      "stop before scan");
    std::atomic<bool> sampling{memory};
    auto const prepare_base_kib = rss_kib();
    long sampled_peak           = prepare_base_kib;
    std::thread sampler;
    if (memory) {
      sampler = std::thread([&] {
        while (sampling.load(std::memory_order_relaxed)) {
          sampled_peak = std::max(sampled_peak, rss_kib());
          std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
      });
    }

    if (trace) marker("BEGIN prepare " + scenario + " " + std::to_string(attempt));
    auto begin    = clock_type::now();
    auto prepared = con->Prepare(query);
    auto end      = clock_type::now();
    if (trace) marker("END prepare " + scenario + " " + std::to_string(attempt));
    sampling.store(false, std::memory_order_relaxed);
    if (sampler.joinable()) sampler.join();
    auto const prepare_end_kib    = rss_kib();
    auto const prepare_peak_kib   = std::max(sampled_peak, prepare_end_kib);
    auto const prepare_added_kib  = std::max(0L, prepare_peak_kib - prepare_base_kib);
    auto const retained_added_kib = std::max(0L, prepare_end_kib - prepare_base_kib);
    prepare_growth_kib            = std::max(prepare_growth_kib, prepare_added_kib);
    retained_kib                  = std::max(retained_kib, retained_added_kib);
    REQUIRE(prepared);
    auto const prepare_error = prepared->HasError() ? prepared->GetError() : std::string{};
    INFO(prepare_error);
    REQUIRE_FALSE(prepared->HasError());
    prepare_us.push_back(std::chrono::duration<double, std::micro>(end - begin).count());

    auto const add_path = env("SCAN_PLANNING_ADD_PATH");
    if (attempt == 0 && !add_path.empty()) {
      std::filesystem::create_hard_link(env("SCAN_PLANNING_SOURCE"), add_path);
    }

    sampling.store(memory, std::memory_order_relaxed);
    auto const execute_base_kib = rss_kib();
    sampled_peak                = execute_base_kib;
    if (memory) {
      sampler = std::thread([&] {
        while (sampling.load(std::memory_order_relaxed)) {
          sampled_peak = std::max(sampled_peak, rss_kib());
          std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
      });
    }
    if (trace) marker("BEGIN execute " + scenario + " " + std::to_string(attempt));
    begin       = clock_type::now();
    auto result = prepared->Execute();
    end         = clock_type::now();
    if (trace) marker("END execute " + scenario + " " + std::to_string(attempt));
    sampling.store(false, std::memory_order_relaxed);
    if (sampler.joinable()) sampler.join();
    auto const execute_end_kib   = rss_kib();
    auto const execute_peak_kib  = std::max(sampled_peak, execute_end_kib);
    auto const execute_added_kib = std::max(0L, execute_peak_kib - execute_base_kib);
    execute_growth_kib           = std::max(execute_growth_kib, execute_added_kib);
    REQUIRE(result);
    auto const execution_error =
      result->HasError() ? result->GetError() : std::string{"execution unexpectedly succeeded"};
    INFO(execution_error);
    REQUIRE(result->HasError());
    REQUIRE(result->GetError().find(
              "injected transparent GPU failure: scan planning measurement") != std::string::npos);
    execute_us.push_back(std::chrono::duration<double, std::micro>(end - begin).count());
    process_peak_kib = std::max({process_peak_kib, prepare_peak_kib, execute_peak_kib});
    if (memory) {
      std::cout << "SCAN_PLANNING_MEMORY scenario=" << scenario << " scans=" << scan_count
                << " attempt=" << attempt << " sampling_sleep_us=100"
                << " prepare_base_kib=" << prepare_base_kib
                << " prepare_peak_kib=" << prepare_peak_kib
                << " prepare_end_kib=" << prepare_end_kib
                << " prepare_growth_kib=" << prepare_added_kib
                << " retained_growth_kib=" << retained_added_kib
                << " execute_base_kib=" << execute_base_kib
                << " execute_peak_kib=" << execute_peak_kib
                << " execute_end_kib=" << execute_end_kib
                << " execute_growth_kib=" << execute_added_kib << std::endl;
    }
    require_ok(con->Query("SET sirius_test_inject_pin_registry_change=false"), "clear rebuild");
    require_ok(con->Query("SET sirius_test_inject_transparent_gpu_error=''"),
               "clear injected error");
  }

  std::cout << "SCAN_PLANNING_RESULT memory_sampled=" << memory << " scenario=" << scenario
            << " scans=" << scan_count << " attempts=" << attempts
            << " prepare_median_us=" << percentile(prepare_us, 0.5)
            << " prepare_p95_us=" << percentile(prepare_us, 0.95)
            << " execute_median_us=" << percentile(execute_us, 0.5)
            << " execute_p95_us=" << percentile(execute_us, 0.95)
            << " rss_prepare_growth_kib=" << prepare_growth_kib
            << " rss_execute_growth_kib=" << execute_growth_kib
            << " rss_retained_growth_kib=" << retained_kib
            << " rss_process_peak_kib=" << process_peak_kib << std::endl;
}
