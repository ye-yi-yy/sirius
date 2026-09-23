#include <catch.hpp>
#include <dlfcn.h>
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
  auto line = "T6_MARK " + value + "\n";
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

#ifndef SIRIUS_T6_BASELINE
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
  info->table_name = "t6_native";
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
#endif
}  // namespace

TEST_CASE_METHOD(sirius::test::GpuExecutionFixture,
                 "T6 indicative benchmark",
                 "[.][t6_measure][integration][gpu_execution]")
{
  auto scenario       = env("T6_SCENARIO", "small");
  auto const attempts = std::max(1, std::stoi(env("T6_ATTEMPTS", "25")));
  auto const trace    = env("T6_TRACE") == "1";
  auto const memory   = env("T6_MEMORY") == "1";
  std::string query;

  if (scenario == "small") {
    for (auto const* table : {"t6_a", "t6_b", "t6_c", "t6_d"}) {
      require_ok(con->Query("CREATE OR REPLACE TABLE " + std::string(table) +
                            " AS SELECT range AS id FROM range(16)"),
                 "create small table");
    }
    require_ok(con->Query("CHECKPOINT"), "checkpoint small tables");
    for (auto const* table : {"t6_a", "t6_b", "t6_c", "t6_d"}) {
      require_ok(con->Query("CALL pin_table(format='duckdb', name='" + std::string(table) +
                            "', tier='gpu')"),
                 "pin small table");
    }
    query =
      "SELECT sum(a.id+b.id+c.id+d.id) FROM t6_a a JOIN t6_b b USING(id) "
      "JOIN t6_c c USING(id) JOIN t6_d d USING(id)";
  } else if (scenario == "glob") {
    auto pattern = env("T6_GLOB");
    REQUIRE_FALSE(pattern.empty());
    query = "SELECT sum(id) FROM read_parquet('" + pattern + "')";
  } else if (scenario == "iceberg") {
    require_ok(con->Query("LOAD iceberg"), "load iceberg");
    require_ok(con->Query("SET unsafe_enable_version_guessing=true"), "iceberg version guessing");
    auto path = env("T6_ICEBERG");
    REQUIRE_FALSE(path.empty());
    query =
      "SELECT sum(count) FROM iceberg_scan('" + path + "', snapshot_from_id=9400000000000002)";
  } else if (scenario == "native") {
    require_ok(con->Query("CREATE OR REPLACE TABLE t6_native AS "
                          "SELECT range::BIGINT AS i FROM range(300000)"),
               "create native table");
    require_ok(con->Query("CHECKPOINT"), "checkpoint native table");
  } else {
    FAIL("unknown T6_SCENARIO: " << scenario);
  }

  require_ok(con->Query("SET gpu_execution=true"), "enable GPU execution");
  require_ok(con->Query("SET enable_duckdb_fallback=false"), "disable fallback");
  require_ok(con->Query("SET sirius_test_inject_pin_registry_change=false"), "clear epoch hook");
  require_ok(con->Query("SET sirius_test_inject_transparent_gpu_error=''"), "clear error hook");
#ifndef SIRIUS_T6_BASELINE
  require_ok(con->Query("SET sirius_test_inject_read_view_mismatch='off'"), "clear view hook");
  require_ok(con->Query("SET sirius_test_pause_native_after_prepare_ms=0"),
             "clear pre-window pause");
  require_ok(con->Query("SET sirius_test_pause_after_native_leaf_ms=0"), "clear native-leaf pause");
  require_ok(con->Query("SET sirius_test_pause_native_decode_ms=0"), "clear native pause");
  require_ok(con->Query("SET sirius_test_inject_native_walk_failure=''"), "clear walk hook");
  require_ok(con->Query("SET sirius_test_inject_native_decode_failure=''"), "clear decode hook");
  require_ok(con->Query("SET sirius_test_mark_runtime_unavailable_before_window=false"),
             "clear unavailable hook");

  if (scenario == "native") {
    require_ok(con->Query("BEGIN TRANSACTION READ ONLY"), "begin native measurement snapshot");
    auto& storage = table_storage(*con, "t6_native");
    auto context  = con->context->registered_state->Get<duckdb::SiriusContext>("sirius_state");
    REQUIRE(context);
    std::vector<double> lock_us;
    std::vector<double> walk_us;
    for (int attempt = 0; attempt < attempts; ++attempt) {
      sirius::op::scan::duckdb_native_gpu_ingestible ingestible(native_info(*con, storage));
      duckdb::SiriusContext::StandaloneQueryScope window(
        *context, *con->context, "t6_native_preparation");
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
    std::cout << "T6_NATIVE attempts=" << attempts << " lock_median_us=" << percentile(lock_us, 0.5)
              << " lock_p95_us=" << percentile(lock_us, 0.95)
              << " walk_median_us=" << percentile(walk_us, 0.5)
              << " walk_p95_us=" << percentile(walk_us, 0.95) << std::endl;
    return;
  }

  if (env("T6_CAPTURE_ONLY") == "1") {
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
    std::cout << "T6_CAPTURE scenario=" << scenario << " attempts=" << attempts
              << " median_us=" << percentile(capture_us, 0.5)
              << " p95_us=" << percentile(capture_us, 0.95) << std::endl;
    require_ok(con->Query("ROLLBACK"), "end capture benchmark");
    return;
  }

  sirius::op::scan::read_view_capture_metrics allocation_terms;
  if (env("T6_METRICS") == "1" || env("T6_ALLOCATIONS") == "1") {
    require_ok(con->Query("BEGIN"), "begin metrics capture");
    auto plan = con->ExtractPlan(query);
    REQUIRE(plan);
    auto view          = sirius::op::scan::capture_bound_read_view(first_get(*plan), *con->context);
    auto const& metric = view.metrics;
    allocation_terms   = metric;
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
            static_cast<std::size_t>(std::stoull(env("T6_EXPECTED_FILES", "10000"))));
    }
    std::cout << "T6_CAPACITY_MODEL actual_peak_verified=false scenario=" << scenario
              << " F=" << metric.file_count << " L=" << metric.canonical_capacity
              << " E=" << metric.evidence_capacity << " C=" << metric.transient_path_capacity
              << " I=" << metric.sort_index_capacity << " fresh_model=" << fresh_model
              << " fresh_bound=" << fresh_bound << " rebind_model=" << rebind_model
              << " rebind_bound=" << rebind_bound << " steady_model=" << steady_model
              << " steady_bound=" << steady_bound << std::endl;
    require_ok(con->Query("ROLLBACK"), "end metrics capture");
  }

#endif
  struct allocation_measurement {
    uint64_t live, peak, cumulative, allocations;
    uint64_t all_live, all_peak, all_cumulative;
    uint64_t overflow;
  };
  using begin_probe = int (*)();
  using end_probe   = allocation_measurement (*)();
  auto begin_allocations =
    reinterpret_cast<begin_probe>(dlsym(RTLD_DEFAULT, "sirius_t6_allocation_begin"));
  auto end_allocations =
    reinterpret_cast<end_probe>(dlsym(RTLD_DEFAULT, "sirius_t6_allocation_end"));
  bool const allocation_mode = env("T6_ALLOCATIONS") == "1";
  if (allocation_mode) {
    REQUIRE(begin_allocations);
    REQUIRE(end_allocations);
  }
  std::vector<double> finalize_us;
  std::vector<double> rebuild_us;
  long process_peak_kib = rss_kib();
  long prepare_base_kib = 0;
  long prepare_peak_kib = 0;
  long rebind_base_kib  = 0;
  long rebind_peak_kib  = 0;
  long retained_kib     = 0;

  for (int attempt = 0; attempt < attempts; ++attempt) {
    require_ok(con->Query("SET sirius_test_inject_pin_registry_change=true"), "force rebuild");
    require_ok(con->Query("SET sirius_test_inject_transparent_gpu_error='t6'"), "stop before scan");
    std::atomic<bool> sampling{memory};
    long sampled_peak = rss_kib();
    prepare_base_kib  = rss_kib();
    std::thread sampler;
    if (memory) {
      sampler = std::thread([&] {
        while (sampling.load(std::memory_order_relaxed)) {
          sampled_peak = std::max(sampled_peak, rss_kib());
          std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
      });
    }

    if (trace) marker("BEGIN finalize " + scenario + " " + std::to_string(attempt));
    if (allocation_mode) REQUIRE(begin_allocations());
    auto begin             = clock_type::now();
    auto prepared          = con->Prepare(query);
    auto end               = clock_type::now();
    auto fresh_allocations = allocation_mode ? end_allocations() : allocation_measurement{};
    if (trace) marker("END finalize " + scenario + " " + std::to_string(attempt));
    REQUIRE(prepared);
    auto const prepare_error = prepared->HasError() ? prepared->GetError() : std::string{};
    INFO(prepare_error);
    REQUIRE_FALSE(prepared->HasError());
    finalize_us.push_back(std::chrono::duration<double, std::micro>(end - begin).count());

    sampling.store(false, std::memory_order_relaxed);
    if (sampler.joinable()) sampler.join();
    prepare_peak_kib = std::max(prepare_peak_kib, sampled_peak);
    retained_kib     = std::max(retained_kib, rss_kib() - prepare_base_kib);

    auto const add_path = env("T6_ADD_PATH");
    if (attempt == 0 && !add_path.empty()) {
      std::filesystem::create_hard_link(env("T6_SOURCE"), add_path);
    }

    sampling.store(memory, std::memory_order_relaxed);
    sampled_peak    = rss_kib();
    rebind_base_kib = sampled_peak;
    if (memory) {
      sampler = std::thread([&] {
        while (sampling.load(std::memory_order_relaxed)) {
          sampled_peak = std::max(sampled_peak, rss_kib());
          std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
      });
    }
    if (trace) marker("BEGIN rebuild " + scenario + " " + std::to_string(attempt));
    if (allocation_mode) REQUIRE(begin_allocations());
    begin                   = clock_type::now();
    auto result             = prepared->Execute();
    end                     = clock_type::now();
    auto rebind_allocations = allocation_mode ? end_allocations() : allocation_measurement{};
    if (trace) marker("END rebuild " + scenario + " " + std::to_string(attempt));
    sampling.store(false, std::memory_order_relaxed);
    if (sampler.joinable()) sampler.join();
    rebind_peak_kib = std::max(rebind_peak_kib, sampled_peak);
    REQUIRE(result);
    auto const execution_error =
      result->HasError() ? result->GetError() : std::string{"execution unexpectedly succeeded"};
    INFO(execution_error);
    REQUIRE(result->HasError());
    REQUIRE(result->GetError().find("injected transparent GPU failure: t6") != std::string::npos);
    rebuild_us.push_back(std::chrono::duration<double, std::micro>(end - begin).count());
    process_peak_kib = std::max(process_peak_kib, rss_kib());
#ifndef SIRIUS_T6_BASELINE
    if (allocation_mode) {
      // Re-capture outside the observed windows to obtain actual capacities for the added-file
      // generation too. The measured peak above includes the old retained PREPARE generation.
      require_ok(con->Query("BEGIN"), "begin post-rebind capacity capture");
      auto plan = con->ExtractPlan(query);
      REQUIRE(plan);
      auto view = sirius::op::scan::capture_bound_read_view(first_get(*plan), *con->context);
      auto const& current = view.metrics;
      auto L = std::max(allocation_terms.canonical_capacity, current.canonical_capacity);
      auto E = std::max(allocation_terms.evidence_capacity, current.evidence_capacity);
      auto C = std::max(allocation_terms.transient_path_capacity, current.transient_path_capacity);
      auto I = std::max(allocation_terms.sort_index_capacity, current.sort_index_capacity);
      require_ok(con->Query("ROLLBACK"), "end post-rebind capacity capture");
      std::cout << "T6_ALLOCATIONS scenario=" << scenario << " F=" << current.file_count
                << " L=" << L << " E=" << E << " C=" << C << " I=" << I
                << " fresh_peak=" << fresh_allocations.peak
                << " retained_prepare=" << fresh_allocations.live
                << " rebind_peak=" << rebind_allocations.peak
                << " rebind_resident=" << rebind_allocations.live
                << " fresh_total_allocated=" << fresh_allocations.all_cumulative
                << " fresh_all_peak=" << fresh_allocations.all_peak
                << " rebind_total_allocated=" << rebind_allocations.all_cumulative
                << " rebind_all_peak=" << rebind_allocations.all_peak
                << " attributed_allocations=" << fresh_allocations.allocations
                << " overflow=" << fresh_allocations.overflow + rebind_allocations.overflow
                << std::endl;
      REQUIRE(fresh_allocations.overflow == 0);
      REQUIRE(rebind_allocations.overflow == 0);
      // A missing interposer or missing exported attribution frames cannot silently pass.
      REQUIRE(fresh_allocations.allocations > current.file_count);
      REQUIRE(fresh_allocations.peak >= L);
      CHECK(fresh_allocations.peak <= 3 * L + 2 * E + C + I + 256 * 1024);
      CHECK(rebind_allocations.peak <= 5 * (L + E) + C + I + 256 * 1024);
      CHECK(fresh_allocations.live <= L + 2 * E + 256 * 1024);
    }
#endif

    require_ok(con->Query("SET sirius_test_inject_pin_registry_change=false"), "clear rebuild");
    require_ok(con->Query("SET sirius_test_inject_transparent_gpu_error=''"),
               "clear injected error");
  }

  std::cout << "T6_RESULT scenario=" << scenario << " attempts=" << attempts
            << " finalize_median_us=" << percentile(finalize_us, 0.5)
            << " finalize_p95_us=" << percentile(finalize_us, 0.95)
            << " rebuild_median_us=" << percentile(rebuild_us, 0.5)
            << " rebuild_p95_us=" << percentile(rebuild_us, 0.95)
            << " rss_before_kib=" << prepare_base_kib
            << " rss_prepare_peak_kib=" << prepare_peak_kib
            << " rss_prepare_growth_kib=" << std::max(0L, prepare_peak_kib - prepare_base_kib)
            << " rss_rebind_base_kib=" << rebind_base_kib
            << " rss_rebind_peak_kib=" << rebind_peak_kib
            << " rss_rebind_growth_kib=" << std::max(0L, rebind_peak_kib - rebind_base_kib)
            << " rss_retained_growth_kib=" << std::max(0L, retained_kib)
            << " rss_process_peak_kib=" << process_peak_kib << std::endl;
}

#ifndef SIRIUS_T6_BASELINE
TEST_CASE("T6 evidence index allocations include retained and temporary storage",
          "[.][t6_measure][allocation_coverage]")
{
  struct measurement {
    uint64_t live, peak, cumulative, allocations;
    uint64_t all_live, all_peak, all_cumulative;
    uint64_t overflow;
  };
  auto begin = reinterpret_cast<int (*)()>(dlsym(RTLD_DEFAULT, "sirius_t6_allocation_begin"));
  auto end   = reinterpret_cast<measurement (*)()>(dlsym(RTLD_DEFAULT, "sirius_t6_allocation_end"));
  REQUIRE(begin);
  REQUIRE(end);
  uint64_t previous_live = 0;
  for (auto count : {100u, 10000u}) {
    std::vector<std::string> paths;
    for (unsigned i = 0; i < count; ++i)
      paths.push_back(std::to_string(count - i));
    REQUIRE(begin());
    auto const before = end();
    REQUIRE(begin());
    auto index          = sirius::op::scan::make_read_view_evidence_index(paths);
    auto const measured = end();
    CAPTURE(count, measured.live, measured.peak, measured.all_peak);
    std::cout << "T6_INDEX_ALLOCATION files=" << count
              << " retained=" << measured.live - before.live
              << " peak=" << measured.peak - before.live << " allocations=" << measured.allocations
              << "\n";
    CHECK(measured.overflow == 0);
    CHECK(measured.allocations >= 2);
    CHECK(measured.live >= before.live + count * sizeof(std::size_t));
    CHECK(measured.peak >= before.live + 2 * count * sizeof(std::size_t));
    CHECK(measured.live - before.live > previous_live);
    previous_live = measured.live - before.live;
    // The returned index is the persistent buffer the ingestible owns.
    // Its destruction outside the window must still clear the live record.
    std::vector<std::size_t>().swap(index);
    CHECK(end().live == before.live);
  }
}
#endif
