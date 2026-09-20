/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "exec/stream_plan_bindings.hpp"
#include "op/scan/table_scan/bound_read_view.hpp"
#include "op/scan/table_scan/scan_contract.hpp"
#include "planner/scan_source_registry.hpp"
#include "planner/sirius_physical_plan_generator.hpp"
#include "sirius_extension.hpp"
#include "utils/child_process_environment.hpp"
#include "utils/sirius_test_env.hpp"

#include <catch.hpp>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp>
#include <duckdb/common/multi_file/multi_file_function.hpp>
#include <duckdb/function/table/table_scan.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <csignal>
#include <set>
#include <string>
#include <thread>
#include <vector>

namespace {
using namespace sirius::op::scan;

bound_read_view file_view(
  std::vector<std::string> const& paths,
  std::string selector                      = "",
  duckdb::vector<duckdb::LogicalType> types = {duckdb::LogicalType::INTEGER},
  duckdb::vector<std::string> names         = {"id"})
{
  bound_read_identity identity;
  identity.source      = {"read_parquet", source_kind::parquet_local, "duckdb.read_parquet.v1"};
  identity.data_view   = file_inventory{static_cast<uint32_t>(paths.size())};
  identity.bound_types = std::move(types);
  identity.bound_names = std::move(names);
  identity.selector    = std::move(selector);
  bound_read_view view;
  view.identity = make_bound_read_identity(std::move(identity), paths);
  return view;
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
  throw std::out_of_range("No LogicalGet");
}

void fake_scan(duckdb::ClientContext&, duckdb::TableFunctionInput&, duckdb::DataChunk&) {}

duckdb::TableCatalogEntry* replacement_table = nullptr;

duckdb::unique_ptr<duckdb::FunctionData> replacement_bind(
  duckdb::ClientContext&,
  duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& types,
  duckdb::vector<std::string>& names)
{
  types            = {duckdb::LogicalType::INTEGER};
  names            = {"id"};
  auto const& name = input.table_function.name;
  if (name == "seq_scan") return duckdb::make_uniq<duckdb::TableScanBindData>(*replacement_table);
  if (name == "sirius_read_parquet")
    return duckdb::make_uniq<duckdb::SiriusReadParquetBindData>("s3://bucket/a.parquet", 1);
  if (name == "sirius_stream_source")
    return duckdb::make_uniq<sirius::exec::stream_source_bind_data>(1);
  return duckdb::make_uniq<duckdb::MultiFileBindData>();
}

duckdb::unique_ptr<duckdb::FunctionData> fake_bind(duckdb::ClientContext&,
                                                   duckdb::TableFunctionBindInput&,
                                                   duckdb::vector<duckdb::LogicalType>&,
                                                   duckdb::vector<std::string>&)
{
  return nullptr;
}
duckdb::unique_ptr<duckdb::MultiFileReader> fake_reader(duckdb::TableFunction const&)
{
  return nullptr;
}
struct registry_test_generator : sirius::planner::sirius_physical_plan_generator {
  using sirius_physical_plan_generator::create_plan;
  using sirius_physical_plan_generator::sirius_physical_plan_generator;
};
}  // namespace

TEST_CASE("Read-view equality ignores independent evidence", "[scan][contracts]")
{
  auto base = file_view({"one.parquet"});
  std::vector<bound_read_view> views{base};
  for (int field = 0; field < 2; ++field) {
    for (int value = 0; value < 3; ++value) {
      auto view                      = file_view({"one.parquet"});
      auto evidence                  = std::make_shared<file_evidence_arrays>();
      evidence->size                 = {value == 2 ? 1024 : 512};
      evidence->size_present         = {static_cast<uint8_t>(field == 0 && value != 0)};
      evidence->etag                 = {field == 1 && value != 0 ? (value == 2 ? "v2" : "v1") : ""};
      view.evidence                  = std::move(evidence);
      view.transaction_id            = 100 + value;
      view.logical_selector_evidence = std::to_string(value);
      views.push_back(std::move(view));
    }
  }
  for (auto const& a : views) {
    for (auto const& b : views) {
      for (auto const& c : views) {
        REQUIRE(canonical_read_view_text(a) == canonical_read_view_text(b));
        REQUIRE(canonical_read_view_text(b) == canonical_read_view_text(c));
        REQUIRE(canonical_read_view_text(c) == canonical_read_view_text(a));
        REQUIRE(a.identity->fingerprint.hash == b.identity->fingerprint.hash);
      }
    }
  }
}

TEST_CASE("Read-view file inventory is a collision-safe multiset", "[scan][contracts]")
{
  auto a = file_view({"a|b", "'\n", ""});
  REQUIRE(canonical_read_view_text(a) == canonical_read_view_text(file_view({"", "'\n", "a|b"})));
  REQUIRE(canonical_read_view_text(a) !=
          canonical_read_view_text(file_view({"a|b", "'\n", "", ""})));
  REQUIRE(canonical_read_view_text(file_view({"a", "b"})) !=
          canonical_read_view_text(file_view({"a|b"})));
  REQUIRE(canonical_read_view_text(file_view({""})) != canonical_read_view_text(file_view({})));
  REQUIRE(canonical_read_view_text(file_view({"a", "b"})) !=
          canonical_read_view_text(file_view({"a"})));
  REQUIRE(canonical_read_view_text(file_view({std::string("a\0b", 3)})) !=
          canonical_read_view_text(file_view({"a"})));
}

TEST_CASE("Read-view identity changes with bound schema and options", "[scan][contracts]")
{
  auto base = file_view({"a"});
  REQUIRE(canonical_read_view_text(base) !=
          canonical_read_view_text(file_view({"a"}, "", {duckdb::LogicalType::BIGINT})));
  REQUIRE(
    canonical_read_view_text(base) !=
    canonical_read_view_text(file_view({"a"}, "", {duckdb::LogicalType::INTEGER}, {"renamed"})));
  for (auto const* option : {"hive_partitioning", "union_by_name", "filename"}) {
    REQUIRE(canonical_read_view_text(base) != canonical_read_view_text(file_view({"a"}, option)));
  }
}

TEST_CASE("Native read-view identity includes catalog schema and table incarnation",
          "[scan][contracts]")
{
  auto make = [](duckdb::idx_t catalog_oid, std::string schema, duckdb::idx_t table_oid) {
    bound_read_identity identity;
    identity.source = {"seq_scan", source_kind::duckdb_native, "duckdb.seq_scan.v1"};
    identity.data_view =
      native_table_identity{"db", catalog_oid, std::move(schema), "t", table_oid, "db.db"};
    identity.bound_types = {duckdb::LogicalType::INTEGER};
    identity.bound_names = {"id"};
    bound_read_view view;
    view.identity = make_bound_read_identity(std::move(identity), {});
    return canonical_read_view_text(view);
  };
  REQUIRE(make(1, "main", 2) != make(2, "main", 2));
  REQUIRE(make(1, "main", 2) != make(1, "other", 2));
  REQUIRE(make(1, "main", 2) != make(1, "main", 3));
}

TEST_CASE("Scan registry verifies all six catalog functions", "[scan][contracts][shared_context]")
{
  REQUIRE(sirius::test::g_shared_env);
  auto con = sirius::test::g_shared_env->make_connection();
  REQUIRE_FALSE(con.Query("SET gpu_execution=false")->HasError());
  REQUIRE_FALSE(con.Query("LOAD iceberg")->HasError());
  REQUIRE_FALSE(con.Query("CREATE TEMP TABLE r1_registry_native(id INTEGER)")->HasError());
  REQUIRE_FALSE(con.Query("BEGIN")->HasError());
  auto native      = con.ExtractPlan("SELECT * FROM r1_registry_native");
  auto& native_get = first_get(*native);
  REQUIRE(sirius::planner::lookup_scan_source(native_get, *con.context));

  std::set<std::string> names;
  for (auto const& entry : sirius::planner::registered_scan_sources())
    names.insert(entry.function_name);
  REQUIRE(names == std::set<std::string>{"seq_scan",
                                         "parquet_scan",
                                         "read_parquet",
                                         "sirius_read_parquet",
                                         "iceberg_scan",
                                         "sirius_stream_source"});
  for (auto const& name : names) {
    auto& catalog_entry =
      duckdb::Catalog::GetSystemCatalog(*con.context)
        .GetEntry<duckdb::TableFunctionCatalogEntry>(*con.context, DEFAULT_SCHEMA, name);
    for (auto function : catalog_entry.functions.functions) {
      duckdb::unique_ptr<duckdb::FunctionData> bind;
      if (name == "seq_scan") {
        bind = native_get.bind_data->Copy();
      } else if (name == "sirius_read_parquet") {
        bind = duckdb::make_uniq<duckdb::SiriusReadParquetBindData>("s3://bucket/a.parquet", 1);
      } else if (name == "sirius_stream_source") {
        bind = duckdb::make_uniq<sirius::exec::stream_source_bind_data>(1);
      } else {
        bind = duckdb::make_uniq<duckdb::MultiFileBindData>();
      }
      duckdb::LogicalGet get(1, function, std::move(bind), {}, {});
      REQUIRE(sirius::planner::lookup_scan_source(get, *con.context));
      get.function.function = fake_scan;
      REQUIRE_FALSE(sirius::planner::lookup_scan_source(get, *con.context));
      get.function      = function;
      get.function.bind = fake_bind;
      REQUIRE_FALSE(sirius::planner::lookup_scan_source(get, *con.context));
      get.function                       = function;
      get.function.get_multi_file_reader = fake_reader;
      REQUIRE_FALSE(sirius::planner::lookup_scan_source(get, *con.context));
      get.function           = function;
      get.function.arguments = {duckdb::LogicalType::BLOB, duckdb::LogicalType::BLOB};
      REQUIRE_FALSE(sirius::planner::lookup_scan_source(get, *con.context));
      get.function = function;
      get.bind_data.reset();
      REQUIRE_FALSE(sirius::planner::lookup_scan_source(get, *con.context));
    }
  }
  native_get.function.name = "r1_unknown_scan";
  REQUIRE_FALSE(sirius::planner::lookup_scan_source(native_get, *con.context));
  registry_test_generator generator(*con.context);
  REQUIRE_THROWS_WITH(
    generator.create_plan(native_get),
    Catch::Matchers::Contains("Table function 'r1_unknown_scan' is not supported in Sirius"));

  REQUIRE_FALSE(con.Query("ROLLBACK")->HasError());
}

TEST_CASE("Scan consumption fields do not alter read identity", "[scan][contracts]")
{
  bound_table_scan a;
  a.view                   = std::make_shared<bound_read_view>(file_view({"one.parquet"}));
  auto b                   = a;
  a.scan_node_id           = 10;
  b.scan_node_id           = 11;
  a.output_types           = {duckdb::LogicalType::INTEGER};
  b.output_types           = {duckdb::LogicalType::BIGINT};
  a.columns.column_ids     = {duckdb::ColumnIndex(0)};
  b.columns.column_ids     = {duckdb::ColumnIndex(1)};
  a.columns.projection_ids = {0};
  b.columns.projection_ids = {1};
  a.predicates.static_filter_fingerprint = "id > 5";
  b.predicates.static_filter_fingerprint = "id < 10";
  REQUIRE(canonical_read_view_text(*a.view) == canonical_read_view_text(*b.view));
  REQUIRE(a.view->identity->fingerprint.hash == b.view->identity->fingerprint.hash);
}

TEST_CASE("Logical scan copy retains table index and verified source",
          "[scan][contracts][shared_context]")
{
  REQUIRE(sirius::test::g_shared_env);
  auto con = sirius::test::g_shared_env->make_connection();
  REQUIRE_FALSE(con.Query("SET gpu_execution=false")->HasError());
  REQUIRE_FALSE(
    con.Query("CREATE TEMP TABLE r1_copy_native(id INTEGER, value BIGINT)")->HasError());
  REQUIRE_FALSE(con.Query("INSERT INTO r1_copy_native VALUES (0, 1), (10, 2)")->HasError());
  REQUIRE_FALSE(con.Query("BEGIN")->HasError());
  auto plan        = con.ExtractPlan("SELECT value FROM r1_copy_native WHERE id > 5");
  auto& get        = first_get(*plan);
  auto copy        = get.Copy(*con.context);
  auto& copied_get = copy->Cast<duckdb::LogicalGet>();
  REQUIRE(copied_get.table_index == get.table_index);
  REQUIRE(sirius::planner::lookup_scan_source(copied_get, *con.context));
  REQUIRE_FALSE(con.Query("ROLLBACK")->HasError());
}

TEST_CASE("Canonical values distinguish NULL empty text and logical type", "[scan][contracts]")
{
  REQUIRE(canonical_value_text(duckdb::Value(duckdb::LogicalType::VARCHAR)) !=
          canonical_value_text(duckdb::Value("")));
  REQUIRE(canonical_value_text(duckdb::Value(duckdb::LogicalType::VARCHAR)) !=
          canonical_value_text(duckdb::Value(duckdb::LogicalType::INTEGER)));
  auto first  = file_view({"a"}).identity->fingerprint;
  auto second = file_view({"b"}).identity->fingerprint;
  second.hash = first.hash;
  REQUIRE_FALSE(first == second);
}

TEST_CASE("Dropping and recreating a native table changes read identity",
          "[scan][contracts][shared_context]")
{
  REQUIRE(sirius::test::g_shared_env);
  auto con = sirius::test::g_shared_env->make_connection();
  REQUIRE_FALSE(con.Query("SET gpu_execution=false")->HasError());
  auto identity = [&] {
    auto plan  = con.ExtractPlan("SELECT * FROM r1_recreated_native");
    auto& bind = first_get(*plan).bind_data->Cast<duckdb::TableScanBindData>();
    bound_read_identity value;
    value.source = {"seq_scan", source_kind::duckdb_native, "duckdb.seq_scan.v1"};
    value.data_view =
      native_table_identity{"memory", 1, "main", "r1_recreated_native", bind.table.oid, ""};
    value.bound_types = {duckdb::LogicalType::INTEGER};
    value.bound_names = {"id"};
    return make_bound_read_identity(std::move(value), {})->fingerprint;
  };
  REQUIRE_FALSE(con.Query("CREATE TEMP TABLE r1_recreated_native(id INTEGER)")->HasError());
  auto before = identity();
  REQUIRE_FALSE(con.Query("DROP TABLE r1_recreated_native")->HasError());
  REQUIRE_FALSE(con.Query("CREATE TEMP TABLE r1_recreated_native(id INTEGER)")->HasError());
  REQUIRE_FALSE(before == identity());
}

TEST_CASE("Scan registry rejects registered replacements before or after first lookup",
          "[scan][contracts][isolated_context]")
{
  for (auto const* phase : {"cold", "warm", "cold_same", "warm_same"}) {
    INFO(phase);
    sirius::test::child_process_environment environment{{{"SIRIUS_REGISTRY_TRUST_PHASE", phase}}};
    std::string executable = "sirius_unittest";
    std::string filter     = "Scan registry first lookup child";
    char* arguments[]      = {executable.data(), filter.data(), nullptr};
    pid_t pid{};
    REQUIRE(
      ::posix_spawn(&pid, "/proc/self/exe", nullptr, nullptr, arguments, environment.data()) == 0);
    int status{};
    auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    pid_t waited{};
    while ((waited = ::waitpid(pid, &status, WNOHANG)) == 0 &&
           std::chrono::steady_clock::now() < deadline)
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    if (waited == 0) {
      ::kill(pid, SIGKILL);
      ::waitpid(pid, &status, 0);
      FAIL("registry lookup child timed out");
    }
    REQUIRE(waited == pid);
    REQUIRE(WIFEXITED(status));
    CHECK(WEXITSTATUS(status) == 0);
  }
}

TEST_CASE("Scan registry first lookup child", "[.][registry_trust_child][shared_context]")
{
  auto const* phase = std::getenv("SIRIUS_REGISTRY_TRUST_PHASE");
  if (!phase) return;
  REQUIRE(sirius::test::g_shared_env);
  auto con = sirius::test::g_shared_env->make_connection();
  REQUIRE_FALSE(con.Query("SET gpu_execution=false")->HasError());
  REQUIRE_FALSE(con.Query("LOAD iceberg")->HasError());
  REQUIRE_FALSE(con.Query("CREATE TEMP TABLE r1_replacement_native(id INTEGER)")->HasError());
  REQUIRE_FALSE(con.Query("INSERT INTO r1_replacement_native VALUES (1)")->HasError());
  REQUIRE_FALSE(con.Query("BEGIN")->HasError());
  auto native       = con.ExtractPlan("SELECT * FROM r1_replacement_native");
  replacement_table = &first_get(*native).bind_data->Cast<duckdb::TableScanBindData>().table;
  duckdb::ExtensionLoader loader(*con.context->db, "registry_replacement_test");
  for (auto const& source : sirius::planner::registered_scan_sources()) {
    INFO(source.function_name);
    auto& catalog = duckdb::Catalog::GetSystemCatalog(*con.context);
    auto original = catalog
                      .GetEntry<duckdb::TableFunctionCatalogEntry>(
                        *con.context, DEFAULT_SCHEMA, source.function_name)
                      .functions.functions.front();
    duckdb::unique_ptr<duckdb::FunctionData> bind;
    if (source.function_name == "seq_scan")
      bind = first_get(*native).bind_data->Copy();
    else if (source.function_name == "sirius_read_parquet")
      bind = duckdb::make_uniq<duckdb::SiriusReadParquetBindData>("s3://bucket/a.parquet", 1);
    else if (source.function_name == "sirius_stream_source")
      bind = duckdb::make_uniq<sirius::exec::stream_source_bind_data>(1);
    else
      bind = duckdb::make_uniq<duckdb::MultiFileBindData>();
    if (std::string_view(phase).starts_with("warm"))
      REQUIRE(sirius::planner::lookup_scan_source(original, bind.get(), *con.context));
    if (std::string_view(phase).ends_with("_same")) {
      auto replacement     = original;
      replacement.function = fake_scan;
      duckdb::CreateTableFunctionInfo info(replacement);
      info.on_conflict = duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
      loader.RegisterFunction(std::move(info));
      auto registered = catalog
                          .GetEntry<duckdb::TableFunctionCatalogEntry>(
                            *con.context, DEFAULT_SCHEMA, source.function_name)
                          .functions.functions.front();
      REQUIRE(registered.function == fake_scan);
      CHECK_FALSE(sirius::planner::lookup_scan_source(registered, bind.get(), *con.context));
      duckdb::CreateTableFunctionInfo restore(original);
      restore.on_conflict = duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
      loader.RegisterFunction(std::move(restore));
    } else {
      duckdb::TableFunction replacement(
        source.function_name, {duckdb::LogicalType::INTEGER}, fake_scan, replacement_bind);
      loader.RegisterFunction(replacement);
      auto plan = con.ExtractPlan("SELECT * FROM " + source.function_name + "(42)");
      auto& get = first_get(*plan);
      REQUIRE(get.function.function == fake_scan);
      REQUIRE(source.bind_data_matches(get.bind_data.get()));
      CHECK_FALSE(sirius::planner::lookup_scan_source(get, *con.context));
      registry_test_generator generator(*con.context);
      CHECK_THROWS_WITH(generator.create_plan(get),
                        Catch::Matchers::Contains("unverified callbacks"));
    }
    CHECK(sirius::planner::lookup_scan_source(original, bind.get(), *con.context));
  }
  REQUIRE_FALSE(con.Query("ROLLBACK")->HasError());
  replacement_table = nullptr;
}
