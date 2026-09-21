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
#include "sirius_extension.hpp"
#include "transparent/plan_source_policy.hpp"
#include "transparent/read_view_registry.hpp"
#include "utils/sirius_test_env.hpp"

#include <catch.hpp>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp>
#include <duckdb/common/multi_file/multi_file_function.hpp>
#include <duckdb/execution/operator/scan/physical_table_scan.hpp>
#include <duckdb/execution/physical_plan_generator.hpp>
#include <duckdb/planner/operator/logical_dummy_scan.hpp>
#include <duckdb/planner/operator/logical_get.hpp>

namespace {
using namespace sirius::transparent;

duckdb::unique_ptr<duckdb::LogicalGet> file_scan(std::string name, std::string path)
{
  auto bind       = duckdb::make_uniq<duckdb::MultiFileBindData>();
  bind->file_list = duckdb::make_shared_ptr<duckdb::SimpleMultiFileList>(
    duckdb::vector<duckdb::OpenFileInfo>{duckdb::OpenFileInfo(std::move(path))});
  duckdb::TableFunction function(std::move(name), {}, nullptr);
  return duckdb::make_uniq<duckdb::LogicalGet>(1,
                                               std::move(function),
                                               std::move(bind),
                                               duckdb::vector<duckdb::LogicalType>{},
                                               duckdb::vector<std::string>{});
}

struct throwing_list final : duckdb::SimpleMultiFileList {
  throwing_list() : SimpleMultiFileList({}) {}
  duckdb::vector<duckdb::OpenFileInfo> GetAllFiles() const override
  {
    throw std::runtime_error("inventory extraction failed");
  }
  duckdb::FileExpandResult GetExpandResult() const override
  {
    throw std::runtime_error("inventory extraction failed");
  }
  duckdb::idx_t GetTotalFileCount() const override
  {
    throw std::runtime_error("inventory extraction failed");
  }
  duckdb::OpenFileInfo GetFile(duckdb::idx_t) const override
  {
    throw std::runtime_error("inventory extraction failed");
  }
};

struct external_child_operator final : duckdb::PhysicalOperator {
  external_child_operator(duckdb::PhysicalPlan& plan,
                          duckdb::PhysicalOperator const* child,
                          bool fail)
    : PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION, {}, 0),
      child(child),
      fail(fail)
  {
  }
  duckdb::PhysicalOperator const* child;
  bool fail;
  duckdb::vector<duckdb::const_reference<duckdb::PhysicalOperator>> GetChildren() const override
  {
    if (fail) throw std::runtime_error("physical walk failed");
    if (child) return {*child};
    return {};
  }
};
}  // namespace

TEST_CASE("Source policy permits complete empty local and native plans",
          "[transparent][policy][shared_context]")
{
  REQUIRE(sirius::test::g_shared_env);
  auto con = sirius::test::g_shared_env->make_connection();
  REQUIRE_FALSE(con.Query("SET gpu_execution=false")->HasError());
  auto local  = file_scan("unregistered_local", "local.parquet");
  auto policy = derive_plan_source_policy(*local, *con.context);
  REQUIRE(policy.discovery_complete);
  REQUIRE(policy.cpu_replay_permitted());
  REQUIRE(policy.scans.at(0).source == byte_source_class::local_file);
  duckdb::LogicalDummyScan empty(0);
  REQUIRE(derive_plan_source_policy(empty, *con.context).cpu_replay_permitted());
  REQUIRE_FALSE(con.Query("CREATE TEMP TABLE r1_policy_native(id INTEGER)")->HasError());
  REQUIRE_FALSE(con.Query("BEGIN")->HasError());
  auto native        = con.ExtractPlan("SELECT * FROM r1_policy_native");
  auto native_policy = derive_plan_source_policy(*native, *con.context);
  REQUIRE(native_policy.discovery_complete);
  REQUIRE(native_policy.cpu_replay_permitted());
  REQUIRE(native_policy.scans.at(0).source == byte_source_class::duckdb_native);
  REQUIRE_FALSE(con.Query("ROLLBACK")->HasError());
}

TEST_CASE("Source policy forbids S3 from unregistered MultiFile sources",
          "[transparent][policy][shared_context]")
{
  REQUIRE(sirius::test::g_shared_env);
  auto con    = sirius::test::g_shared_env->make_connection();
  auto s3     = file_scan("unregistered_s3", "s3://bucket/data.parquet");
  auto policy = derive_plan_source_policy(*s3, *con.context);
  REQUIRE(policy.discovery_complete);
  REQUIRE_FALSE(policy.cpu_replay_permitted());
  REQUIRE(policy.scans.at(0).source == byte_source_class::sirius_owned_s3);
  REQUIRE(policy.reason().find("unregistered_s3") != std::string::npos);
}

TEST_CASE("Source policy preserves S3 veto beside local and unclassified nodes",
          "[transparent][policy][shared_context]")
{
  REQUIRE(sirius::test::g_shared_env);
  auto con = sirius::test::g_shared_env->make_connection();
  duckdb::LogicalDummyScan root(0);
  root.children.push_back(file_scan("local", "local.parquet"));
  auto unknown =
    duckdb::make_uniq<duckdb::LogicalGet>(2,
                                          duckdb::TableFunction("opaque_source", {}, nullptr),
                                          nullptr,
                                          duckdb::vector<duckdb::LogicalType>{},
                                          duckdb::vector<std::string>{});
  auto alone = derive_plan_source_policy(*unknown, *con.context);
  REQUIRE(alone.discovery_complete);
  REQUIRE(alone.cpu_replay_permitted());
  REQUIRE(alone.scans.at(0).source == byte_source_class::unclassified);
  root.children.push_back(std::move(unknown));
  root.children.push_back(file_scan("remote", "S3://bucket/data.parquet"));
  auto policy = derive_plan_source_policy(root, *con.context);
  REQUIRE(policy.discovery_complete);
  REQUIRE_FALSE(policy.cpu_replay_permitted());
  REQUIRE(policy.reason().find("remote") != std::string::npos);
}

TEST_CASE("Source policy treats failed node extraction as incomplete",
          "[transparent][policy][shared_context]")
{
  REQUIRE(sirius::test::g_shared_env);
  auto con   = sirius::test::g_shared_env->make_connection();
  auto node  = file_scan("broken", "local.parquet");
  auto& bind = node->bind_data->Cast<duckdb::MultiFileBindData>();
  SECTION("missing list") { bind.file_list.reset(); }
  SECTION("throwing list") { bind.file_list = duckdb::make_shared_ptr<throwing_list>(); }
  auto policy = derive_plan_source_policy(*node, *con.context);
  REQUIRE_FALSE(policy.discovery_complete);
  REQUIRE_FALSE(policy.cpu_replay_permitted());
  REQUIRE(policy.reason().find("incomplete") != std::string::npos);
}

TEST_CASE("Source policy visits physical GetChildren and forbids a failed walk",
          "[transparent][policy][shared_context]")
{
  REQUIRE(sirius::test::g_shared_env);
  auto con = sirius::test::g_shared_env->make_connection();
  duckdb::PhysicalPlan plan(duckdb::Allocator::Get(*con.context));
  auto logical = file_scan("hidden_remote", "s3://bucket/data.parquet");
  auto& leaf   = plan.Make<duckdb::PhysicalTableScan>(duckdb::vector<duckdb::LogicalType>{},
                                                    logical->function,
                                                    std::move(logical->bind_data),
                                                    duckdb::vector<duckdb::LogicalType>{},
                                                    duckdb::vector<duckdb::ColumnIndex>{},
                                                    duckdb::vector<duckdb::idx_t>{},
                                                    duckdb::vector<std::string>{},
                                                    nullptr,
                                                    0,
                                                    duckdb::ExtraOperatorInfo{},
                                                    duckdb::vector<duckdb::Value>{},
                                                    duckdb::virtual_column_map_t{});
  external_child_operator root(plan, &leaf, false);
  REQUIRE(root.children.empty());
  auto policy = derive_plan_source_policy(root, *con.context);
  REQUIRE(policy.discovery_complete);
  REQUIRE_FALSE(policy.cpu_replay_permitted());
  REQUIRE(policy.reason().find("hidden_remote") != std::string::npos);
  root.fail = true;
  policy    = derive_plan_source_policy(root, *con.context);
  REQUIRE_FALSE(policy.discovery_complete);
  REQUIRE_FALSE(policy.cpu_replay_permitted());
}

TEST_CASE("Source policy forbids verified stream and Sirius S3 sources",
          "[transparent][policy][shared_context]")
{
  REQUIRE(sirius::test::g_shared_env);
  auto con = sirius::test::g_shared_env->make_connection();
  REQUIRE_FALSE(con.Query("SET gpu_execution=false")->HasError());
  REQUIRE_FALSE(con.Query("BEGIN")->HasError());
  for (std::string name : {"sirius_stream_source", "sirius_read_parquet"}) {
    auto& entry =
      duckdb::Catalog::GetSystemCatalog(*con.context)
        .GetEntry<duckdb::TableFunctionCatalogEntry>(*con.context, DEFAULT_SCHEMA, name);
    duckdb::unique_ptr<duckdb::FunctionData> bind;
    if (name == "sirius_stream_source") {
      bind = duckdb::make_uniq<sirius::exec::stream_source_bind_data>(42);
    } else {
      bind = duckdb::make_uniq<duckdb::SiriusReadParquetBindData>("s3://bucket/data.parquet", 1);
    }
    duckdb::LogicalGet node(1, entry.functions.GetFunctionByOffset(0), std::move(bind), {}, {});
    auto policy = derive_plan_source_policy(node, *con.context);
    REQUIRE(policy.discovery_complete);
    REQUIRE_FALSE(policy.cpu_replay_permitted());
    if (name == "sirius_stream_source") {
      REQUIRE(policy.scans.at(0).source == byte_source_class::stream);
      REQUIRE(policy.reason().find("stream source has no CPU body") != std::string::npos);
    } else {
      REQUIRE(policy.scans.at(0).source == byte_source_class::sirius_owned_s3);
    }
  }
  REQUIRE_FALSE(con.Query("ROLLBACK")->HasError());
}

TEST_CASE("Source policy preserves S3 error text and stream reason", "[transparent][policy]")
{
  read_view_comparison mismatch;
  mismatch.correspondence     = "none";
  mismatch.reason             = "no_correspondence";
  auto const mismatch_message = describe_read_view_mismatch(mismatch);

  plan_source_policy policy;
  policy.scans.push_back({"remote", byte_source_class::sirius_owned_s3, false, "S3"});
  REQUIRE_THROWS_WITH(
    require_cpu_replay(policy, "", "boom"),
    "S3 CPU fallback is not supported: this query reads s3:// data, GPU execution failed, and "
    "Sirius has no CPU fallback for S3 data sources. Underlying GPU error: boom");
  policy.scans = {
    {"sirius_stream_source", byte_source_class::stream, false, "stream source has no CPU body"}};
  REQUIRE_THROWS_WITH(
    require_cpu_replay(policy, "", mismatch_message),
    "CPU fallback is not supported: sirius_stream_source: stream source has no CPU body. "
    "Underlying GPU error: read-view mismatch: reason=no_correspondence, correspondence=none, "
    "original_count=0, candidate_count=0, different_total=0, different_original=0, "
    "different_candidate=0, "
    "original_hash=none, candidate_hash=none, original_depth=none, candidate_depth=none, "
    "only_original=[], only_candidate=[]");
  policy.scans.clear();
  policy.discovery_complete = false;
  REQUIRE_THROWS_WITH(
    require_cpu_replay(policy, "", "boom"),
    "CPU fallback is not supported: source discovery incomplete. Underlying GPU error: boom");
}

TEST_CASE("Source policy retains known S3 evidence after a sibling extraction failure",
          "[transparent][policy][shared_context]")
{
  REQUIRE(sirius::test::g_shared_env);
  auto con = sirius::test::g_shared_env->make_connection();
  duckdb::LogicalDummyScan root(0);
  auto broken = file_scan("broken", "local.parquet");
  broken->bind_data->Cast<duckdb::MultiFileBindData>().file_list.reset();
  root.children.push_back(std::move(broken));
  root.children.push_back(file_scan("remote_after_failure", "s3://bucket/data.parquet"));
  auto policy = derive_plan_source_policy(root, *con.context);
  REQUIRE_FALSE(policy.discovery_complete);
  REQUIRE_FALSE(policy.cpu_replay_permitted());
  REQUIRE(policy.reads_sirius_owned_s3());
  REQUIRE(policy.reason().find("remote_after_failure") != std::string::npos);
}
