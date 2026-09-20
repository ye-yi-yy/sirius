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

#include "planner/scan_source_registry.hpp"

#include "exec/stream_plan_bindings.hpp"
#include "op/scan/dynamic_filter_merge.hpp"
#include "sirius_registration.hpp"

#include <dlfcn.h>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/multi_file/multi_file_states.hpp>
#include <duckdb/execution/operator/scan/physical_table_scan.hpp>
#include <duckdb/function/table/table_scan.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/main/extension_helper.hpp>
#include <duckdb/main/extension_manager.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <parquet_multi_file_info.hpp>

#include <array>
#include <mutex>

namespace sirius::planner {
namespace {
using kind  = op::scan::source_kind;
using mode  = op::scan::dynamic_filter_apply_mode;
using bytes = transparent::byte_source_class;

template <class T>
bool matches_bind(duckdb::FunctionData const* bind)
{
  return dynamic_cast<T const*>(bind) != nullptr;
}

std::array<scan_source_entry, 6> const entries{{{"seq_scan",
                                                 kind::duckdb_native,
                                                 "duckdb.seq_scan.v1",
                                                 matches_bind<duckdb::TableScanBindData>,
                                                 lower_native_scan,
                                                 mode::include_ast_row_masks,
                                                 bytes::duckdb_native,
                                                 true,
                                                 false,
                                                 nullptr,
                                                 std::nullopt},
                                                {"parquet_scan",
                                                 kind::parquet_local,
                                                 "duckdb.parquet_scan.v1",
                                                 matches_bind<duckdb::MultiFileBindData>,
                                                 lower_parquet_scan,
                                                 mode::membership_masks_only,
                                                 bytes::local_file,
                                                 true,
                                                 false,
                                                 nullptr,
                                                 std::nullopt},
                                                {"read_parquet",
                                                 kind::parquet_local,
                                                 "duckdb.read_parquet.v1",
                                                 matches_bind<duckdb::MultiFileBindData>,
                                                 lower_parquet_scan,
                                                 mode::membership_masks_only,
                                                 bytes::local_file,
                                                 true,
                                                 false,
                                                 nullptr,
                                                 std::nullopt},
                                                {"sirius_read_parquet",
                                                 kind::parquet_s3,
                                                 "sirius.read_parquet.v1",
                                                 matches_bind<duckdb::SiriusReadParquetBindData>,
                                                 lower_parquet_scan,
                                                 mode::membership_masks_only,
                                                 bytes::sirius_owned_s3,
                                                 false,
                                                 false,
                                                 nullptr,
                                                 std::nullopt},
                                                {"iceberg_scan",
                                                 kind::parquet_local,
                                                 "duckdb.iceberg_scan.v1",
                                                 matches_bind<duckdb::MultiFileBindData>,
                                                 lower_iceberg_scan,
                                                 mode::membership_masks_only,
                                                 bytes::local_file,
                                                 true,
                                                 true,
                                                 registered_iceberg_decline_reason,
                                                 std::nullopt},
                                                {"sirius_stream_source",
                                                 kind::stream_source,
                                                 "sirius.stream_source.v1",
                                                 matches_bind<exec::stream_source_bind_data>,
                                                 nullptr,
                                                 mode::membership_masks_only,
                                                 bytes::stream,
                                                 false,
                                                 false,
                                                 nullptr,
                                                 std::nullopt}}};

duckdb::vector<duckdb::TableFunction> iceberg_reference_functions(duckdb::ClientContext& context)
{
  auto info = duckdb::ExtensionManager::Get(context).GetExtensionInfo("iceberg");
  if (!info || !info->is_loaded || !info->install_info) return {};

  auto& fs = duckdb::FileSystem::GetFileSystem(context);
  duckdb::vector<std::string> paths;
  if (info->install_info->mode == duckdb::ExtensionInstallMode::NOT_INSTALLED) {
    paths.push_back(info->install_info->full_path);
  } else {
    for (auto const& directory : duckdb::ExtensionHelper::GetExtensionDirectoryPath(context))
      paths.push_back(fs.JoinPath(directory, "iceberg.duckdb_extension"));
  }
  for (auto const& path : paths) {
    // Only an extension DuckDB has already loaded may supply the reference definition.
    auto* handle = ::dlopen(path.c_str(), RTLD_NOW | RTLD_NOLOAD);
    if (!handle) continue;
    auto close = [](void* value) { ::dlclose(value); };
    std::unique_ptr<void, decltype(close)> guard(handle, close);
    using initialize = void (*)(duckdb::ExtensionLoader&);
    auto init        = reinterpret_cast<initialize>(::dlsym(handle, "iceberg_duckdb_cpp_init"));
    if (!init) continue;

    // Iceberg hides its function factory. Run its existing registration entry point in a
    // private, CPU-only catalog, never the caller's mutable catalog. No scan is bound here.
    duckdb::DBConfig config;
    config.options.load_extensions = false;
    config.options.maximum_threads = 1;
    duckdb::DuckDB reference(nullptr, &config);
    duckdb::ExtensionHelper::LoadExtension(reference, "parquet");
    duckdb::ExtensionLoader loader(*reference.instance, "iceberg");
    init(loader);
    auto entry = loader.TryGetTableFunction("iceberg_scan");
    if (entry) return entry->Cast<duckdb::TableFunctionCatalogEntry>().functions.functions;
  }
  return {};
}

duckdb::vector<duckdb::TableFunction> reference_functions(std::string const& name,
                                                          duckdb::ClientContext& context)
{
  if (name == "seq_scan") return {duckdb::TableScanFunction::GetFunction()};
  if (name == "parquet_scan" || name == "read_parquet")
    return duckdb::ParquetScanFunction::GetFunctionSet().functions;
  if (name == "sirius_read_parquet") return {duckdb::GetSiriusReadParquetFunction()};
  if (name == "sirius_stream_source") return {exec::get_stream_source_function()};
  return iceberg_reference_functions(context);
}

struct verified_callbacks {
  duckdb::table_function_t function;
  duckdb::table_function_bind_t bind;
  duckdb::table_function_get_multi_file_reader_t get_multi_file_reader;
  duckdb::vector<duckdb::LogicalType> arguments;
  duckdb::LogicalType varargs;
  duckdb::named_parameter_type_map_t named_parameters;

  bool matches(duckdb::TableFunction const& candidate) const
  {
    return function == candidate.function && bind == candidate.bind &&
           get_multi_file_reader == candidate.get_multi_file_reader &&
           arguments == candidate.arguments && varargs == candidate.varargs &&
           named_parameters == candidate.named_parameters;
  }
};
struct accepted_callbacks {
  std::mutex mutex;
  std::vector<verified_callbacks> values;
};
std::array<accepted_callbacks, entries.size()> accepted;
}  // namespace

std::span<scan_source_entry const> registered_scan_sources() { return entries; }

scan_source_entry const* lookup_scan_source(duckdb::TableFunction const& function,
                                            duckdb::FunctionData const* bind,
                                            duckdb::ClientContext& context)
{
  for (size_t i = 0; i < entries.size(); ++i) {
    auto const& entry = entries[i];
    if (entry.function_name != function.name || !entry.bind_data_matches(bind)) continue;
    auto& cache = accepted[i];
    std::lock_guard lock(cache.mutex);
    auto catalog_entry =
      duckdb::Catalog::GetSystemCatalog(context).GetEntry<duckdb::TableFunctionCatalogEntry>(
        context, DEFAULT_SCHEMA, entry.function_name, duckdb::OnEntryNotFound::RETURN_NULL);
    if (!catalog_entry) return nullptr;
    if (cache.values.empty()) {
      // A mutable catalog can confirm registration, but must never grant trust.
      for (auto const& value : reference_functions(entry.function_name, context)) {
        cache.values.push_back({value.function,
                                value.bind,
                                value.get_multi_file_reader,
                                value.arguments,
                                value.varargs,
                                value.named_parameters});
      }
    }
    for (auto const& callbacks : cache.values) {
      if (!callbacks.matches(function)) continue;
      for (auto const& registered : catalog_entry->functions.functions) {
        if (callbacks.matches(registered)) return &entry;
      }
    }
    return nullptr;
  }
  return nullptr;
}

scan_source_entry const* lookup_scan_source(duckdb::LogicalGet const& get,
                                            duckdb::ClientContext& context)
{
  return lookup_scan_source(get.function, get.bind_data.get(), context);
}

scan_source_entry const* lookup_scan_source(duckdb::PhysicalTableScan const& get,
                                            duckdb::ClientContext& context)
{
  return lookup_scan_source(get.function, get.bind_data.get(), context);
}
}  // namespace sirius::planner
