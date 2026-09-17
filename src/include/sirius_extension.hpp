/*
 * Copyright 2025, Sirius Contributors.
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

#pragma once

#include "duckdb.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"

#include <cstddef>
#include <memory>
#include <string>
#include <utility>

namespace sirius {
struct sirius_config;
namespace scan {
class bound_schema;
}  // namespace scan
}  // namespace sirius

namespace duckdb {
class GPUBufferManager;
struct DBConfig;

// Bind-time payload for the sirius_read_parquet table function. Carries the
// supplied URI, full schema and the parquet's footer row count. Scan lowering
// reads this retained payload, while DuckDB's
// optimizer sees a real cardinality estimate via the registered cardinality
// callback instead of falling back to "unknown table function output".
struct SiriusReadParquetBindData : public FunctionData {
  SiriusReadParquetBindData(std::string uri,
                            std::size_t total_num_rows,
                            std::shared_ptr<const sirius::scan::bound_schema> schema = nullptr)
    : uri(std::move(uri)), total_num_rows(total_num_rows), schema(std::move(schema))
  {
  }

  const std::string uri;
  const std::size_t total_num_rows;
  // Null denotes metadata-only/incomplete binding, not a complete zero-column schema.
  const std::shared_ptr<const sirius::scan::bound_schema> schema;

  unique_ptr<FunctionData> Copy() const override
  {
    return make_uniq<SiriusReadParquetBindData>(uri, total_num_rows, schema);
  }

  bool Equals(FunctionData const& other_p) const override;
};

// Cardinality callback for sirius_read_parquet. Returns the footer row count
// (exact value, doubles as both estimated and max cardinality). Returns
// nullptr on a null or wrong-type FunctionData so DuckDB falls back to its
// default behavior.
unique_ptr<NodeStatistics> SiriusReadParquetCardinality(ClientContext& context,
                                                        FunctionData const* bind_data);

class SiriusExtension : public Extension {
 public:
  void Load(ExtensionLoader& loader) override;
  std::string Name() override;
  std::string Version() const override;
  /// Register Sirius's extension options. @p defaults supplies the registered default for every
  /// option DuckDB stores per connection, so a sirius.yaml value reaches those connections as
  /// their inherited starting point instead of being shadowed by the compiled default.
  static void InitialGPUConfigs(DBConfig& db, const sirius::sirius_config& defaults);
  static void RegisterGPUFunctions(DatabaseInstance& catalog);
#ifdef SIRIUS_ENABLE_LEGACY
  static void GPUProcessingSubstraitFunction(ClientContext& context,
                                             TableFunctionInput& data_p,
                                             DataChunk& output);
  static void GPUProcessingFunction(ClientContext& context,
                                    TableFunctionInput& data_p,
                                    DataChunk& output);
  static unique_ptr<FunctionData> GPUProcessingSubstraitBind(ClientContext& context,
                                                             TableFunctionBindInput& input,
                                                             vector<LogicalType>& return_types,
                                                             vector<string>& names);
  static unique_ptr<FunctionData> GPUProcessingBind(ClientContext& context,
                                                    TableFunctionBindInput& input,
                                                    vector<LogicalType>& return_types,
                                                    vector<string>& names);
#endif
  static void GPUExecutionFunction(ClientContext& context,
                                   TableFunctionInput& data_p,
                                   DataChunk& output);
#ifdef SIRIUS_ENABLE_LEGACY
  static void GPUBufferInitFunction(ClientContext& context,
                                    TableFunctionInput& data_p,
                                    DataChunk& output);
  static unique_ptr<FunctionData> GPUBufferInitBind(ClientContext& context,
                                                    TableFunctionBindInput& input,
                                                    vector<LogicalType>& return_types,
                                                    vector<string>& names);
#endif
  static unique_ptr<FunctionData> GPUExecutionBind(ClientContext& context,
                                                   TableFunctionBindInput& input,
                                                   vector<LogicalType>& return_types,
                                                   vector<string>& names);
  /// Per-execution state factory for gpu_execution(): a reusable prepared
  /// statement gets fresh execution state (result/connection/interface) on
  /// every execute instead of reusing bind-held state.
  static unique_ptr<GlobalTableFunctionState> GPUExecutionInitGlobal(ClientContext& context,
                                                                     TableFunctionInitInput& input);

  static void PinTableFunction(ClientContext& context,
                               TableFunctionInput& data_p,
                               DataChunk& output);
  static unique_ptr<FunctionData> PinTableBind(ClientContext& context,
                                               TableFunctionBindInput& input,
                                               vector<LogicalType>& return_types,
                                               vector<string>& names);

  static void UnpinTableFunction(ClientContext& context,
                                 TableFunctionInput& data_p,
                                 DataChunk& output);
  static unique_ptr<FunctionData> UnpinTableBind(ClientContext& context,
                                                 TableFunctionBindInput& input,
                                                 vector<LogicalType>& return_types,
                                                 vector<string>& names);

#ifdef SIRIUS_ENABLE_LEGACY
  static bool buffer_is_initialized;
#endif
};

}  // namespace duckdb
