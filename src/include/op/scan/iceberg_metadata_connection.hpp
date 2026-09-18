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

#include "scan/internal_connection.hpp"

namespace sirius::op::scan {

// Callers supply the bound snapshot; this connection must not resolve a new current snapshot.
class iceberg_metadata_connection {
 public:
  explicit iceberg_metadata_connection(duckdb::ClientContext& context)
    : _connection(
        sirius::scan::open_internal_connection(context, {"unsafe_enable_version_guessing"}))
  {
  }

  duckdb::unique_ptr<duckdb::MaterializedQueryResult> Query(const std::string& sql)
  {
    return _connection.Query(sql);
  }

 private:
  sirius::scan::internal_connection _connection;
};

}  // namespace sirius::op::scan
