/*
 * Copyright 2026, Sirius Contributors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */
#pragma once
#include "scan/scan_contract.hpp"

#include <cudf/io/parquet.hpp>
#include <cudf/io/parquet_schema.hpp>

#include <memory>
#include <vector>
namespace sirius::io {
class sirius_datasource;
}
namespace sirius::op::scan {
struct scan_plan;
struct duckdb_row_group_metadata;
}  // namespace sirius::op::scan
namespace sirius::scan {
// File occurrences retain multiplicity, including duplicate paths. This is the runtime's
// inventory; for compatibility providers it is deliberately not D2/original-binding evidence.
struct parquet_file_certificate {
  bound_table_scan_ptr consumer;
  std::shared_ptr<const std::vector<std::string>> inventory;
  std::size_t occurrence;
  std::shared_ptr<const cudf::io::parquet::FileMetaData> footer;
  std::vector<cudf::size_type> allowed_row_groups;
  std::shared_ptr<cudf::io::parquet_reader_options> options;
  std::shared_ptr<const op::scan::scan_plan> plan;
  std::shared_ptr<const void> visibility;
};
struct parquet_slice_certificate {
  std::shared_ptr<const parquet_file_certificate> file;
  std::vector<cudf::size_type> row_groups;
  std::shared_ptr<io::sirius_datasource> datasource;
};
struct native_slice_certificate {
  bound_table_scan_ptr consumer;
  std::shared_ptr<native_checkpoint_lease> lease;
  duckdb::shared_ptr<duckdb::DataTable> storage;
  std::shared_ptr<const op::scan::duckdb_row_group_metadata> layout;
  std::shared_ptr<io::sirius_datasource> datasource;
  std::vector<std::shared_ptr<void>> staging;
};
}  // namespace sirius::scan
