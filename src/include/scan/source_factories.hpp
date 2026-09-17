/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */

#pragma once

namespace duckdb {
class TableFunction;
/// The factory used by both Sirius registration and source verification.
TableFunction sirius_parquet_scan_function();
}  // namespace duckdb
