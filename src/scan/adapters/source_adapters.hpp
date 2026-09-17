/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */

#pragma once
#include "scan/source_adapter.hpp"

namespace sirius::scan {
std::unique_ptr<scan_source_adapter> make_native_source_adapter();
std::unique_ptr<scan_source_adapter> make_parquet_source_adapter();
std::unique_ptr<scan_source_adapter> make_owned_parquet_source_adapter();
std::unique_ptr<scan_source_adapter> make_stream_source_adapter();
std::unique_ptr<scan_source_adapter> make_iceberg_source_adapter();
}  // namespace sirius::scan
