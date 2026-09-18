/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */

#pragma once
#include <string_view>

namespace sirius::scan {

enum class source_kind { native, parquet, owned_parquet, stream, iceberg };
enum class dynamic_filter_mode { post_decode, reader, none };
enum class byte_source_class { native_storage, file_inventory, stream };
enum class scan_runtime_form { ingestible, source_operator };

struct source_profile {
  source_kind kind;
  std::string_view id;
  dynamic_filter_mode dynamic_filters;
  byte_source_class bytes;
  bool allows_cpu_replay;
  bool selector_outside_bind;
  bool implementation_verified = true;
  scan_runtime_form runtime    = scan_runtime_form::ingestible;
  bool protected_construction  = false;
};

}  // namespace sirius::scan
