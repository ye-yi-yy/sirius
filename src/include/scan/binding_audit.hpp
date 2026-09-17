/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */
#pragma once

#include <cstdint>

namespace duckdb {
class ClientContext;
}  // namespace duckdb

namespace sirius::scan {

enum class audit_verdict { safe, unsafe, unproven };

struct planning_repeat_audit {
  std::uint64_t generation            = 0;
  audit_verdict repeat_bind           = audit_verdict::unproven;
  audit_verdict speculative_execution = audit_verdict::unproven;
  audit_verdict cpu_replay            = audit_verdict::unproven;
  bool observations_available         = false;

  [[nodiscard]] bool observed_unsafe() const noexcept
  {
    return repeat_bind == audit_verdict::unsafe || speculative_execution == audit_verdict::unsafe ||
           cpu_replay == audit_verdict::unsafe;
  }
};

/// Compatibility adapter for the unmodified DuckDB dependency. Missing original-binding
/// observations remain unproven; neither an optimized plan nor a rebind can supply them.
[[nodiscard]] planning_repeat_audit capture_planning_repeat_audit(duckdb::ClientContext& context);

}  // namespace sirius::scan
