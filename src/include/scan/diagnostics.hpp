/*
 * Copyright 2026, Sirius Contributors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>

namespace duckdb {
class ClientContext;
}
namespace sirius::scan {

struct contract_counters {
  std::atomic<std::uint64_t> read_view_mismatches{0};
  std::atomic<std::uint64_t> certificate_mismatches{0};
  std::atomic<std::uint64_t> checkpoint_revalidation_failures{0};
  std::atomic<std::uint64_t> scan_capture_incomplete{0};
  std::atomic<std::uint64_t> scan_planning_safety_declines{0};
  std::atomic<std::uint64_t> scan_source_verification_declines{0};
};

enum class scan_refusal_reason {
  source_identity_unverified,
  planning_repeat_unsafe,
  original_capture_incomplete,
  capture_incomplete,
  original_generation_unproven,
  output_schema_mismatch,
  read_view_mismatch,
  no_correspondence,
  selector_unproven
};
const char* refusal_name(scan_refusal_reason) noexcept;

class scan_attempt_diagnostics {
 public:
  scan_attempt_diagnostics(std::shared_ptr<contract_counters>,
                           std::uint64_t instance,
                           std::uint64_t generation);
  void capture_incomplete();
  void refuse(scan_refusal_reason,
              std::uint64_t original_hash  = 0,
              std::uint64_t candidate_hash = 0,
              std::uint64_t differences    = 0);

 private:
  void count_once(unsigned flag, std::atomic<std::uint64_t>& counter);
  const std::shared_ptr<contract_counters> _counters;
  const std::uint64_t _instance;
  const std::uint64_t _generation;
  unsigned _counted = 0;
  bool _reported    = false;
};

std::shared_ptr<scan_attempt_diagnostics> diagnostics_for(duckdb::ClientContext&);
void begin_scan_diagnostic_attempt(duckdb::ClientContext&);

}  // namespace sirius::scan
