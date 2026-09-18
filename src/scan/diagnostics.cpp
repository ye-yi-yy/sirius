/*
 * Copyright 2026, Sirius Contributors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */
#include "scan/diagnostics.hpp"

#include "log/logging.hpp"
#include "scan/source_registry.hpp"
#include "sirius_context.hpp"

namespace sirius::scan {

const char* refusal_name(scan_refusal_reason reason) noexcept
{
  switch (reason) {
    case scan_refusal_reason::source_identity_unverified: return "source_identity_unverified";
    case scan_refusal_reason::planning_repeat_unsafe: return "planning_repeat_unsafe";
    case scan_refusal_reason::original_capture_incomplete: return "original_capture_incomplete";
    case scan_refusal_reason::capture_incomplete: return "capture_incomplete";
    case scan_refusal_reason::original_generation_unproven: return "original_generation_unproven";
    case scan_refusal_reason::output_schema_mismatch: return "output_schema_mismatch";
    case scan_refusal_reason::read_view_mismatch: return "read_view_mismatch";
    case scan_refusal_reason::no_correspondence: return "no_correspondence";
    case scan_refusal_reason::selector_unproven: return "selector_unproven";
  }
  return "unknown";
}

scan_attempt_diagnostics::scan_attempt_diagnostics(std::shared_ptr<contract_counters> counters,
                                                   std::uint64_t instance,
                                                   std::uint64_t generation)
  : _counters(std::move(counters)), _instance(instance), _generation(generation)
{
}

void scan_attempt_diagnostics::count_once(unsigned flag, std::atomic<std::uint64_t>& counter)
{
  if ((_counted & flag) == 0) {
    _counted |= flag;
    counter.fetch_add(1, std::memory_order_relaxed);
  }
}

void scan_attempt_diagnostics::capture_incomplete()
{
  count_once(1, _counters->scan_capture_incomplete);
}

void scan_attempt_diagnostics::refuse(scan_refusal_reason reason,
                                      std::uint64_t original_hash,
                                      std::uint64_t candidate_hash,
                                      std::uint64_t differences)
{
  switch (reason) {
    case scan_refusal_reason::source_identity_unverified:
      count_once(2, _counters->scan_source_verification_declines);
      break;
    case scan_refusal_reason::planning_repeat_unsafe:
      count_once(4, _counters->scan_planning_safety_declines);
      break;
    default:
      count_once(8, _counters->read_view_mismatches);
      if (reason == scan_refusal_reason::capture_incomplete ||
          reason == scan_refusal_reason::original_capture_incomplete) {
        capture_incomplete();
      }
      break;
  }
  if (_reported) { return; }
  _reported = true;
  SIRIUS_LOG_INFO(
    "Scan contract refused: reason={} instance={} generation={} original_hash={} "
    "candidate_hash={} differences={}",
    refusal_name(reason),
    _instance,
    _generation,
    original_hash,
    candidate_hash,
    differences);
}

std::shared_ptr<scan_attempt_diagnostics> diagnostics_for(duckdb::ClientContext& context)
{
  const auto connection = duckdb::get_sirius_connection_state(context);
  if (connection && connection->scan_diagnostics) { return connection->scan_diagnostics; }
  auto& registry   = source_registry::get(*context.db);
  auto diagnostics = std::make_shared<scan_attempt_diagnostics>(
    registry.counters,
    registry.instance_id(),
    connection && connection->planning_generation() ? connection->planning_generation() : 1);
  if (connection) { connection->scan_diagnostics = diagnostics; }
  return diagnostics;
}

void begin_scan_diagnostic_attempt(duckdb::ClientContext& context)
{
  if (auto connection = duckdb::get_sirius_connection_state(context)) {
    connection->scan_diagnostics.reset();
  }
}

}  // namespace sirius::scan
