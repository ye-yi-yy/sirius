/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */
#include "scan/binding_audit.hpp"

#include "sirius_context.hpp"

namespace sirius::scan {

planning_repeat_audit capture_planning_repeat_audit(duckdb::ClientContext& context)
{
  planning_repeat_audit result;
  if (auto connection = duckdb::get_sirius_connection_state(context)) {
    result.generation = connection->planning_generation();
  }
  // TODO(R1, DuckDB D3): consume supported observations before scalar/aggregate/cast binding
  // and table-argument evaluation, with original statement generations and occurrence tokens.
  // Then qualify repeat-bind, speculative-execution and CPU-replay verdicts independently.
  // Until that bridge exists, preserve legacy planning and explicitly report unavailable
  // observations/unproven verdicts. Do not re-evaluate selectors or infer safety from a plan.
  return result;
}

}  // namespace sirius::scan
