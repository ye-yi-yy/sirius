/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */

#include "scan/source_policy.hpp"

#include "adapters/adapter_support.hpp"
#include "scan/source_registry.hpp"
#include "sirius_sql_rewrite.hpp"

#include <duckdb/common/exception.hpp>
#include <duckdb/execution/operator/scan/physical_table_scan.hpp>

#include <unordered_set>
#include <vector>

namespace sirius::scan {
namespace {

void inspect(source_policy& policy,
             source_registry& registry,
             const duckdb::PhysicalTableScan& scan)
{
  const binding_ref binding{registry.database(), scan.function, scan.bind_data.get()};
  const auto* adapter = registry.lookup(scan.function, scan.bind_data.get());
  source_policy_evidence evidence;
  if (adapter) {
    const auto& profile = adapter->profile();
    policy.has_unclassified_source |= !profile.implementation_verified;
    // Record the veto first: failed byte-source discovery cannot erase it.
    if (!profile.allows_cpu_replay) { policy.replay_vetoes.insert(scan.function.name); }
    evidence = adapter->inspect_source(binding);
  } else {
    policy.has_unclassified_source = true;
    // Preserve the legacy unknown-provider S3 check without treating it as D2 evidence.
    evidence = detail::inspect_legacy_multi_file_source(binding);
  }
  policy.byte_source_discovery_complete &= evidence.byte_source_discovery_complete;
  policy.reads_s3 |= evidence.reads_s3;
}

}  // namespace

bool source_policy::permits_source_replay() const noexcept
{
  return scan_discovery_complete && byte_source_discovery_complete && !reads_s3 &&
         replay_vetoes.empty();
}

void source_policy::require_source_replay(const std::string& query_sql,
                                          const std::string& error) const
{
  // Known vetoes dominate missing evidence, regardless of traversal order.
  if (reads_s3 || sirius::references_sirius_owned_s3_parquet(query_sql)) {
    throw duckdb::ExecutorException(
      "S3 CPU fallback is not supported: this query reads s3:// data, GPU execution failed, and "
      "Sirius has no CPU fallback for S3 data sources. Underlying GPU error: " +
      error);
  }
  if (!replay_vetoes.empty()) {
    std::string sources;
    for (const auto& source : replay_vetoes) {
      if (!sources.empty()) { sources += ", "; }
      sources += source;
    }
    throw duckdb::ExecutorException("CPU fallback is not supported for " + sources +
                                    ". Underlying GPU error: " + error);
  }
  if (!scan_discovery_complete || !byte_source_discovery_complete) {
    throw duckdb::ExecutorException(
      "CPU fallback is not supported: original plan source discovery is incomplete. "
      "Underlying GPU error: " +
      error);
  }
}

source_policy capture_source_policy(duckdb::DatabaseInstance& db,
                                    const duckdb::PhysicalOperator& root)
{
  source_policy policy;
  auto& registry = source_registry::get(db);
  std::unordered_set<const duckdb::PhysicalOperator*> visited;
  std::vector<const duckdb::PhysicalOperator*> pending{&root};
  while (!pending.empty()) {
    const auto* op = pending.back();
    pending.pop_back();
    if (!visited.insert(op).second) { continue; }
    try {
      if (const auto* scan = dynamic_cast<const duckdb::PhysicalTableScan*>(op)) {
        inspect(policy, registry, *scan);
      } else if (op->IsSource()) {
        policy.has_unclassified_source = true;
      }
    } catch (duckdb::InterruptException&) {
      throw;
    } catch (const std::exception&) {
      policy.byte_source_discovery_complete = false;
    }
    // Continue after a scan capture failure so a later known veto is not lost.
    try {
      for (const auto& child : op->GetChildren()) {
        pending.push_back(&child.get());
      }
    } catch (duckdb::InterruptException&) {
      throw;
    } catch (const std::exception&) {
      policy.scan_discovery_complete = false;
    }
  }
  return policy;
}

}  // namespace sirius::scan
