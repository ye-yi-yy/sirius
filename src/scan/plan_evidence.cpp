/*
 * Copyright 2026, Sirius Contributors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */
#include "scan/plan_evidence.hpp"

#include "scan/source_registry.hpp"

#include <duckdb/common/exception.hpp>
#include <duckdb/execution/operator/scan/physical_table_scan.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/planner/operator/logical_get.hpp>

#include <algorithm>
#include <map>
#include <unordered_set>

namespace sirius::scan {
namespace {
source_occurrence capture(source_registry& registry,
                          std::uint64_t generation,
                          capture_origin origin,
                          duckdb::idx_t index,
                          const duckdb::TableFunction& function,
                          const duckdb::FunctionData* data,
                          const duckdb::vector<std::string>& names,
                          const duckdb::vector<duckdb::LogicalType>& types)
{
  const auto* adapter = registry.lookup(function, data);
  return {index,
          function.name,
          capture_read_view(registry, generation, origin, function, data, names, types),
          adapter && !adapter->profile().implementation_verified,
          adapter && adapter->profile().selector_outside_bind,
          adapter && adapter->profile().allows_cpu_replay,
          std::nullopt};
}

std::string key(const source_occurrence& source)
{
  if (source.capture.view()) { return "verified:" + source.capture.view()->canonical_identity; }
  // Counts compatibility occurrences without pretending their inputs have been compared.
  return "compatibility:" + source.function;
}

bool available(const plan_evidence& plan)
{
  return plan.complete && std::all_of(plan.sources.begin(), plan.sources.end(), [](const auto& s) {
           return s.capture.status() == capture_status::complete || s.compatibility;
         });
}

bool has_compatibility(const plan_evidence& plan)
{
  return std::any_of(
    plan.sources.begin(), plan.sources.end(), [](const auto& s) { return s.compatibility; });
}

bool same_view(const source_occurrence& a, const source_occurrence& b)
{
  if (a.compatibility || b.compatibility) {
    return a.compatibility && b.compatibility && a.function == b.function;
  }
  return a.capture.view() && b.capture.view() &&
         a.capture.view()->canonical_identity == b.capture.view()->canonical_identity;
}
}  // namespace

std::shared_ptr<const plan_evidence> capture_logical_plan(duckdb::ClientContext& context,
                                                          const duckdb::LogicalOperator& root,
                                                          std::uint64_t generation,
                                                          capture_origin origin)
{
  auto& registry       = source_registry::get(*context.db);
  auto result          = std::make_shared<plan_evidence>();
  result->instance     = registry.instance_id();
  result->generation   = generation;
  result->output_types = root.types;
  std::vector<const duckdb::LogicalOperator*> pending{&root};
  std::unordered_set<const duckdb::LogicalOperator*> seen;
  while (!pending.empty()) {
    const auto* node = pending.back();
    pending.pop_back();
    if (!seen.insert(node).second) { continue; }
    if (node->type == duckdb::LogicalOperatorType::LOGICAL_GET) {
      const auto& get = node->Cast<duckdb::LogicalGet>();
      if (origin == capture_origin::candidate) {
        registry.require(get.function, get.bind_data.get());
      }
      auto source = capture(registry,
                            generation,
                            origin,
                            get.table_index,
                            get.function,
                            get.bind_data.get(),
                            get.names,
                            get.returned_types);
      if (source.requires_selector) {
        source.selector = capture_selector(get.parameters, get.named_parameters);
      }
      result->sources.push_back(std::move(source));
    }
    for (const auto& child : node->children) {
      pending.push_back(child.get());
    }
  }
  return result;
}

std::shared_ptr<const plan_evidence> capture_physical_plan(duckdb::ClientContext& context,
                                                           const duckdb::PhysicalOperator& root,
                                                           std::uint64_t generation)
{
  auto& registry       = source_registry::get(*context.db);
  auto result          = std::make_shared<plan_evidence>();
  result->instance     = registry.instance_id();
  result->generation   = generation;
  result->output_types = root.types;
  std::vector<const duckdb::PhysicalOperator*> pending{&root};
  std::unordered_set<const duckdb::PhysicalOperator*> seen;
  while (!pending.empty()) {
    const auto* node = pending.back();
    pending.pop_back();
    if (!seen.insert(node).second) { continue; }
    if (node->type == duckdb::PhysicalOperatorType::TABLE_SCAN) {
      const auto& get = node->Cast<duckdb::PhysicalTableScan>();
      result->sources.push_back(capture(registry,
                                        generation,
                                        capture_origin::physical_original,
                                        duckdb::DConstants::INVALID_INDEX,
                                        get.function,
                                        get.bind_data.get(),
                                        get.names,
                                        get.returned_types));
    }
    for (const auto& child : node->GetChildren()) {
      pending.push_back(&child.get());
    }
  }
  return result;
}

comparison_result compare_candidate(const original_plan_evidence& original,
                                    const plan_evidence& candidate,
                                    candidate_origin origin)
{
  auto refuse = [](comparison_verdict verdict, const char* reason) {
    return comparison_result{verdict, correspondence_mode::none, false, reason};
  };
  if (!original.physical || original.physical->instance != candidate.instance ||
      original.physical->generation != candidate.generation) {
    return refuse(comparison_verdict::unproven, "original_generation_unproven");
  }
  const auto& physical = *original.physical;
  if (physical.output_types != candidate.output_types) {
    return refuse(comparison_verdict::mismatch, "output_schema_mismatch");
  }
  if (!available(candidate) || !available(physical)) {
    return refuse(comparison_verdict::unproven, "capture_incomplete");
  }
  const bool compatibility = has_compatibility(candidate) || has_compatibility(physical);
  std::map<std::string, std::int64_t> multiplicities;
  for (const auto& source : physical.sources) {
    ++multiplicities[key(source)];
  }
  for (const auto& source : candidate.sources) {
    --multiplicities[key(source)];
  }
  for (const auto& entry : multiplicities) {
    if (entry.second) { return refuse(comparison_verdict::mismatch, "read_view_mismatch"); }
  }

  auto mode = correspondence_mode::empty;
  if (!candidate.sources.empty()) {
    if (origin == candidate_origin::original_copy_chain) {
      if (!original.hook || !available(*original.hook) ||
          original.hook->instance != candidate.instance ||
          original.hook->generation != candidate.generation ||
          original.hook->sources.size() != candidate.sources.size()) {
        return refuse(comparison_verdict::unproven, "no_correspondence");
      }
      std::map<duckdb::idx_t, const source_occurrence*> partners;
      for (const auto& source : original.hook->sources) {
        if (!partners.emplace(source.table_index, &source).second) {
          return refuse(comparison_verdict::unproven, "no_correspondence");
        }
      }
      for (const auto& source : candidate.sources) {
        const auto it = partners.find(source.table_index);
        if (it == partners.end()) {
          return refuse(comparison_verdict::unproven, "no_correspondence");
        }
        if (!same_view(source, *it->second)) {
          return refuse(comparison_verdict::mismatch, "read_view_mismatch");
        }
        if (source.requires_selector && !source.compatibility &&
            (!source.selector || !it->second->selector ||
             source.selector != it->second->selector)) {
          return refuse(comparison_verdict::unproven, "selector_unproven");
        }
        partners.erase(it);
      }
      mode = correspondence_mode::table_index;
    } else {
      if (candidate.sources.size() != 1) {
        // TODO(R1 D1/D2/D3): remove this explicitly requested compatibility exception
        // when the actual Parquet/Iceberg bridges can support full admission.
        if (!compatibility) { return refuse(comparison_verdict::unproven, "no_correspondence"); }
        mode = correspondence_mode::none;
      } else {
        mode               = correspondence_mode::single;
        const auto& source = candidate.sources.front();
        if (source.requires_selector && !source.compatibility &&
            (!original.hook || original.hook->instance != candidate.instance ||
             original.hook->generation != candidate.generation ||
             original.hook->sources.size() != 1 ||
             !same_view(source, original.hook->sources.front()) || !source.selector ||
             !original.hook->sources.front().selector ||
             source.selector != original.hook->sources.front().selector)) {
          return refuse(comparison_verdict::unproven, "selector_unproven");
        }
      }
    }
  }
  return {compatibility ? comparison_verdict::unproven : comparison_verdict::equal,
          mode,
          compatibility,
          compatibility ? "provider_bridge_deferred" : ""};
}

void comparison_result::require_match() const
{
  if (verdict != comparison_verdict::equal && !compatibility) {
    throw duckdb::NotImplementedException("Sirius scan contract: %s", reason);
  }
}
const char* comparison_name(comparison_verdict value) noexcept
{
  switch (value) {
    case comparison_verdict::equal: return "equal";
    case comparison_verdict::mismatch: return "mismatch";
    default: return "unproven";
  }
}
const char* correspondence_name(correspondence_mode value) noexcept
{
  switch (value) {
    case correspondence_mode::table_index: return "table_index";
    case correspondence_mode::single: return "single";
    case correspondence_mode::empty: return "empty";
    case correspondence_mode::direct: return "direct";
    default: return "none";
  }
}
}  // namespace sirius::scan
