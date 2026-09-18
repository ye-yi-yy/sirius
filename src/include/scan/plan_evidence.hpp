/*
 * Copyright 2026, Sirius Contributors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */
#pragma once
#include "scan/binding_audit.hpp"
#include "scan/bound_read_view.hpp"

#include <memory>
#include <optional>
#include <string>
#include <vector>
namespace duckdb {
class ClientContext;
class LogicalOperator;
class PhysicalOperator;
}  // namespace duckdb
namespace sirius::scan {
enum class candidate_origin { original_copy_chain, sql_replan, direct };
enum class comparison_verdict { equal, mismatch, unproven };
enum class correspondence_mode { table_index, single, empty, none, direct };
struct source_occurrence {
  duckdb::idx_t table_index;
  std::string function;
  read_view_capture capture;
  bool compatibility;
  bool requires_selector;
  bool allows_cpu_replay;
  std::optional<std::string> selector;
};
struct plan_evidence {
  std::uint64_t instance;
  std::uint64_t generation;
  bool complete = true;
  duckdb::vector<duckdb::LogicalType> output_types;
  std::vector<source_occurrence> sources;
};
struct comparison_result {
  comparison_verdict verdict = comparison_verdict::unproven;
  correspondence_mode mode   = correspondence_mode::none;
  // An explicit staging exception, never a positive identity/safety verdict.
  bool compatibility = false;
  std::string reason;
  void require_match() const;
};
struct original_plan_evidence {
  planning_repeat_audit audit;
  std::shared_ptr<const plan_evidence> hook;
  std::shared_ptr<const plan_evidence> physical;
};
std::shared_ptr<const plan_evidence> capture_logical_plan(duckdb::ClientContext&,
                                                          const duckdb::LogicalOperator&,
                                                          std::uint64_t,
                                                          capture_origin);
std::shared_ptr<const plan_evidence> capture_physical_plan(duckdb::ClientContext&,
                                                           const duckdb::PhysicalOperator&,
                                                           std::uint64_t);
comparison_result compare_candidate(const original_plan_evidence&,
                                    const plan_evidence&,
                                    candidate_origin);
const char* comparison_name(comparison_verdict) noexcept;
const char* correspondence_name(correspondence_mode) noexcept;
}  // namespace sirius::scan
