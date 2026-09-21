/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "transparent/read_view_registry.hpp"

#include "op/scan/gpu_ingestible_types.hpp"

#include <atomic>
#include <limits>
#include <stdexcept>

namespace sirius::transparent {

read_view_registry_entry const& read_view_registry::entry(op::scan::scan_contract_id id) const
{
  auto const found = by_contract_id_.find(id);
  if (found == by_contract_id_.end()) {
    throw std::runtime_error("unknown scan contract id " + std::to_string(id));
  }
  return entries_.at(found->second);
}

read_view_registry_entry const& read_view_registry::entry_for_scan_node(uint64_t scan_node_id) const
{
  auto const found = by_scan_node_id_.find(scan_node_id);
  if (found == by_scan_node_id_.end()) {
    throw std::runtime_error("unknown scan node id " + std::to_string(scan_node_id));
  }
  return entries_.at(found->second);
}

}  // namespace sirius::transparent

namespace sirius::op::scan {
namespace {
std::atomic<scan_contract_id> next_contract_id{1};

scan_contract_id reserve_contract_id()
{
  auto current = next_contract_id.load(std::memory_order_relaxed);
  for (;;) {
    if (current == 0) { throw std::overflow_error("scan contract id space exhausted"); }
    auto const next = current == std::numeric_limits<scan_contract_id>::max() ? 0 : current + 1;
    if (next_contract_id.compare_exchange_weak(
          current, next, std::memory_order_relaxed, std::memory_order_relaxed)) {
      return current;
    }
  }
}
}  // namespace

scan_contract_id allocate_scan_contract(transparent::read_view_registry& registry,
                                        std::optional<uint64_t> window_id,
                                        uint64_t finalize_generation,
                                        uint64_t scan_node_id,
                                        std::shared_ptr<bound_read_view const> view,
                                        column_requirements columns,
                                        predicate_contract predicates,
                                        materializer_contract_identity materializer,
                                        duckdb::vector<duckdb::LogicalType> output_types)
{
  if (registry.by_scan_node_id_.contains(scan_node_id)) {
    throw std::runtime_error("duplicate scan node id " + std::to_string(scan_node_id));
  }
  auto const id = reserve_contract_id();

  bound_table_scan contract;
  contract.scan_node_id = scan_node_id;
  contract.view         = std::move(view);
  contract.output_types = std::move(output_types);
  contract.columns      = std::move(columns);
  contract.predicates   = std::move(predicates);
  contract.materializer = std::move(materializer);
  contract.contract_id  = id;

  eligibility_certificate eligibility;
  eligibility.contract_id  = id;
  eligibility.depth        = contract.view ? contract.view->depth : evidence_depth::path;
  eligibility.materializer = contract.materializer;
  switch (contract.materializer.kind) {
    case materializer_kind::duckdb_native: eligibility.later_checks = {"segments_per_range"}; break;
    case materializer_kind::parquet:
    case materializer_kind::iceberg: eligibility.later_checks = {"footer_per_file"}; break;
    case materializer_kind::stream: eligibility.later_checks = {}; break;
  }

  auto const index = registry.entries_.size();
  registry.entries_.push_back(
    {std::move(contract), std::move(eligibility), window_id, finalize_generation});
  registry.by_contract_id_.emplace(id, index);
  registry.by_scan_node_id_.emplace(scan_node_id, index);
  return id;
}

bound_table_scan const& contract_of(transparent::read_view_registry const& registry,
                                    scan_contract_id contract_id)
{
  return registry.entry(contract_id).contract;
}

void validate_split_contract(scan_contract_id expected, scan_info const& split)
{
  if (split.contract_id() != expected) {
    throw std::runtime_error("scan split contract mismatch: expected " + std::to_string(expected) +
                             ", got " + std::to_string(split.contract_id()));
  }
  if (split.certificates().size() != split.dependencies().size()) {
    throw std::runtime_error("scan split contract has non-parallel certificates and dependencies");
  }
  for (auto const& certificate : split.certificates()) {
    if (certificate.contract_id != expected) {
      throw std::runtime_error("scan split certificate contract mismatch: expected " +
                               std::to_string(expected) + ", got " +
                               std::to_string(certificate.contract_id));
    }
  }
}

}  // namespace sirius::op::scan
