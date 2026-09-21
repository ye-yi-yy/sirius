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

#pragma once

#include "op/scan/table_scan/scan_contract.hpp"

#include <optional>
#include <unordered_map>
#include <vector>

namespace sirius::transparent {

struct read_view_registry_entry {
  op::scan::bound_table_scan contract;
  op::scan::eligibility_certificate eligibility;
  std::optional<uint64_t> window_id;
  uint64_t finalize_generation = 0;
};

class read_view_registry {
 public:
  [[nodiscard]] read_view_registry_entry const& entry(op::scan::scan_contract_id id) const;
  [[nodiscard]] read_view_registry_entry const& entry_for_scan_node(uint64_t scan_node_id) const;
  [[nodiscard]] std::vector<read_view_registry_entry> const& entries() const noexcept
  {
    return entries_;
  }

 private:
  friend op::scan::scan_contract_id op::scan::allocate_scan_contract(
    read_view_registry&,
    std::optional<uint64_t>,
    uint64_t,
    uint64_t,
    std::shared_ptr<op::scan::bound_read_view const>,
    op::scan::column_requirements,
    op::scan::predicate_contract,
    op::scan::materializer_contract_identity,
    duckdb::vector<duckdb::LogicalType>);

  std::vector<read_view_registry_entry> entries_;
  std::unordered_map<op::scan::scan_contract_id, std::size_t> by_contract_id_;
  std::unordered_map<uint64_t, std::size_t> by_scan_node_id_;
};

}  // namespace sirius::transparent
