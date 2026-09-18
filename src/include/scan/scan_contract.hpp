/*
 * Copyright 2026, Sirius Contributors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */
#pragma once
#include "helper/logical_type.hpp"
#include "scan/plan_evidence.hpp"

#include <duckdb/common/column_index.hpp>
#include <duckdb/common/shared_ptr.hpp>
#include <duckdb/planner/table_filter.hpp>

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <vector>
namespace duckdb {
class AttachedDatabase;
class DataTable;
class SingleFileBlockManager;
class StorageLockKey;
class SiriusConnectionState;
}  // namespace duckdb
namespace sirius::scan {
struct contract_counters {
  std::atomic<std::uint64_t> read_view_mismatches{0};
  std::atomic<std::uint64_t> certificate_mismatches{0};
  std::atomic<std::uint64_t> checkpoint_revalidation_failures{0};
};
class native_checkpoint_lease {
 public:
  native_checkpoint_lease(duckdb::DataTable&, std::uint64_t, std::shared_ptr<contract_counters>);
  ~native_checkpoint_lease();
  void validate(const duckdb::DataTable&) const;
  void release() noexcept;
  void validate() const;
  const duckdb::SingleFileBlockManager* block_manager() const noexcept { return _block_manager; }
  std::uint64_t iteration() const noexcept { return _iteration; }
  std::uint64_t epoch() const noexcept { return _epoch; }

 private:
  duckdb::shared_ptr<duckdb::AttachedDatabase> _database;
  duckdb::unique_ptr<duckdb::StorageLockKey> _lock;
  const duckdb::SingleFileBlockManager* _block_manager;
  const std::uint64_t _iteration;
  const std::uint64_t _epoch;
  std::shared_ptr<contract_counters> _counters;
};

// These fields are ownership snapshots, not a second equality format. Dynamic filters stay out.
struct consumer_requirements {
  std::vector<sirius::logical_type> logical_output;
  std::vector<std::pair<int, int>> physical_output;
  duckdb::vector<duckdb::ColumnIndex> required_columns;
  duckdb::vector<duckdb::idx_t> projection;
  std::shared_ptr<const duckdb::TableFilterSet> static_filters;
  std::string materializer;
};
class query_scan_registry;
struct registry_membership;
struct bound_table_scan {
  const std::uint64_t handle;
  const std::uint64_t instance;
  const std::uint64_t generation;
  const std::uint64_t window;
  const source_occurrence source;
  const consumer_requirements requirements;
  const std::weak_ptr<registry_membership> membership;
  const std::shared_ptr<contract_counters> counters;
  void validate() const;
};
using bound_table_scan_ptr = std::shared_ptr<const bound_table_scan>;

struct registry_membership {
  std::atomic<bool> active{false};
  std::map<std::uint64_t, std::weak_ptr<const bound_table_scan>> entries;
  std::shared_ptr<contract_counters> counters;
};

// Runtime windows own this registry. Reusable transparent operators retain only original evidence.
class query_scan_registry {
 public:
  query_scan_registry(std::shared_ptr<const plan_evidence>,
                      comparison_result,
                      std::shared_ptr<contract_counters>,
                      planning_repeat_audit);
  ~query_scan_registry();
  void declare_native(duckdb::ClientContext&, duckdb::DataTable&);
  void seal_and_acquire(duckdb::ClientContext&);
  std::shared_ptr<native_checkpoint_lease> lease_for(duckdb::DataTable&) const;
  bound_table_scan_ptr add(duckdb::idx_t, consumer_requirements);
  void freeze();
  void close() noexcept;
  void validate() const;
  const comparison_result& comparison() const noexcept { return _comparison; }
  bool admission_supported() const noexcept
  {
    return _membership->active.load(std::memory_order_acquire) &&
           _comparison.verdict == comparison_verdict::equal && _audit.observations_available &&
           _audit.repeat_bind == audit_verdict::safe &&
           _audit.speculative_execution == audit_verdict::safe &&
           _audit.cpu_replay == audit_verdict::safe;
  }
  std::uint64_t window() const noexcept { return _window; }

 private:
  const std::shared_ptr<const plan_evidence> _candidate;
  const comparison_result _comparison;
  const planning_repeat_audit _audit;
  const std::uint64_t _window;
  std::shared_ptr<registry_membership> _membership;
  std::vector<bound_table_scan_ptr> _tickets;
  std::map<duckdb::idx_t, duckdb::shared_ptr<duckdb::DataTable>> _native;
  std::map<duckdb::idx_t, std::shared_ptr<native_checkpoint_lease>> _leases;
  duckdb::shared_ptr<duckdb::SiriusConnectionState> _connection;
  bool _sealed = false;
};

void certificate_failure(const bound_table_scan_ptr&, const char* reason);
void validate_consumer(const bound_table_scan_ptr& expected, const bound_table_scan_ptr& actual);
std::string contract_summary(const bound_table_scan&, const comparison_result&);
}  // namespace sirius::scan
