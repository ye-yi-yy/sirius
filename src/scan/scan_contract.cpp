/*
 * Copyright 2026, Sirius Contributors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */
#include "scan/scan_contract.hpp"

#include <duckdb/common/exception.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/single_file_block_manager.hpp>
#include <duckdb/storage/storage_lock.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>

#include <limits>
#include <set>

namespace sirius::scan {
namespace {
std::atomic<std::uint64_t> next_handle{1};
std::uint64_t allocate_handle()
{
  auto value = next_handle.load(std::memory_order_relaxed);
  do {
    if (value == std::numeric_limits<std::uint64_t>::max()) {
      throw duckdb::ExecutorException("Sirius scan handle space exhausted");
    }
  } while (!next_handle.compare_exchange_weak(value, value + 1, std::memory_order_relaxed));
  return value;
}
const duckdb::SingleFileBlockManager* single_file(duckdb::AttachedDatabase& db)
{
  return dynamic_cast<const duckdb::SingleFileBlockManager*>(
    &db.GetStorageManager().GetBlockManager());
}
}  // namespace

native_checkpoint_lease::native_checkpoint_lease(duckdb::DataTable& table,
                                                 std::shared_ptr<contract_counters> counters)
  : _database(table.GetAttached().shared_from_this()),
    _lock(duckdb::DuckTransactionManager::Get(*_database).SharedCheckpointLock()),
    _block_manager(single_file(*_database)),
    _iteration(_block_manager ? _block_manager->GetCheckpointIteration() : 0),
    _epoch(allocate_handle()),
    _counters(std::move(counters))
{
  if (!_block_manager) {
    throw duckdb::NotImplementedException("Native scan requires a single-file block manager");
  }
}
native_checkpoint_lease::~native_checkpoint_lease() = default;
void native_checkpoint_lease::release() noexcept { _lock.reset(); }
void native_checkpoint_lease::validate() const
{
  if (!_lock || single_file(*_database) != _block_manager ||
      _block_manager->GetCheckpointIteration() != _iteration) {
    _counters->checkpoint_revalidation_failures.fetch_add(1, std::memory_order_relaxed);
    throw duckdb::ExecutorException("Sirius scan contract: checkpoint_revalidation_failed");
  }
}
void native_checkpoint_lease::validate(const duckdb::DataTable& table) const
{
  if (&const_cast<duckdb::DataTable&>(table).GetAttached() != _database.get()) {
    _counters->checkpoint_revalidation_failures.fetch_add(1, std::memory_order_relaxed);
    throw duckdb::ExecutorException("Sirius scan contract: checkpoint_database_mismatch");
  }
  validate();
}

query_scan_registry::query_scan_registry(std::shared_ptr<const plan_evidence> candidate,
                                         comparison_result comparison,
                                         std::shared_ptr<contract_counters> counters,
                                         planning_repeat_audit audit,
                                         candidate_origin origin)
  : _candidate(std::move(candidate)),
    _comparison(std::move(comparison)),
    _audit(audit),
    _origin(origin),
    _window(allocate_handle()),
    _membership(std::make_shared<registry_membership>())
{
  _membership->counters = std::move(counters);
}
query_scan_registry::~query_scan_registry() { close(); }
void query_scan_registry::declare_native(duckdb::ClientContext& context, duckdb::DataTable& table)
{
  if (_sealed) { throw duckdb::ExecutorException("Sirius scan construction is sealed"); }
  const auto database_id = table.GetAttached().oid;
  if (_native.contains(database_id)) { return; }
  struct storage_owner {
    duckdb::shared_ptr<duckdb::AttachedDatabase> database;
    duckdb::shared_ptr<duckdb::DataTable> table;
  };
  // DataTable borrows its database; retain both without acquiring a checkpoint key.
  auto owner = duckdb::make_shared_ptr<storage_owner>(
    storage_owner{table.GetAttached().shared_from_this(), table.shared_from_this()});
  _native.emplace(
    database_id,
    native_scan_resource{&context, duckdb::shared_ptr<duckdb::DataTable>(owner, &table)});
}
void query_scan_registry::seal()
{
  if (_sealed) { throw duckdb::InternalException("Sirius scan construction sealed twice"); }
  _sealed = true;
}
void query_scan_registry::activate()
{
  if (!_frozen || _closed || _membership->active.load(std::memory_order_acquire)) {
    throw duckdb::ExecutorException("Sirius scan contract: invalid window activation");
  }
  _membership->active.store(true, std::memory_order_release);
}
bound_table_scan_ptr query_scan_registry::add(duckdb::idx_t index,
                                              consumer_requirements requirements)
{
  if (_frozen || _closed) {
    throw duckdb::ExecutorException("Sirius scan contract registry is frozen");
  }
  const source_occurrence* source = nullptr;
  for (const auto& entry : _candidate->sources) {
    if (entry.table_index == index) {
      if (source) {
        throw duckdb::NotImplementedException("Sirius scan contract: no_correspondence");
      }
      source = &entry;
    }
  }
  if (!source) { throw duckdb::InternalException("Sirius scan binding missing from candidate"); }
  for (const auto& entry : _tickets) {
    if (entry->source.table_index == index) {
      throw duckdb::NotImplementedException("Sirius scan contract: duplicate_consumer");
    }
  }
  auto ticket = std::make_shared<const bound_table_scan>(bound_table_scan{allocate_handle(),
                                                                          _candidate->instance,
                                                                          _candidate->generation,
                                                                          _window,
                                                                          *source,
                                                                          std::move(requirements),
                                                                          _membership,
                                                                          _membership->counters});
  _membership->entries.emplace(ticket->handle, ticket);
  _tickets.push_back(ticket);
  return ticket;
}
void query_scan_registry::freeze()
{
  if (!_sealed || _frozen || _closed) {
    throw duckdb::InternalException("Sirius scan contract: invalid registry freeze");
  }
  if (_tickets.size() != _candidate->sources.size()) {
    throw duckdb::NotImplementedException("Sirius scan contract: incomplete_consumer_registry");
  }
  _frozen = true;
}
void query_scan_registry::close() noexcept
{
  _membership->active.store(false, std::memory_order_release);
  _closed = true;
}
void query_scan_registry::validate() const
{
  if (!_membership->active.load(std::memory_order_acquire)) {
    throw duckdb::ExecutorException("Sirius scan contract: inactive_window");
  }
}
void bound_table_scan::validate() const
{
  auto registry = membership.lock();
  if (!registry || !registry->active.load(std::memory_order_acquire)) {
    counters->certificate_mismatches.fetch_add(1, std::memory_order_relaxed);
    throw duckdb::ExecutorException("Sirius scan contract: inactive_window");
  }
  auto it = registry->entries.find(handle);
  if (it == registry->entries.end() || it->second.lock().get() != this) {
    counters->certificate_mismatches.fetch_add(1, std::memory_order_relaxed);
    throw duckdb::ExecutorException("Sirius scan contract: unknown_consumer");
  }
}
void certificate_failure(const bound_table_scan_ptr& expected, const char* reason)
{
  if (expected) {
    expected->counters->certificate_mismatches.fetch_add(1, std::memory_order_relaxed);
  }
  throw duckdb::ExecutorException("Sirius scan contract: certificate_mismatch (%s)", reason);
}
void validate_consumer(const bound_table_scan_ptr& expected, const bound_table_scan_ptr& actual)
{
  if (!expected || !actual || expected.get() != actual.get()) {
    certificate_failure(expected, "consumer");
  }
  expected->validate();
}
namespace {
const char* kind_name(const source_profile* profile)
{
  if (!profile) { return "unknown"; }
  switch (profile->kind) {
    case source_kind::native: return "native";
    case source_kind::parquet: return "parquet";
    case source_kind::owned_parquet: return "owned_parquet";
    case source_kind::stream: return "stream";
    case source_kind::iceberg: return "iceberg";
  }
  return "unknown";
}
const char* audit_name(audit_verdict verdict)
{
  switch (verdict) {
    case audit_verdict::safe: return "safe";
    case audit_verdict::unsafe: return "unsafe";
    case audit_verdict::unproven: return "unproven";
  }
  return "unproven";
}
const char* origin_name(candidate_origin origin)
{
  switch (origin) {
    case candidate_origin::original_copy_chain: return "original_copy_chain";
    case candidate_origin::sql_replan: return "sql_replan";
    case candidate_origin::direct: return "direct";
  }
  return "unknown";
}
}  // namespace

std::string contract_summary(const bound_table_scan& ticket,
                             const comparison_result& comparison,
                             const planning_repeat_audit& audit,
                             candidate_origin origin)
{
  const auto& source   = ticket.source;
  const auto* profile  = source.profile;
  const auto& view     = source.capture.view();
  const bool verified  = profile && profile->implementation_verified && !source.compatibility;
  const bool validated = verified && view && comparison.verdict == comparison_verdict::equal;
  const bool safe      = audit.observations_available && audit.generation == ticket.generation &&
                    audit.repeat_bind == audit_verdict::safe &&
                    audit.speculative_execution == audit_verdict::safe &&
                    audit.cpu_replay == audit_verdict::safe;
  const char* selector  = !source.requires_selector ? "not_required"
                          : !source.selector        ? "missing"
                          : validated               ? "equal"
                                                    : "present";
  const char* admission = audit.observed_unsafe() ? "unsupported"
                          : validated && safe     ? "supported"
                                                  : "unproven";
  // Only static profile labels and bounded evidence metadata are displayed.
  return "scan ticket=" + std::to_string(ticket.handle) +
         " window=" + std::to_string(ticket.window) +
         " instance=" + std::to_string(ticket.instance) +
         " generation=" + std::to_string(ticket.generation) + " kind=" + kind_name(profile) +
         " profile=" + (profile ? std::string(profile->id) : "unverified") +
         " origin=" + origin_name(origin) +
         " implementation=" + (verified ? "verified" : "unverified") +
         " identity=" + comparison_name(comparison.verdict) +
         " correspondence=" + correspondence_name(comparison.mode) +
         " hash=" + (view ? std::to_string(view->identity_hash) : "unavailable") +
         " available_evidence_depth=" + (view ? "binding_identity" : "none") +
         " validated_depth=" + (validated ? "binding_identity" : "none") +
         " evidence_scope=" + (validated ? "binding_correspondence" : "none") +
         " selector=" + selector +
         " replay=" + (source.allows_cpu_replay ? "source_permitted" : "source_veto") +
         " observations=" + (audit.observations_available ? "available" : "unavailable") +
         " repeat_bind=" + audit_name(audit.repeat_bind) +
         " speculation=" + audit_name(audit.speculative_execution) +
         " cpu_replay=" + audit_name(audit.cpu_replay) + " admission=" + admission;
}
}  // namespace sirius::scan
