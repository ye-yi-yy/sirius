/*
 * Copyright 2026, Sirius Contributors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */
#include "scan/scan_contract.hpp"

#include "sirius_context.hpp"

#include <duckdb/common/exception.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/single_file_block_manager.hpp>
#include <duckdb/storage/storage_lock.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/transaction/duck_transaction.hpp>
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
                                                 std::uint64_t epoch,
                                                 std::shared_ptr<contract_counters> counters)
  : _database(table.GetAttached().shared_from_this()),
    _lock(duckdb::DuckTransactionManager::Get(*_database).SharedCheckpointLock()),
    _block_manager(single_file(*_database)),
    _iteration(_block_manager ? _block_manager->GetCheckpointIteration() : 0),
    _epoch(epoch),
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
                                         planning_repeat_audit audit)
  : _candidate(std::move(candidate)),
    _comparison(std::move(comparison)),
    _audit(audit),
    _window(allocate_handle()),
    _membership(std::make_shared<registry_membership>())
{
  _membership->counters = std::move(counters);
}
query_scan_registry::~query_scan_registry() { close(); }
void query_scan_registry::declare_native(duckdb::ClientContext& context, duckdb::DataTable& table)
{
  if (_sealed) { throw duckdb::ExecutorException("Sirius scan construction is sealed"); }
  // All lazy outer transactions start before any checkpoint reader is acquired.
  duckdb::DuckTransaction::Get(context, table.GetAttached());
  _native.emplace(table.GetAttached().oid, table.shared_from_this());
}
void query_scan_registry::seal_and_acquire(duckdb::ClientContext& context)
{
  if (_sealed) { throw duckdb::InternalException("Sirius scan construction sealed twice"); }
  _sealed = true;
  if (!_native.empty()) {
    _connection = duckdb::get_sirius_connection_state(context);
    if (_connection) { ++_connection->protected_scan_windows; }
  }
  for (const auto& [id, table] : _native) {
    _leases.emplace(
      id, std::make_shared<native_checkpoint_lease>(*table, _window, _membership->counters));
  }
}
std::shared_ptr<native_checkpoint_lease> query_scan_registry::lease_for(
  duckdb::DataTable& table) const
{
  auto it = _leases.find(table.GetAttached().oid);
  if (!_sealed || it == _leases.end()) {
    throw duckdb::ExecutorException("Sirius scan contract: undeclared_native_resource");
  }
  it->second->validate(table);
  return it->second;
}
bound_table_scan_ptr query_scan_registry::add(duckdb::idx_t index,
                                              consumer_requirements requirements)
{
  if (_membership->active.load()) {
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
  if (!_sealed || _membership->active.load()) {
    throw duckdb::InternalException("Sirius scan contract: invalid registry freeze");
  }
  if (_tickets.size() != _candidate->sources.size()) {
    throw duckdb::NotImplementedException("Sirius scan contract: incomplete_consumer_registry");
  }
  _membership->active.store(true, std::memory_order_release);
}
void query_scan_registry::close() noexcept
{
  _membership->active.store(false, std::memory_order_release);
  for (const auto& [id, lease] : _leases) {
    lease->release();
  }
  if (_connection) {
    --_connection->protected_scan_windows;
    _connection.reset();
  }
}
void query_scan_registry::validate() const
{
  for (const auto& [id, lease] : _leases) {
    lease->validate();
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
std::string contract_summary(const bound_table_scan& ticket, const comparison_result& comparison)
{
  // No paths, table names, selectors or predicate values in the bounded diagnostic.
  return "scan ticket=" + std::to_string(ticket.handle) +
         " window=" + std::to_string(ticket.window) + " source=" + ticket.source.function +
         " identity=" + comparison_name(comparison.verdict) +
         " correspondence=" + correspondence_name(comparison.mode) + " hash=" +
         (ticket.source.capture.view() ? std::to_string(std::hash<std::string>{}(
                                           ticket.source.capture.view()->canonical_identity))
                                       : "unavailable") +
         " evidence=" + (ticket.source.capture.view() ? "binding" : "compatibility") +
         " replay=" + (ticket.source.allows_cpu_replay ? "source_permitted" : "source_veto") +
         " safety=unproven admission=unproven";
}
}  // namespace sirius::scan
