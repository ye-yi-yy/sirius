/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */

#include "scan/source_registry.hpp"

#include "adapters/source_adapters.hpp"

#include <duckdb/common/exception.hpp>
#include <duckdb/main/database.hpp>

#include <atomic>
#include <limits>
#include <mutex>

namespace sirius::scan {
namespace {
std::uint64_t next_instance()
{
  static std::atomic<std::uint64_t> next{1};
  auto value = next.load(std::memory_order_relaxed);
  do {
    if (value == std::numeric_limits<std::uint64_t>::max()) {
      throw duckdb::InvalidInputException("scan registry instance IDs exhausted");
    }
  } while (!next.compare_exchange_weak(value, value + 1, std::memory_order_relaxed));
  return value;
}

}  // namespace

source_registry::source_registry(duckdb::DatabaseInstance& db) : _db(db), _instance(next_instance())
{
  _adapters.push_back(make_native_source_adapter());
  _adapters.push_back(make_parquet_source_adapter());
  _adapters.push_back(make_owned_parquet_source_adapter());
  _adapters.push_back(make_stream_source_adapter());
  _adapters.push_back(make_iceberg_source_adapter());
}

source_registry& source_registry::get(duckdb::DatabaseInstance& db)
{
  // The callback manager retains the registry for exactly this DatabaseInstance's lifetime.
  const auto find = [&]() -> source_registry* {
    for (auto& callback : duckdb::ExtensionCallback::Iterate(db)) {
      if (auto* registry = dynamic_cast<source_registry*>(callback.get())) { return registry; }
    }
    return nullptr;
  };
  if (auto* registry = find()) { return *registry; }
  // Serialize only first publication, including direct planner callers without a runtime.
  static std::mutex initialization;
  std::lock_guard<std::mutex> guard(initialization);
  if (auto* registry = find()) { return *registry; }
  auto registry = duckdb::make_shared_ptr<source_registry>(db);
  auto& result  = *registry;
  duckdb::ExtensionCallback::Register(duckdb::DBConfig::GetConfig(db), std::move(registry));
  return result;
}

const scan_source_adapter* source_registry::lookup(const duckdb::TableFunction& function,
                                                   const duckdb::FunctionData* data) const
{
  for (const auto& adapter : _adapters) {
    if (adapter->verify_binding({_db, function, data}) != binding_verification::rejected) {
      return adapter.get();
    }
  }
  return nullptr;
}

const scan_source_adapter& source_registry::require(const duckdb::TableFunction& function,
                                                    const duckdb::FunctionData* data,
                                                    scan_attempt_diagnostics* diagnostics) const
{
  const auto* adapter = lookup(function, data);
  if (!adapter) {
    if (diagnostics) { diagnostics->refuse(scan_refusal_reason::source_identity_unverified); }
    throw duckdb::NotImplementedException("Table function '%s' is not supported in Sirius",
                                          function.name);
  }
  return *adapter;
}

}  // namespace sirius::scan
