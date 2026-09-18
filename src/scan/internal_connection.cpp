/*
 * Copyright 2026, Sirius Contributors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */
#include "scan/internal_connection.hpp"

#include "sirius_context.hpp"

#include <duckdb/parser/parser.hpp>
#include <duckdb/transaction/meta_transaction.hpp>

namespace sirius::scan {

struct internal_connection::state {
  std::unique_ptr<duckdb::Connection> connection;
  std::unique_ptr<duckdb::SiriusContext::InternalQueryGuard> guard;
  duckdb::transaction_t transaction = 0;

  ~state()
  {
    if (connection) {
      try {
        if (connection->context->transaction.HasActiveTransaction()) { connection->Rollback(); }
      } catch (...) {
      }
      // Connection-close callbacks run while the target's internal bracket is still live.
      connection.reset();
    }
  }

  void require_transaction() const
  {
    auto& context = *connection->context;
    if (!context.transaction.HasActiveTransaction() || !context.ActiveTransaction().IsReadOnly() ||
        context.ActiveTransaction().global_transaction_id != transaction) {
      throw duckdb::ExecutorException(
        "Sirius scan contract: internal read-only transaction changed");
    }
  }
};

internal_connection::internal_connection(duckdb::ClientContext& outer,
                                         std::initializer_list<const char*> mirrored_settings)
  : _state(std::make_unique<state>())
{
  _state->connection = std::make_unique<duckdb::Connection>(*outer.db);
  auto& context      = *_state->connection->context;
  _state->guard      = std::make_unique<duckdb::SiriusContext::InternalQueryGuard>(context);
  if (auto target = duckdb::get_sirius_connection_state(context)) {
    target->internal_query_parent = duckdb::get_sirius_connection_state(outer);
  }
  auto begin = _state->connection->Query("BEGIN TRANSACTION READ ONLY");
  if (begin->HasError()) { begin->ThrowError(); }
  _state->transaction = context.ActiveTransaction().global_transaction_id;
  _state->require_transaction();
  for (const auto* setting : mirrored_settings) {
    duckdb::Value value;
    if (!outer.TryGetCurrentSetting(setting, value) || value.IsNull()) { continue; }
    const auto enabled     = value.DefaultCastAs(duckdb::LogicalType::BOOLEAN).GetValue<bool>();
    std::string identifier = "\"";
    for (const char c : std::string(setting)) {
      identifier += c == '"' ? "\"\"" : std::string(1, c);
    }
    identifier += "\"";
    auto result =
      _state->connection->Query("SET SESSION " + identifier + " = " + (enabled ? "true" : "false"));
    if (result->HasError()) { result->ThrowError(); }
    _state->require_transaction();
  }
}

internal_connection::~internal_connection() = default;

duckdb::unique_ptr<duckdb::MaterializedQueryResult> internal_connection::Query(
  const std::string& sql)
{
  _state->require_transaction();
  duckdb::Parser parser(_state->connection->context->GetParserOptions());
  parser.ParseQuery(sql);
  if (parser.statements.size() != 1 ||
      parser.statements.front()->type != duckdb::StatementType::SELECT_STATEMENT) {
    throw duckdb::ExecutorException(
      "Sirius scan contract: internal connection requires one SELECT");
  }
  auto result = _state->connection->Query(std::move(parser.statements.front()));
  if (!result->HasError()) { _state->require_transaction(); }
  return result;
}

internal_connection open_internal_connection(duckdb::ClientContext& context,
                                             std::initializer_list<const char*> mirrored_settings)
{
  return internal_connection(context, mirrored_settings);
}

}  // namespace sirius::scan
