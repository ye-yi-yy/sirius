/*
 * Copyright 2026, Sirius Contributors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */
#pragma once

#include <duckdb/main/connection.hpp>

#include <initializer_list>
#include <memory>

namespace sirius::scan {

class internal_connection {
 public:
  internal_connection(duckdb::ClientContext&, std::initializer_list<const char*> mirrored_settings);
  ~internal_connection();
  internal_connection(const internal_connection&)            = delete;
  internal_connection& operator=(const internal_connection&) = delete;

  duckdb::unique_ptr<duckdb::MaterializedQueryResult> Query(const std::string& sql);

 private:
  struct state;
  std::unique_ptr<state> _state;
};

internal_connection open_internal_connection(
  duckdb::ClientContext&, std::initializer_list<const char*> mirrored_settings = {});

}  // namespace sirius::scan
