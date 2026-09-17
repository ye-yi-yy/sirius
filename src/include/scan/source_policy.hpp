/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */

#pragma once

#include <set>
#include <string>

namespace duckdb {
class DatabaseInstance;
class PhysicalOperator;
}  // namespace duckdb

namespace sirius::scan {

/// Preserved evidence from the ORIGINAL CPU physical plan. Unknown non-file sources are
/// unclassified, never presumed local; missing file inventory prevents CPU replay.
struct source_policy {
  bool scan_discovery_complete        = true;
  bool byte_source_discovery_complete = true;
  bool reads_s3                       = false;
  bool has_unclassified_source        = false;
  std::set<std::string> replay_vetoes;

  [[nodiscard]] bool permits_source_replay() const noexcept;
  /// Apply source vetoes before a generic decline. SQL can add an S3 veto, never remove one.
  void require_source_replay(const std::string& query_sql, const std::string& error) const;
};

[[nodiscard]] source_policy capture_source_policy(duckdb::DatabaseInstance& db,
                                                  const duckdb::PhysicalOperator& root);

}  // namespace sirius::scan
