/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */

#pragma once

#include "source_adapters.hpp"

namespace sirius::scan::detail {

bool same_implementation(const duckdb::TableFunction&, const duckdb::TableFunction&);
bool is_s3(const std::string& path);
// TODO(R1 D2): replace legacy expanding enumeration with supported provider snapshots.
std::optional<std::vector<std::string>> legacy_multi_file_paths(const duckdb::FunctionData*);
source_policy_evidence inspect_legacy_multi_file_source(const binding_ref&);

void identity_field(std::string& out, const std::string& tag, const std::string& value);
template <typename T>
void identity_number(std::string& out, const std::string& tag, T value)
{
  identity_field(out, tag, std::to_string(value));
}
bound_schema_ptr capture_schema(const capture_request&);
read_view_capture complete_read_view(const capture_request&,
                                     const source_profile&,
                                     bound_schema_ptr,
                                     std::string source_identity);

class factory_source_adapter : public scan_source_adapter {
 public:
  factory_source_adapter(source_profile profile, duckdb::TableFunction function)
    : _profile(profile), _function(std::move(function))
  {
  }
  const source_profile& profile() const noexcept final { return _profile; }
  binding_verification verify_binding(const binding_ref& binding) const final
  {
    return same_implementation(binding.function, _function) && valid_payload(binding)
             ? binding_verification::verified
             : binding_verification::rejected;
  }

 protected:
  virtual bool valid_payload(const binding_ref&) const = 0;

 private:
  const source_profile _profile;
  const duckdb::TableFunction _function;
};

}  // namespace sirius::scan::detail
