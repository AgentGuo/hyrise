#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "file_based_table_generator.hpp"

namespace hyrise {

// Generates the Hybench data
class HybenchTableGenerator : virtual public FileBasedTableGenerator {
 public:
  // Constructor for creating a HybenchTableGenerator in a benchmark.
  explicit HybenchTableGenerator(const std::string& data_path,
                                 const std::shared_ptr<BenchmarkConfig>& benchmark_config);

  std::unordered_map<std::string, BenchmarkTableInfo> generate() override;

 protected:
  void _add_constraints(std::unordered_map<std::string, BenchmarkTableInfo>& table_info_by_name) const final;

  const std::string _data_path;
};

}  // namespace hyrise
