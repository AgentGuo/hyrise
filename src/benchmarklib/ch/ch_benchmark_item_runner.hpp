#pragma once
#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "abstract_benchmark_item_runner.hpp"
#include "tpch/tpch_benchmark_item_runner.hpp"
#include "tpcc/tpcc_benchmark_item_runner.hpp"

namespace hyrise {

class CHBenchmarkItemRunner : public AbstractBenchmarkItemRunner {
 public:
  // Constructor for a CHBenchmarkItemRunner containing all TPC-H queries
  CHBenchmarkItemRunner(const std::shared_ptr<BenchmarkConfig>& config, bool use_prepared_statements,
                          float scale_factor, ClusteringConfiguration clustering_configuration);

  void on_tables_loaded() override;

  std::string item_name(const BenchmarkItemID item_id) const override;
  const std::vector<BenchmarkItemID>& items() const override;
  const std::vector<int>& weights() const override;

 protected:
  bool _on_execute_item(const BenchmarkItemID item_id, BenchmarkSQLExecutor& sql_executor) override;

  std::unique_ptr<TPCHBenchmarkItemRunner> _tpch_item_runner_ptr;
  std::unique_ptr<TPCCBenchmarkItemRunner> _tpcc_item_runner_ptr;
  const float _scale_factor;

  std::vector<BenchmarkItemID> _items;


};

}  // namespace hyrise
