#include "ch_benchmark_item_runner.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/date_time/gregorian/greg_date.hpp>

#include "abstract_benchmark_item_runner.hpp"
#include "benchmark_config.hpp"
#include "benchmark_sql_executor.hpp"

#include "hyrise.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/date_time_utils.hpp"

namespace hyrise {

CHBenchmarkItemRunner::CHBenchmarkItemRunner(const std::shared_ptr<BenchmarkConfig>& config,
                                                 bool use_prepared_statements, float scale_factor,
                                                 ClusteringConfiguration clustering_configuration)
    : AbstractBenchmarkItemRunner(config),
      _scale_factor(scale_factor){
  _tpch_item_runner_ptr = std::make_unique<TPCHBenchmarkItemRunner>(config, use_prepared_statements, scale_factor, clustering_configuration);
  _tpcc_item_runner_ptr = std::make_unique<TPCCBenchmarkItemRunner>(config, (int)scale_factor);
  _items.resize(27);
  std::iota(_items.begin(), _items.end(), BenchmarkItemID{0});
}

const std::vector<BenchmarkItemID>& CHBenchmarkItemRunner::items() const {
  return _items;
}

bool CHBenchmarkItemRunner::_on_execute_item(const BenchmarkItemID item_id, BenchmarkSQLExecutor& sql_executor) {
  if (item_id < 5){
   return _tpcc_item_runner_ptr->_on_execute_ch_item(item_id, sql_executor);
  }else{
    return _tpch_item_runner_ptr->_on_execute_ch_item(BenchmarkItemID{item_id-5}, sql_executor);
  }
}

void CHBenchmarkItemRunner::on_tables_loaded() {
  _tpch_item_runner_ptr->on_tables_loaded();
  _tpcc_item_runner_ptr->on_tables_loaded();
}

std::string CHBenchmarkItemRunner::item_name(const BenchmarkItemID item_id) const {
  if (item_id < 5){
    return _tpcc_item_runner_ptr->item_name(item_id);
  }else{
    return _tpch_item_runner_ptr->item_name(BenchmarkItemID{item_id-5});
  }
}

const std::vector<int>& CHBenchmarkItemRunner::weights() const {
  // Except for New-Order, the given weights are minimums (see 5.2.3 in the standard). Since New-Order is the
  // transaction being counted for tpmC, we want it to have the highest weight possible.
  static const auto weights = std::vector<int>{4, 45, 4, 43, 4, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5};
//  static const auto weights = std::vector<int>{0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  return weights;
}

}  // namespace hyrise
