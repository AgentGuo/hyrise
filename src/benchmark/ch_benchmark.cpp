#include <algorithm>

#include "benchmark_runner.hpp"
#include "cli_config_parser.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "ch/ch_benchmark_item_runner.hpp"
#include "tpcc/tpcc_table_generator.hpp"
#include "jcch/jcch_benchmark_item_runner.hpp"
#include "jcch/jcch_table_generator.hpp"
#include "tpch/tpch_benchmark_item_runner.hpp"
#include "tpch/tpch_queries.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "tpcc/tpcc_benchmark_item_runner.hpp"

using namespace hyrise;  // NOLINT

/**
 * This benchmark measures Hyrise's performance executing the TPC-C benchmark. As with the other TPC-* benchmarks, we
 * took some liberty in interpreting the standard. Most notably, all parts about the simulated terminals are ignored.
 * Instead, only the queries leading to the terminal output are executed. In the research world, doing so has become
 * the de facto standard.
 *
 * Other limitations (that may be removed in the future):
 *  - No primary / foreign keys are used as they are currently unsupported
 *  - Values that are "retrieved" by the terminal are just selected, but not necessarily materialized
 *  - Data is not persisted as logging is currently unsupported; this means that the durability tests are not executed
 *  - As decimals are not supported, we use floats instead
 *  - The delivery transaction is not executed in a "deferred" mode; as such, no delivery result file is written
 *  - We do not execute the isolation tests, as we consider our MVCC tests to be sufficient
 *
 * Most importantly, we do not claim to report correctly calculated tpmC.
 *
 * main() is mostly concerned with parsing the CLI options while BenchmarkRunner.run() performs the actual benchmark
 * logic.
 */

//int main(int argc, char* argv[]) {
//  auto cli_options = BenchmarkRunner::get_basic_cli_options("TPC-C Benchmark");
//
//  // clang-format off
//  cli_options.add_options()
//    // We use -s instead of -w for consistency with the options of our other TPC-x binaries.
//    ("s,scale", "Scale factor (warehouses)", cxxopts::value<size_t>()->default_value("10"))
//    ("consistency_checks", "Run TPC-C consistency checks after benchmark (included with --verify)", cxxopts::value<bool>()->default_value("false"));  // NOLINT(whitespace/line_length)
//  // clang-format on
//
//  auto config = std::shared_ptr<BenchmarkConfig>{};
//  auto num_warehouses = size_t{0};
//
//  // Parse command line args
//  const auto cli_parse_result = cli_options.parse(argc, argv);
//
//  if (CLIConfigParser::print_help_if_requested(cli_options, cli_parse_result)) {
//    return 0;
//  }
//
//  num_warehouses = cli_parse_result["scale"].as<size_t>();
//
//  config = std::make_shared<BenchmarkConfig>(CLIConfigParser::parse_cli_options(cli_parse_result));
//
//  // As TPC-C procedures may run into conflicts on both the Hyrise and the SQLite side, we cannot guarantee that the
//  // two databases stay in sync.
//  Assert(!config->verify || config->clients == 1, "Cannot run verification with more than one client.");
//
//  auto context = BenchmarkRunner::create_context(*config);
//
//  std::cout << "- TPC-C scale factor (number of warehouses) is " << num_warehouses << '\n';
//
//  // Add TPC-C-specific information
//  context.emplace("scale_factor", num_warehouses);
//
//  // Run the benchmark
//  auto item_runner = std::make_unique<TPCCBenchmarkItemRunner>(config, num_warehouses);
//  BenchmarkRunner(*config, std::move(item_runner), std::make_unique<TPCCTableGenerator>(num_warehouses, config),
//                  context)
//      .run();
//}
int main(int argc, char* argv[]) {
  auto cli_options = BenchmarkRunner::get_basic_cli_options("TPC-C Benchmark");

  // clang-format off
  cli_options.add_options()
    // We use -s instead of -w for consistency with the options of our other TPC-x binaries.
    ("s,scale", "Scale factor (warehouses)", cxxopts::value<size_t>()->default_value("1"))
    ("consistency_checks", "Run TPC-C consistency checks after benchmark (included with --verify)", cxxopts::value<bool>()->default_value("false"));  // NOLINT(whitespace/line_length)
  // clang-format on

  auto config = std::shared_ptr<BenchmarkConfig>{};
  auto num_warehouses = size_t{0};
  auto scale_factor = float{};
  auto clustering_configuration = ClusteringConfiguration::None;
  auto table_generator = std::unique_ptr<AbstractTableGenerator>{};
  auto use_prepared_statements = false;

  // Parse command line args
  const auto cli_parse_result = cli_options.parse(argc, argv);

  if (CLIConfigParser::print_help_if_requested(cli_options, cli_parse_result)) {
    return 0;
  }

  num_warehouses = cli_parse_result["scale"].as<size_t>();
  scale_factor = (float)num_warehouses;

  config = std::make_shared<BenchmarkConfig>(CLIConfigParser::parse_cli_options(cli_parse_result));

  // As TPC-C procedures may run into conflicts on both the Hyrise and the SQLite side, we cannot guarantee that the
  // two databases stay in sync.
  Assert(!config->verify || config->clients == 1, "Cannot run verification with more than one client.");

  auto context = BenchmarkRunner::create_context(*config);

  std::cout << "- TPC-C scale factor (number of warehouses) is " << num_warehouses << '\n';

  // Add TPC-C-specific information
  context.emplace("scale_factor", num_warehouses);

  // Run the benchmark
  table_generator = std::make_unique<TPCHTableGenerator>(scale_factor, clustering_configuration, config);
  auto item_runner = std::make_unique<CHBenchmarkItemRunner>(config, use_prepared_statements, scale_factor,
                                                               clustering_configuration);
  table_generator->generate_and_store();
  BenchmarkRunner(*config, std::move(item_runner), std::make_unique<TPCCTableGenerator>(num_warehouses, config),
                  context)
      .run();
//  std::cout<<scale_factor<<use_prepared_statements<<std::endl;
  return 0;
}
