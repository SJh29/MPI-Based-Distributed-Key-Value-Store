#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "client/client.h"

enum class BenchmarkMode {
  BLOCKING,
};

struct BenchmarkOptions {
  BenchmarkMode mode{BenchmarkMode::BLOCKING};
  bool verbose{true};
  int operation_count{1000};
  int key_space{256};
  int rounds{1};
  int read_ratio{50};
  std::string csv_path{"benchmark_results.csv"};
};

struct BenchmarkSample {
  int round{0};
  int op_index{0};
  BenchmarkMode mode{BenchmarkMode::BLOCKING};
  long long latency_us{0};
  int request_id{0};
  int target_rank{-1};
  Status status{Status::ERROR};
};

class BenchmarkRunner {
 public:
  explicit BenchmarkRunner(KvClient& client);

  void run(const BenchmarkOptions& options);

 private:
  std::vector<ClientCommand> build_workload(const BenchmarkOptions& options) const;
  std::vector<BenchmarkSample> run_blocking_round(const std::vector<ClientCommand>& commands,
                                                  const BenchmarkOptions& options,
                                                  int round) const;

  void print_round_summary(const std::vector<BenchmarkSample>& samples,
                           const BenchmarkOptions& options,
                           int round) const;
  void print_overall_summary(const std::vector<BenchmarkSample>& samples,
                             const BenchmarkOptions& options) const;
  void write_csv(const std::vector<BenchmarkSample>& samples, const std::string& csv_path) const;

  KvClient& client_;
};

std::string benchmark_mode_to_string(BenchmarkMode mode);
