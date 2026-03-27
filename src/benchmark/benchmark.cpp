#include "benchmark/benchmark.h"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iostream>
#include <numeric>
#include <stdexcept>

std::string benchmark_mode_to_string(BenchmarkMode mode) {
  switch (mode) {
    case BenchmarkMode::BLOCKING:
      return "blocking";
  }
  return "unknown";
}

BenchmarkRunner::BenchmarkRunner(KvClient& client) : client_(client) {}

std::vector<ClientCommand> BenchmarkRunner::build_workload(const BenchmarkOptions& options) const {
  std::vector<ClientCommand> commands;
  commands.reserve(static_cast<size_t>(options.operation_count));

  const int bounded_read_ratio = std::clamp(options.read_ratio, 0, 100);
  for (int i = 0; i < options.operation_count; ++i) {
    const int key_id = i % std::max(1, options.key_space);
    const std::string key = "bench_key_" + std::to_string(key_id);
    const std::string value = "bench_value_" + std::to_string(i);

    const int op_percent = (i * 100) / options.operation_count;
    if (op_percent < bounded_read_ratio) {
      commands.push_back(ClientCommand{ClientCommandType::GET, key, ""});
    } else {
      commands.push_back(ClientCommand{ClientCommandType::PUT, key, value});
    }
  }

  return commands;
}

std::vector<BenchmarkSample> BenchmarkRunner::run_blocking_round(
    const std::vector<ClientCommand>& commands,
    const BenchmarkOptions& options,
    int round) const {
  std::vector<BenchmarkSample> samples;
  samples.reserve(commands.size());

  for (size_t i = 0; i < commands.size(); ++i) {
    const ClientResult result = client_.execute(commands[i]);

    samples.push_back(BenchmarkSample{
        round,
        static_cast<int>(i),
        options.mode,
        result.latency_us,
        result.request_id,
        result.target_rank,
        result.status,
    });
  }

  return samples;
}

void BenchmarkRunner::print_round_summary(const std::vector<BenchmarkSample>& samples,
                                          const BenchmarkOptions& options,
                                          int round) const {
  if (!options.verbose || samples.empty()) return;

  const long long total_us = std::accumulate(
      samples.begin(), samples.end(), 0LL,
      [](long long acc, const BenchmarkSample& sample) { return acc + sample.latency_us; });

  const double avg_us = static_cast<double>(total_us) / static_cast<double>(samples.size());
  const long long max_us = std::max_element(
      samples.begin(), samples.end(),
      [](const BenchmarkSample& a, const BenchmarkSample& b) { return a.latency_us < b.latency_us; })
                               ->latency_us;
  const long long min_us = std::min_element(
      samples.begin(), samples.end(),
      [](const BenchmarkSample& a, const BenchmarkSample& b) { return a.latency_us < b.latency_us; })
                               ->latency_us;

  std::cout << "[Benchmark] round=" << round
            << " mode=" << benchmark_mode_to_string(options.mode)
            << " ops=" << samples.size()
            << " avg_latency_us=" << avg_us
            << " min_latency_us=" << min_us
            << " max_latency_us=" << max_us << "\n";
}

void BenchmarkRunner::print_overall_summary(const std::vector<BenchmarkSample>& samples,
                                            const BenchmarkOptions& options) const {
  if (!options.verbose || samples.empty()) return;

  const long long total_us = std::accumulate(
      samples.begin(), samples.end(), 0LL,
      [](long long acc, const BenchmarkSample& sample) { return acc + sample.latency_us; });

  const double avg_us = static_cast<double>(total_us) / static_cast<double>(samples.size());

  std::cout << "[Benchmark] overall mode=" << benchmark_mode_to_string(options.mode)
            << " samples=" << samples.size()
            << " avg_latency_us=" << avg_us << "\n";
}

void BenchmarkRunner::write_csv(const std::vector<BenchmarkSample>& samples,
                                const std::string& csv_path) const {
  std::ofstream out(csv_path);
  if (!out.is_open()) {
    throw std::runtime_error("failed to open benchmark csv output: " + csv_path);
  }

  out << "round,op_index,mode,latency_us,request_id,target_rank,status\n";
  for (const BenchmarkSample& sample : samples) {
    out << sample.round << ','
        << sample.op_index << ','
        << benchmark_mode_to_string(sample.mode) << ','
        << sample.latency_us << ','
        << sample.request_id << ','
        << sample.target_rank << ','
        << static_cast<int>(sample.status) << '\n';
  }
}

void BenchmarkRunner::run(const BenchmarkOptions& options) {
  if (options.operation_count <= 0) {
    throw std::invalid_argument("operation_count must be > 0");
  }

  if (options.read_ratio < 0 || options.read_ratio > 100) {
    throw std::invalid_argument("read_ratio must be in [0, 100]");
  }

  std::vector<BenchmarkSample> all_samples;
  double total_wall_seconds = 0.0;
  for (int round = 0; round < options.rounds; ++round) {
    const std::vector<ClientCommand> commands = build_workload(options);

    const auto round_started = std::chrono::steady_clock::now();
    std::vector<BenchmarkSample> samples = run_blocking_round(commands, options, round);
    const auto round_finished = std::chrono::steady_clock::now();

    const double round_seconds =
        std::chrono::duration_cast<std::chrono::duration<double>>(round_finished - round_started)
            .count();
    const double round_throughput =
        round_seconds > 0.0 ? static_cast<double>(samples.size()) / round_seconds : 0.0;
    total_wall_seconds += round_seconds;

    print_round_summary(samples, options, round);
    if (options.verbose) {
      std::cout << "[Benchmark] round=" << round
                << " throughput_ops_per_sec=" << round_throughput << "\n";
    }
    all_samples.insert(all_samples.end(), samples.begin(), samples.end());
  }

  const long long total_us = std::accumulate(
      all_samples.begin(), all_samples.end(), 0LL,
      [](long long acc, const BenchmarkSample& sample) { return acc + sample.latency_us; });

  const double avg_us = all_samples.empty()
                            ? 0.0
                            : static_cast<double>(total_us) / static_cast<double>(all_samples.size());
  const size_t total_ops = all_samples.size();
  const double throughput_ops_per_sec =
      total_wall_seconds > 0.0 ? static_cast<double>(total_ops) / total_wall_seconds : 0.0;

  if (!options.verbose) {
    std::cout << "Throughput (ops/sec): " << throughput_ops_per_sec << "\n";
    std::cout << "Avg Latency (us): " << avg_us << "\n";
  }

  print_overall_summary(all_samples, options);
  write_csv(all_samples, options.csv_path);
  if (options.verbose) {
    std::cout << "[Benchmark] wrote csv=" << options.csv_path << " rows=" << all_samples.size() << "\n";
  }
}
