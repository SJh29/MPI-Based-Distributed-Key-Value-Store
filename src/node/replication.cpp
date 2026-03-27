#include "node/replication.h"

#include <algorithm>
#include <iostream>

#include "common/mpi_comm.h"

using namespace mpi_comm;

std::vector<int> compute_replica_ranks(int primary_rank,
                                       int world_size,
                                       int replication_factor) {
  std::vector<int> replicas;
  const int storage_nodes = world_size - 1;
  if (storage_nodes <= 1 || primary_rank < 1 || primary_rank >= world_size) {
    return replicas;
  }

  const int replica_count = std::min(replication_factor - 1, storage_nodes - 1);
  if (replica_count <= 0) {
    return replicas;
  }

  replicas.reserve(static_cast<size_t>(replica_count));

  const int primary_index = primary_rank - 1;
  for (int offset = 1; offset <= replica_count; ++offset) {
    const int replica_index = (primary_index + offset) % storage_nodes;
    replicas.push_back(replica_index + 1);
  }

  return replicas;
}

void replicate_put(const Message& client_msg,
                   std::string_view key,
                   std::string_view value,
                   uint64_t version,
                   int world_size,
                   int replication_factor,
                   bool verbose) {
  const int primary_rank = client_msg.primary_rank;
  const std::vector<int> replicas =
      compute_replica_ranks(primary_rank, world_size, replication_factor);

  for (size_t i = 0; i < replicas.size(); ++i) {
    const Message repl = make_repl_put(client_msg.origin_rank,
                                       primary_rank,
                                       static_cast<int>(i + 1),
                                       key,
                                       value,
                                       version);
    send_msg(replicas[i], repl);
    if (verbose) {
      std::cout << "[Node " << primary_rank << "] replicated PUT key=" << key
                << " to replica_rank=" << replicas[i]
                << " replica_index=" << (i + 1) << "\n";
    }
  }
}

void replicate_del(const Message& client_msg,
                   std::string_view key,
                   uint64_t version,
                   int world_size,
                   int replication_factor,
                   bool verbose) {
  const int primary_rank = client_msg.primary_rank;
  const std::vector<int> replicas =
      compute_replica_ranks(primary_rank, world_size, replication_factor);

  for (size_t i = 0; i < replicas.size(); ++i) {
    const Message repl = make_repl_del(client_msg.origin_rank,
                                       primary_rank,
                                       static_cast<int>(i + 1),
                                       key,
                                       version);
    send_msg(replicas[i], repl);
    if (verbose) {
      std::cout << "[Node " << primary_rank << "] replicated DEL key=" << key
                << " to replica_rank=" << replicas[i]
                << " replica_index=" << (i + 1) << "\n";
    }
  }
}
