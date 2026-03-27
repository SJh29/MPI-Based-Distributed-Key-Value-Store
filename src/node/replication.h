#pragma once

#include <string_view>
#include <vector>

#include "common/protocol.h"

// Returns replication targets for the given primary rank using a ring over
// storage ranks [1, world_size-1].
std::vector<int> compute_replica_ranks(int primary_rank,
                                       int world_size,
                                       int replication_factor);

// Forward write/delete mutations to replica nodes.
void replicate_put(const Message& client_msg,
                   std::string_view key,
                   std::string_view value,
                   uint64_t version,
                   int world_size,
                   int replication_factor,
                   bool verbose = false);

void replicate_del(const Message& client_msg,
                   std::string_view key,
                   uint64_t version,
                   int world_size,
                   int replication_factor,
                   bool verbose = false);
