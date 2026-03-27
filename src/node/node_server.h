#pragma once

#include <cstdint>

void run_node_server(int rank,
                     bool verbose = true,
                     bool snapshot_enabled = true,
                     bool wal_enabled = true,
                     uint64_t snapshot_interval_seconds = 1800);
