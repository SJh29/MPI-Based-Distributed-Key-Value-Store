#include "router/router.h"

#include <functional>

int route_key_to_primary(std::string_view key, int world_size) {
  if (world_size < 2) {
    return -1;
  }

  const std::size_t shard_count = static_cast<std::size_t>(world_size - 1);
  const std::size_t key_hash = std::hash<std::string_view>{}(key);
  return 1 + static_cast<int>(key_hash % shard_count);
}
