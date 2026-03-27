#pragma once

#include <string_view>

// Maps a key to a primary storage node rank in [1, world_size - 1].
// Returns -1 when world_size is invalid (< 2).
int route_key_to_primary(std::string_view key, int world_size);
