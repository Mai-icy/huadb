#pragma once

#include "storage/buffer_strategy.h"
#include <unordered_map>
#include <list>

namespace huadb {

class LRUBufferStrategy : public BufferStrategy {
 public:
  void Access(size_t frame_no) override;
  size_t Evict() override;
 private:
  std::list<size_t> cache_items_;
  std::unordered_map<size_t, std::list<size_t>::iterator> cache_map_;
};

}  // namespace huadb
