#include "storage/lru_buffer_strategy.h"

namespace huadb {

void LRUBufferStrategy::Access(size_t frame_no) {
  // 缓存页面访问
  // LAB 1 DONE
  auto it = cache_map_.find(frame_no);
  if(it == cache_map_.end()){
    cache_items_.insert(cache_items_.begin(), frame_no);
    cache_map_[frame_no] = cache_items_.begin();
  }else{
    cache_items_.splice(cache_items_.begin(), cache_items_, it->second);
  }
};

size_t LRUBufferStrategy::Evict() {
  // 缓存页面淘汰，返回淘汰的页面在 buffer pool 中的下标
  // LAB 1 BEGIN
  size_t res = cache_items_.back();
  cache_items_.pop_back();
  return res;
}

}  // namespace huadb
