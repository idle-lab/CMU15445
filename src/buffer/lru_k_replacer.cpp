//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

#include <algorithm>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : k_(k), replacer_size_(num_frames) {}

void LRUKReplacer::RmFrame(frame_id_t frame_id) {
  if (node_store_[frame_id]->k_ < k_) {
    young_list_.erase(node_store_[frame_id]);
  } else {
    old_list_.erase(node_store_[frame_id]);
  }
  node_store_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lg(latch_);
  if (Size() == 0) {  // 没有可以替换的页面
    return false;
  }
  if (auto res = std::find_if(young_list_.rbegin(), young_list_.rend(), [](auto it) { return it.is_evictable_; });
      res != young_list_.rend()) {
    // 在未到达 k 次的队列中找到第一个可以替换的
    *frame_id = res->fid_;
  } else {
    // 在到达 k 次的队列中找到可以替换的
    *frame_id = std::find_if(old_list_.rbegin(), old_list_.rend(), [](auto it) { return it.is_evictable_; })->fid_;
  }
  RmFrame(*frame_id);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lg(latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception("Invalid frame id");
  }

  LRUKNode t;
  // 取出 frame
  if (node_store_.find(frame_id) != node_store_.end()) {
    // 对应 frame 在 replacer 中

    if (node_store_[frame_id]->k_ < k_ - 1) {  // 不需要移动的
      node_store_[frame_id]->k_++;
      node_store_[frame_id]->history_.push_front(timestamp_++);
      return;
    }
    t = *node_store_[frame_id];
    if (node_store_[frame_id]->k_ == k_ - 1) {
      young_list_.erase(node_store_[frame_id]);
    } else {
      old_list_.erase(node_store_[frame_id]);
    }
  } else {
    // 对应 frame 不在 replacer 中
    t.fid_ = frame_id;
  }

  // 记录这次访问
  t.k_++;
  t.history_.push_front(timestamp_++);
  if (t.k_ > k_) {
    t.history_.pop_back();
    t.k_--;
  }

  // 将 frame 重新放入 replacer，并维护顺序
  // 放入 old 链表
  if (t.k_ == k_) {
    for (auto it = old_list_.begin(); it != old_list_.end(); ++it) {
      if (it->history_.back() < t.history_.back()) {
        node_store_[frame_id] = old_list_.insert(it, std::move(t));
        return;
      }
    }
    old_list_.push_back(std::move(t));
    node_store_[frame_id] = (--old_list_.end());
    return;
  }

  // 放入 yong 链表·
  if (t.k_ == 1) {  // 如果是第一次被访问，就加入到 yong 队列中
    young_list_.push_front(std::move(t));
    node_store_[frame_id] = young_list_.begin();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lg(latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception("Invalid frame id");
  }

  if (set_evictable && !node_store_[frame_id]->is_evictable_) {
    curr_size_++;
  } else if (node_store_[frame_id]->is_evictable_ && !set_evictable) {
    curr_size_--;
  }

  node_store_[frame_id]->is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lg(latch_);

  if (node_store_.find(frame_id) == node_store_.end()) {  // 页面不存在
    return;
  }
  if (!node_store_[frame_id]->is_evictable_) {  // 页面不可替换
    throw Exception("Remove a non-evictable frame");
  }

  RmFrame(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
