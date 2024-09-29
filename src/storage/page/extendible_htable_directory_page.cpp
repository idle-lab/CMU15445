//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  global_depth_ = 0;
  max_depth_ = max_depth;
  for (int i = 0; i < (1 << max_depth); ++i) {
    local_depths_[i] = 0;
    bucket_page_ids_[i] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  return hash & GetGlobalDepthMask();
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  if (bucket_idx >= (1 << global_depth_)) {
    return;
  }
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t { return (1 << global_depth_) - 1; }

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  return (1 << local_depths_[bucket_idx]) - 1;
}

/**
 * Gets the split image of an index.
 * 在Global Depth增大时，我会调用该函数。Global Depth增大，htable_dir的容量会翻倍
 * 如：位置 10 处（这里都是二进制数），在 Global Depth 增大时，会有两个位置指向同一个 bucket 页面：
 * 010 和 110
 * 010 对应的下标不变
 * 110 下标位置的 page id 要从 010 位置拷贝来
 *
 * @param bucket_idx the directory index for which to find the split image
 * @return the directory index of the split image
 **/
auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  return bucket_idx + (1 << global_depth_);
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return max_depth_; }

/**
 * Increment the global depth of the directory
 * 扩容 directory 的时候就要通过 GetSplitImageIndex 获取要拷贝到的索引位置
 */
void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  if (global_depth_ == max_depth_) {
    return;
  }
  for (int i = 0; i < (1 << global_depth_); ++i) {
    auto j = GetSplitImageIndex(i);
    bucket_page_ids_[j] = bucket_page_ids_[i];
    local_depths_[j] = local_depths_[i];
  }
  global_depth_++;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  if (global_depth_ <= 0) {
    return;
  }
  global_depth_--;
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  if (global_depth_ == 0) {
    return false;
  }
  for (int i = 0; i < (1 << global_depth_); ++i) {
    if (local_depths_[i] == global_depth_) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return (1 << global_depth_); }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  if (bucket_idx >= (1 << global_depth_)) {
    return;
  }
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  if (local_depths_[bucket_idx] == global_depth_) {
    return;
  }
  local_depths_[bucket_idx]++;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  if (local_depths_[bucket_idx] == 0) {
    return;
  }
  local_depths_[bucket_idx]--;
}

}  // namespace bustub
