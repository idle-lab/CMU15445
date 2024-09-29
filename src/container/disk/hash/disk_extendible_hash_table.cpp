//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  // 初始化 header 页
  index_name_ = name;
  auto header_page_guard = bpm_->NewPageGuarded(&header_page_id_).UpgradeWrite();
  auto header_page = header_page_guard.AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth_);
}

/*****************************************************************************
 * SEARCH
 * Get the value associated with a given key in the hash table.
 *
 * Note(fall2023): This semester you will only need to support unique key-value pairs.
 *
 * @param key the key to look up
 * @param[out] result the value(s) associated with a given key
 * @param transaction the current transaction
 * @return 是否存在对应 key-value 值
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  auto hash = Hash(key);
  page_id_t directory_page_id;
  page_id_t bucket_page_id;
  {  // 取 directory page id
    auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
    auto header_page = header_page_guard.As<ExtendibleHTableHeaderPage>();
    auto directory_idx = header_page->HashToDirectoryIndex(hash);
    directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  }
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }

  {  // 取 bucket page id
    auto directory_page_guard = bpm_->FetchPageRead(directory_page_id);
    auto directory_page = directory_page_guard.As<ExtendibleHTableDirectoryPage>();
    auto bucket_idx = directory_page->HashToBucketIndex(hash);
    bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  }
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  {  // 取数据
    auto bucket_page_guard = bpm_->FetchPageRead(bucket_page_id);
    auto bucket_page = bucket_page_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();
    if (V value{}; bucket_page->Lookup(key, value, cmp_)) {
      result->push_back(std::move(value));
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 * Inserts a key-value pair into the hash table.
 *
 * @param key the key to create
 * @param value the value to be associated with the key
 * @param transaction the current transaction
 * @return true if insert succeeded, false otherwise
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  auto hash = Hash(key);
  page_id_t directory_page_id;
  page_id_t bucket_page_id;
  {
    auto header_page_guard = bpm_->FetchPageWrite(header_page_id_);
    auto header_page = header_page_guard.AsMut<ExtendibleHTableHeaderPage>();
    auto directory_idx = header_page->HashToDirectoryIndex(hash);
    directory_page_id = header_page->GetDirectoryPageId(directory_idx);

    // 不存在对应的 directory，创建一个新的
    if (directory_page_id == INVALID_PAGE_ID) {
      return InsertToNewDirectory(header_page, directory_idx, hash, key, value);
    }
  }

  // 以读方式，获取 directory_page 和 bucket_page_id
  auto directory_page_read_guard = bpm_->FetchPageRead(directory_page_id);
  auto directory_read_page = directory_page_read_guard.As<ExtendibleHTableDirectoryPage>();
  auto bucket_idx = directory_read_page->HashToBucketIndex(hash);
  bucket_page_id = directory_read_page->GetBucketPageId(bucket_idx);

  // 没有存在的页面
  if (bucket_page_id == INVALID_PAGE_ID) {
    directory_page_read_guard.Drop();
    auto directory_page_guard = bpm_->FetchPageWrite(directory_page_id);
    return InsertToNewBucket(directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>(), bucket_idx, key, value);
  }

  auto bucket_page_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  if (!bucket_page->IsFull()) {
    return bucket_page->Insert(key, value, cmp_);
  }
  directory_page_read_guard.Drop();

  // bucket 满了，要扩容，重新以写申请 directory_page
  auto directory_page_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();

  while (bucket_page->IsFull()) {
    /**
     * bucket 满了以后
     * 1. 判断 directory 是否还能扩容，如果能就扩容，否则直接返回 false
     * 2. 创建 new_bucket
     * 3. 分裂 bucket 中的元素到 new_bucket
     * 4. 更新 directory 信息
     * 5. 重复上述步骤，知道能将元素插入为止
     */
    // 1. 判断 directory 是否还能扩容，如果能就扩容，否则返回 false
    uint32_t local_depth = directory_page->GetLocalDepth(bucket_idx);
    uint32_t global_depth = directory_page->GetGlobalDepth();
    if (local_depth == global_depth && global_depth == directory_max_depth_) {
      return false;  // 当前 directory 已经插入不下该键值对
    }

    // 2.创建 new_bucket，并获取相关信息
    page_id_t new_bucket_page_id = INVALID_PAGE_ID;
    auto new_bucket_page_guard = bpm_->NewPageGuarded(&new_bucket_page_id).UpgradeWrite();
    auto new_bucket_page = new_bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    new_bucket_page->Init(bucket_max_size_);

    // 3. 分裂 bucket 中的元素到 new_bucket
    MigrateEntries(bucket_page, new_bucket_page, 0, directory_page->GetLocalDepthMask(bucket_idx));

    // 4. 更新 directory 信息
    if (local_depth >= global_depth) {
      directory_page->IncrGlobalDepth();
      bucket_idx = directory_page->HashToBucketIndex(hash);
      global_depth++;
    }

    uint32_t change_sz = 1 << (global_depth - local_depth);
    uint32_t change_idx = bucket_idx & directory_page->GetLocalDepthMask(bucket_idx);
    for (uint32_t i = 0; i < change_sz; ++i) {
      uint32_t cur_idx = (i << local_depth) + change_idx;
      if ((i % 2) == 1) {
        if (bucket_idx == cur_idx) {
          bucket_page = new_bucket_page;
        }
        UpdateDirectoryMapping(directory_page, cur_idx, new_bucket_page_id, local_depth + 1, 0);
      } else {
        directory_page->IncrLocalDepth(cur_idx);
      }
    }
    // 5. 重复上述过程，直到能桶不再满
  }
  directory_page_guard.Drop();
  return bucket_page->Insert(key, value, cmp_);
}

/*****************************************************************************
 * REMOVE
 * TODO(P2): Add implementation
 * Removes a key-value pair from the hash table.
 *
 * @param key the key to delete
 * @param value the value to delete
 * @param transaction the current transaction
 * @return true if remove succeeded, false otherwise
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  auto hash = Hash(key);
  page_id_t directory_page_id;
  page_id_t bucket_page_id;

  {  // 获取 directory_page_id
    auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
    auto header_page = header_page_guard.As<ExtendibleHTableHeaderPage>();
    directory_page_id = header_page->GetDirectoryPageId(header_page->HashToDirectoryIndex(hash));
  }

  // 不存在对应 directory，则删除失败
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto directory_page_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_idx = directory_page->HashToBucketIndex(hash);
  bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  // 对应页不存在
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto bucket_page_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  if (!bucket_page->Remove(key, cmp_)) {
    return false;  // 删除失败
  }
  bucket_page_guard.Drop();

  auto bucket_read_guard = bpm_->FetchPageRead(bucket_page_id);
  auto read_bucket_page = bucket_read_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();
  // 如果删除元素后 bucket 为空，就可以进行合并
  while (directory_page->GetGlobalDepth() != 0) {
    /**
     * 1. 找到 img bucket 的相关信息
     * 2. 判断是否可以合并
     *      1> 二者 local depth 是否相等
     *      2> merge bucket 或 bucket 有一个为空
     * 3. 修改 directory 信息
     * 5. 检查是否可以收缩 global_depth
     * 6. 如果合并后 merge bucket 还是空的，重复上述过程
     */
    // 没有 merge bucket
    uint32_t local_depth = directory_page->GetLocalDepth(bucket_idx);
    if (local_depth == 0) {
      directory_page->SetBucketPageId(bucket_idx, INVALID_PAGE_ID);
      return true;
    }
    // 1. 找到 merge bucket 的相关信息
    uint32_t merge_bucket_idx = bucket_idx ^ (1 << (local_depth - 1));
    page_id_t merge_bucket_page_id = directory_page->GetBucketPageId(merge_bucket_idx);
    auto merge_bucket_page_guard = bpm_->FetchPageRead(merge_bucket_page_id);
    auto merge_bucket_page = merge_bucket_page_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();

    // 2. 判断二者 local depth 是否相等，只有相等才能合并
    // bucket 和 merge_bucket 都不为空直接返回
    if (directory_page->GetLocalDepth(merge_bucket_idx) != local_depth ||
        (!merge_bucket_page->IsEmpty() && !read_bucket_page->IsEmpty())) {
      return true;
    }

    // 让 merge bucket 是空的桶
    if (read_bucket_page->IsEmpty()) {
      bucket_read_guard = std::move(merge_bucket_page_guard);
      bpm_->DeletePage(bucket_page_id);
      read_bucket_page = merge_bucket_page;
      bucket_page_id = merge_bucket_page_id;
    } else {
      // merge bucket_page 为空，直接释放页面
      merge_bucket_page_guard.Drop();
      bpm_->DeletePage(merge_bucket_page_id);
    }

    // 3. 修改 directory 信息
    uint32_t change_idx = bucket_idx & ((1 << (local_depth - 1)) - 1);
    uint32_t change_sz = 1 << (directory_page->GetGlobalDepth() - local_depth + 1);
    for (uint32_t i = 0; i < change_sz; ++i) {
      int cur_idx = (i << (local_depth - 1)) + change_idx;
      UpdateDirectoryMapping(directory_page, cur_idx, bucket_page_id, local_depth - 1, 0);
    }

    // 5. 检查是否可以收缩 global_depth
    while (directory_page->CanShrink()) {
      directory_page->DecrGlobalDepth();
    }
    // 6. 如果合并后 bucket 还是空的，重复上述过程
  }
  return true;
}

/*****************************************************************************
 * HELPER functional impl
 *****************************************************************************/

/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the down-casted 32-bit hash
 */
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Hash(K key) const -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

/**
 * 创建新 directory
 */
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t new_directory_page_id;
  auto directory_page_guard = bpm_->NewPageGuarded(&new_directory_page_id).UpgradeWrite();
  if (new_directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto directory_page = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();
  directory_page->Init(directory_max_depth_);
  header->SetDirectoryPageId(directory_idx, new_directory_page_id);

  return InsertToNewBucket(directory_page, directory_page->HashToBucketIndex(hash), key, value);
}

/**
 * 创建新 bucket
 */
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t new_bucket_page_id;
  auto bucket_page_guard = bpm_->NewPageGuarded(&new_bucket_page_id).UpgradeWrite();
  if (new_bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket_page->Init(bucket_max_size_);
  directory->SetBucketPageId(bucket_idx, new_bucket_page_id);
  return bucket_page->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);
  directory->SetLocalDepth(new_bucket_idx, new_local_depth);
}

// 将 old_bucket 中的元素分裂到 new_bucket
template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t new_bucket_idx, uint32_t local_depth_mask) {
  local_depth_mask++;
  for (uint32_t i = 0; i < old_bucket->Size();) {
    if ((Hash(old_bucket->KeyAt(i)) & local_depth_mask) != 0U) {
      auto &entry = old_bucket->EntryAt(i);
      new_bucket->Insert(std::move(entry.first), std::move(entry.second), cmp_);
      old_bucket->RemoveAt(i);
    } else {
      i++;
    }
  }
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
