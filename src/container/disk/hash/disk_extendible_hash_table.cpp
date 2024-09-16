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
    if (V value; bucket_page->Lookup(key, value, cmp_)) {
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

  // 以写方式，获取 directory_page 和 bucket_page_id
  auto directory_page_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_idx = directory_page->HashToBucketIndex(hash);
  bucket_page_id = directory_page->GetBucketPageId(bucket_idx);

  // 没有存在的页面
  if(bucket_page_id == INVALID_PAGE_ID) {
    return InsertToNewBucket(directory_page,bucket_idx,key,value);
  }

  auto bucket_page_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  while (bucket_page->IsFull()) {
    /**
     * bucket 满了以后
     * 1. 判断 directory 是否还能扩容，如果能就扩容，否则直接返回 false
     * 2. 创建 new_bucket
     * 3. 更新 directory 信息
     * 4. 分裂 bucket 中的元素到 new_bucket
     * 5. 重复上述步骤，知道能将元素插入为止
     */
    // 1. 判断 directory 是否还能扩容，如果能就扩容，否则返回 false
    if (directory_page->GetLocalDepth(bucket_idx) == directory_page->GetGlobalDepth() &&
        directory_page->GetGlobalDepth() == directory_page->GetMaxDepth()) {
      return false;  // 当前 directory 已经插入不下该键值对
    }

    // 2.创建 new_bucket，并获取相关信息
    page_id_t new_bucket_page_id;
    auto new_bucket_page_guard = bpm_->NewPageGuarded(&new_bucket_page_id).UpgradeWrite();
    auto new_bucket_page = new_bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    new_bucket_page->Init(bucket_max_size_);
    auto new_bucket_idx = directory_page->GetSplitImageIndex(bucket_idx);

    // 3. 更新 directory 信息
    if (directory_page->GetLocalDepth(bucket_idx) >= directory_page->GetGlobalDepth()) {
      directory_page->IncrGlobalDepth();
    }
    for(uint32_t i = 0;i < directory_page->Size();++i) {
      
    }
    directory_page->IncrLocalDepth(bucket_idx);
    directory_page->SetLocalDepth(new_bucket_idx,directory_page->GetLocalDepth(bucket_idx));
    directory_page->SetBucketPageId(new_bucket_idx,new_bucket_page_id);

    // 4. 分裂 bucket 中的元素到 new_bucket
    MigrateEntries(bucket_page, new_bucket_page, new_bucket_idx, directory_page->GetLocalDepthMask(bucket_idx));

    // 5. 重复上述过程，直到能桶不再满
  }

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

  auto bucket_page_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if (!bucket_page->Remove(key, cmp_)) {
    return false;
  }
  // 没有 img bucket
  if(directory_page->GetLocalDepth(bucket_idx) == 0) {
    directory_page->SetBucketPageId(bucket_idx,INVALID_PAGE_ID);
    return true;
  }
  // 如果删除元素后 bucket 为空，就可以进行合并
  while (bucket_page->IsEmpty() && directory_page->GetGlobalDepth() != 0) {
    uint32_t merge_bucket_idx = bucket_idx ^ (1 << (directory_page->GetLocalDepth(bucket_idx) - 1));
    page_id_t merge_bucket_page_id = directory_page->GetBucketPageId(merge_bucket_idx);
    if(directory_page->GetLocalDepth(merge_bucket_idx) != directory_page->GetLocalDepth(bucket_idx)) {
      return true;
    }

    directory_page->DecrLocalDepth(merge_bucket_idx);
    directory_page->DecrLocalDepth(bucket_idx);
    directory_page->SetBucketPageId(bucket_idx,merge_bucket_page_id);

    if (directory_page->CanShrink()) {
      directory_page->DecrGlobalDepth();
    }
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
  auto directory_page = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();
  directory_page->Init(directory_max_depth_);
  header->SetDirectoryPageId(directory_idx, new_directory_page_id);

  return InsertToNewBucket(directory_page,directory_page->HashToBucketIndex(hash),key,value);
}

/**
 * 创建新 bucket
*/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t new_bucket_page_id;
  auto bucket_page_guard = bpm_->NewPageGuarded(&new_bucket_page_id).UpgradeWrite();
  directory->SetBucketPageId(bucket_idx, new_bucket_page_id);

  auto bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket_page->Init(bucket_max_size_);
  return bucket_page->Insert(key, value, cmp_);
}

// template <typename K, typename V, typename KC>
// void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
//                                                                uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
//                                                                uint32_t new_local_depth, uint32_t local_depth_mask) {
//   for(uint32_t i = 0;i < directory->Size();++i) {
//     if((i & local_depth_mask) == new_bucket_idx) {
//       directory->SetBucketPageId(i,new_bucket_page_id);
//     }
//   }
// }

// 将 old_bucket 中的元素分裂到 new_bucket
template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t new_bucket_idx, uint32_t local_depth_mask) {
  for (uint32_t i = 0; i < old_bucket->Size();) {
    if ((Hash(old_bucket->KeyAt(i)) & local_depth_mask) == new_bucket_idx) {
      auto entry = old_bucket->EntryAt(i);
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
