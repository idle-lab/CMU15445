//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  throw Exception("BufferPoolManager is not implemented yet.");
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

void BufferPoolManager::PinPage(page_id_t page_id) {
  frame_id_t fid = page_table_[page_id];

  replacer_->RecordAccess(fid);
  if (pages_[fid].pin_count_ == 0) {
    replacer_->SetEvictable(fid, false);
  }
  pages_[fid].pin_count_++;
}

auto BufferPoolManager::GetPageFromDisk(page_id_t page_id) -> Page * {
  // 找到可用的 frame
  frame_id_t fid = INVALID_PAGE_ID;
  if (!free_list_.empty()) {
    fid = *free_list_.begin();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&fid)) {
      return nullptr;
    }
    Flush(pages_[fid].GetPageId());
    page_table_.erase(pages_[fid].GetPageId());
    ResetPage(fid);
  }
  // 没有可用的 frame
  if (fid == INVALID_PAGE_ID) {
    return nullptr;
  }

  pages_[fid].page_id_ = page_id;
  page_table_[page_id] = fid;

  PinPage(page_id);
  return &pages_[fid];
}

void BufferPoolManager::ResetPage(frame_id_t frame_id) {
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].ResetMemory();
}

auto BufferPoolManager::Flush(page_id_t page_id) -> bool {
  auto &page = pages_[page_table_[page_id]];
  if (page.IsDirty()) {
    auto pro = disk_scheduler_->CreatePromise();
    auto fut = pro.get_future();
    disk_scheduler_->Schedule(DiskRequest{/*is_write=*/true, /*data=*/page.GetData(),
                                          /*page_id=*/page.page_id_, std::move(pro)});
    if (fut.get()) {
      page.is_dirty_ = false;
      return true;
    }
    return false;  // 写出失败
  }
  return true;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lk(latch_);
  *page_id = AllocatePage();
  return GetPageFromDisk(*page_id);
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lk(latch_);
  if (page_table_.find(page_id) != page_table_.end()) {
    // 已经在内存中，直接返回
    PinPage(page_id);
    return &pages_[page_table_[page_id]];
  }
  // 不存在，要从磁盘调页面
  auto page = GetPageFromDisk(page_id);
  if (page == nullptr) {
    return nullptr;
  }
  auto pro = disk_scheduler_->CreatePromise();
  auto fut = pro.get_future();
  disk_scheduler_->Schedule(DiskRequest{/*is_write=*/false, /*data=*/page->GetData(),
                                        /*page_id=*/page->page_id_, std::move(pro)});

  // 等到读取到页面后返回值。
  if (fut.get()) {
    return page;
  }
  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lk(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  auto *page_ptr = &pages_[page_table_[page_id]];
  if (page_ptr->pin_count_ <= 0) {
    return false;
  }
  // 记录是否被修改过
  page_ptr->is_dirty_ |= is_dirty;

  page_ptr->pin_count_--;
  if (page_ptr->pin_count_ == 0) {
    replacer_->SetEvictable(page_table_[page_id], true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lk(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  return Flush(page_id);
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lk(latch_);
  for (auto &tmp : page_table_) {
    Flush(tmp.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lk(latch_);
  if (pages_[page_table_[page_id]].pin_count_ > 0) {
    return false;
  }
  frame_id_t fid = page_table_[page_id];

  // 脏页先刷新，再删除
  Flush(page_id);

  // 移出 replacer，并将 frame 加入 free_list
  replacer_->Remove(fid);
  free_list_.emplace_back(fid);
  // 清空页面
  ResetPage(fid);

  // 从页表移除页面
  page_table_.erase(page_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
