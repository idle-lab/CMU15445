//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) { request_queue_.Put(std::make_optional(std::move(r))); }

void DiskScheduler::StartWorkerThread() {
  for (;;) {
    auto t = request_queue_.Get();
    if (t == std::nullopt) {
      disk_manager_->ShutDown();
      return;
    }
    if (t->is_write_) {
      disk_manager_->WritePage(t->page_id_, t->data_);
    } else {
      disk_manager_->ReadPage(t->page_id_, t->data_);
    }
    t->callback_.set_value(true);
  }
}

}  // namespace bustub
