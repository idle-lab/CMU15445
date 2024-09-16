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

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager), background_threads_(5) {
  // Spawn the background thread
  for (auto &thread : background_threads_) {
    thread = std::thread([this] { StartWorkerThread(); });
  }
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  for (int i = 0; i < 5; ++i) {
    request_queue_.Put(std::nullopt);
  }
  for (auto &thread : background_threads_) {
    thread.join();
  }
  disk_manager_->ShutDown();
}

void DiskScheduler::Schedule(DiskRequest r) { request_queue_.Put(std::make_optional(std::move(r))); }

void DiskScheduler::StartWorkerThread() {
  for (;;) {
    auto t = request_queue_.Get();
    if (t == std::nullopt) {
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
