#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept { *this = std::move(that); }

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  // 首先释放原来维护的值
  Drop();

  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  return *this;
}

void BasicPageGuard::Drop() {
  if (bpm_ == nullptr || page_ == nullptr) {  // 如果 bpm 或 page 为空，就什么都不做
    return;
  }
  bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  bpm_ = nullptr;
  page_ = nullptr;
}

/**
 * @brief Upgrade a BasicPageGuard to a ReadPageGuard
 * @return an upgraded ReadPageGuard
 */
auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  page_->RLatch();
  return ReadPageGuard(std::move(*this));
}

/**
 * @brief Upgrade a BasicPageGuard to a WritePageGuard
 * @return an upgraded WritePageGuard
 */
auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  page_->WLatch();
  return WritePageGuard(std::move(*this));
}

/**
 * @brief Destructor for BasicPageGuard
 */
BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { *this = std::move(that); }

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  // 先释放旧资源
  Drop();
  guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if (!guard_.Expired()) {
    return;
  }
  // 解锁后释放
  guard_.page_->RUnlatch();
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { *this = std::move(that); }

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  // 先释放旧资源
  Drop();
  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (!guard_.Expired()) {
    return;
  }
  // 解锁后释放
  guard_.is_dirty_ = true;
  guard_.page_->WUnlatch();
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
