#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  // TODO(fall2023): implement me!
  if (read_ts < watermark_) {
    watermark_ = read_ts;
  }
  current_reads_[read_ts]++;
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  if (current_reads_[read_ts] <= 0) {
    throw Exception("number of read ts < 0");
  }
  current_reads_[read_ts]--;
  while (!current_reads_.empty() && current_reads_[watermark_] == 0) {
    current_reads_.erase(watermark_);
    watermark_++;
  }
}

}  // namespace bustub
