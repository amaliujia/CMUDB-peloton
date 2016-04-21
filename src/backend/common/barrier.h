//
// Created by Rui Wang on 16-4-20.
//

#pragma once

#include <mutex>

namespace peloton {


class Barrier {
public:
  Barrier(size_t bound) : bound_(bound) { }

  void Notify() {
    std::unique_lock lock(lock_);
    count_++;
    lock.unlock();
    cond_.notify_one();
  }

  void Wait() {
    std::unique_lock lock(lock_);
    while (count_ < bound_) {
      cond_.wait(lock);
    }
    lock.unlock();
  }

private:
  std::mutex lock_;
  std::condition_variable cond_;

  size_t bound_ = 0;
  size_t count_ = 0;
};

}   // namespace peloton
