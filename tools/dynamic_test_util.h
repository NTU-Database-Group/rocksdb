#pragma once

#include "rocksdb/options.h"
#include "rocksdb/experimental.h"

#include "dynamic_thread_pool.h"
#include "dynamic_test_monitor.h"

#include <thread>
#include <chrono>
#include <iostream>
#include <random>
#include <queue>
#include <list>

const uint64_t MAX_KEY_VAL = 9999999999UL;
const uint64_t MIN_KEY_VAL = 0;

const int CPU_NUM = 32  ;

class WorkloadManager {
 private:
  enum OpType : char {
    RANGE_LOOKUP = 0x0,
    UPDATE = 0x1,
  };

  struct WorkloadWindow {
    int total_range_lookup_cnt;
    int total_update_cnt;
    std::vector<OpType> ops;
    int cur_op_idx;

    int intensity; // in us
  };

  ThreadPool pool;

  std::vector<std::shared_ptr<WorkloadWindow>> workloads_;
  int cur_window_idx_;
  int key_size_;
  int value_size_;
  int range_lookup_len_;
  std::atomic<int> update_cnt;
  uint64_t buffer_size_;
  std::list<int> range_lookup_workload_brief_;
  std::list<int> update_workload_brief_;
  std::unordered_map<uint64_t, bool> existing_keys_;
  rocksdb::AtomicCompactionController* compaction_controller_;
  DynamicTestLogger* test_monitor_;

  void ProcessWindow(rocksdb::DB* db) {
    auto window = workloads_[cur_window_idx_];
    while (window->cur_op_idx < (int)window->ops.size()) {
      if (window->ops[window->cur_op_idx] == OpType::RANGE_LOOKUP) {
        auto key = GenExistingKey();
        PadStringWithPrefix(key_size_, key);
        pool.enqueue([=]() {
          auto it = db->NewIterator(rocksdb::ReadOptions());
          std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
          it->Seek(key);
          for (int i = 0; i < range_lookup_len_; i++) {
            if (!it->Valid()) {
              break;
            }
            it->Next();
          }
          std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
          test_monitor_->Log(MonitorOpType::RANGE_LOOKUP, std::to_string(std::chrono::duration_cast<std::chrono::microseconds>(end - start).count()));
          delete it;
        });
      } else {
        // do update
        auto key = GenNewKey();
        auto value = key;
        PadStringWithPrefix(key_size_, key);
        PadStringWithPrefix(value_size_, value);
        pool.enqueue([=]() {
          std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
          auto s = db->Put(rocksdb::WriteOptions(), key, value);
          std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
          test_monitor_->Log(MonitorOpType::UPDATE, std::to_string(std::chrono::duration_cast<std::chrono::microseconds>(end - start).count()));
          if (!s.ok()) {
            std::cout << "fail to put key: " << s.ToString() << std::endl;
            return;
          }
          update_cnt++;
        });
      }
      if ((uint64_t)update_cnt.load() * (key_size_ + value_size_) >= buffer_size_) {
        update_cnt.store(0);
        db->Flush(rocksdb::FlushOptions());
      }
      std::this_thread::sleep_for(std::chrono::microseconds(window->intensity));
      window->cur_op_idx++;
    }
  }
  void PadStringWithPrefix(int length, std::string& str) {
    if ((int)str.size() < length) {
      str.insert(0, length - str.size(), '0');
    }
  }
  std::string GenNewKey() {
    auto rand_val = std::rand();
    if (rand_val % 20480 <= 10) {
      return std::to_string(MAX_KEY_VAL);
    } else if (rand_val % 20480 <= 20) {
      return std::to_string(MIN_KEY_VAL);
    } else {
      return std::to_string(rand_val % MAX_KEY_VAL);
    }
  }
  std::string GenExistingKey() {
    if (existing_keys_.size() == 0) {
      return "";
    }
    auto it = existing_keys_.begin();
    std::advance(it, std::rand() % existing_keys_.size());
    return std::to_string(it->first);
  }
 public:
  void InitWorkload(int window_num, int window_ops_cnt, int window_intensity) {
    std::srand(0);
    for (int i = 0; i < window_num; i++) {
      auto window = std::make_shared<WorkloadWindow>();
      // randomly generated workload
      // int rnd = 50;
      int rnd = std::rand() % 100;
      window->total_range_lookup_cnt = window_ops_cnt * rnd / 100;
      window->total_update_cnt = window_ops_cnt - window->total_range_lookup_cnt;
      window->cur_op_idx = 0;
      window->intensity = window_intensity; // 100us by default
      for (int j = 0; j < window->total_range_lookup_cnt; j++) {
        window->ops.push_back(OpType::RANGE_LOOKUP);
      }
      for (int j = 0; j < window->total_update_cnt; j++) {
        window->ops.push_back(OpType::UPDATE);
      }
      std::shuffle(window->ops.begin(), window->ops.end(), std::default_random_engine());
      workloads_.push_back(window);
      range_lookup_workload_brief_.push_back(window->total_range_lookup_cnt);
      update_workload_brief_.push_back(window->total_update_cnt);
    }
  }
  void StartProcessing(rocksdb::DB* db) {
    for (int i = 0; i < (int)workloads_.size(); i++) {
      cur_window_idx_ = i;
      test_monitor_->Log(MonitorOpType::WINDOW_START, "window #" + std::to_string(i) + ", range lookup cnt: " + std::to_string(range_lookup_workload_brief_.front()) + ", update cnt: " + std::to_string(update_workload_brief_.front()));
      ProcessWindow(db);
      range_lookup_workload_brief_.pop_front();
      update_workload_brief_.pop_front();
      compaction_controller_->PrepareCompaction(
        std::vector(range_lookup_workload_brief_.begin(), range_lookup_workload_brief_.end()),
        std::vector(update_workload_brief_.begin(), update_workload_brief_.end())
      );
      if (db->GetOptions().compaction_style == rocksdb::kCompactionStyleDynamic) {
        db->SuggestCompactRange(db->DefaultColumnFamily(), nullptr, nullptr);
      }
      test_monitor_->Log(MonitorOpType::WINDOW_END, "window #" + std::to_string(i));
    }
  }
  WorkloadManager(rocksdb::AtomicCompactionController* comp, DynamicTestLogger* logger,  int key_size = 24, int value_size = 1000, int range_lookup_len = 16, uint64_t buffer_size = 2 * (1<<20)) :
    key_size_(key_size), value_size_(value_size), range_lookup_len_(range_lookup_len), buffer_size_(buffer_size), compaction_controller_(comp), test_monitor_(logger), pool(CPU_NUM) {
    cur_window_idx_ = 0;
  }
};