#pragma once

#include <chrono>
#include <thread>
#include <mutex>
#include <unordered_map>
#include <string>
#include <iostream>

#include "rocksdb/listener.h"

enum MonitorOpType : char {
  RANGE_LOOKUP = 0x0,
  UPDATE = 0x1,
  FLUSH = 0x2,
  COMPACTION_START = 0x3,
  COMPACTION_END = 0x4,
  WINDOW_START = 0x5,
  WINDOW_END = 0x6,
};

const std::unordered_map<MonitorOpType, std::string> opTypeToStr = {
  {RANGE_LOOKUP, "RANGE_LOOKUP"},
  {UPDATE, "UPDATE"},
  {FLUSH, "FLUSH"},
  {COMPACTION_START, "COMPACTION_START"},
  {COMPACTION_END, "COMPACTION_END"},
  {WINDOW_START, "WINDOW_START"},
  {WINDOW_END, "WINDOW_END"},
};

class DynamicTestLogger {
 private:
  std::chrono::high_resolution_clock::time_point start_;
  std::mutex mtx_;
  
  uint64_t get_timestamp() {
    return (uint64_t)std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::high_resolution_clock::now() - start_).count();
  }
 public:
  DynamicTestLogger() {
    start_ = std::chrono::high_resolution_clock::now();
  }
  
  void Log(MonitorOpType op, const std::string& msg) {
    std::lock_guard<std::mutex> lock(mtx_);
    std::cout << get_timestamp() << "[" << opTypeToStr.at(op) << "]: " << msg << std::endl;
  }
};

class DynamicTestListener : public rocksdb::EventListener {
 private:
  DynamicTestLogger* logger_;
 public:
  virtual void OnFlushCompleted(rocksdb::DB* db, const rocksdb::FlushJobInfo& flush_job_info) override {
    logger_->Log(FLUSH, std::to_string(flush_job_info.file_number));
  }

  virtual void OnCompactionCompleted(rocksdb::DB* db, const rocksdb::CompactionJobInfo& ci) override {
    std::string msg;
    std::unordered_map<int, int> input_file_cnt;
    std::unordered_map<int, int> output_file_cnt;
    std::unordered_map<int, int> l0_files;
    for (const auto& f : ci.input_file_infos) {
      input_file_cnt[f.level]++;
      if (f.level == 0) {
        l0_files[f.file_number]++;
      }
    }
    for (const auto& f : ci.output_file_infos) {
      output_file_cnt[f.level]++;
      if (l0_files.size() > 0) {
        if (l0_files.find(f.file_number) != l0_files.end()) {
          // skip trivial move
          return;
        }
      }
    }
    for (auto it : input_file_cnt) {
      msg += "input level " + std::to_string(it.first) + ": " + std::to_string(it.second) + ", ";
    }
    for (auto it : output_file_cnt) {
      msg += "output level " + std::to_string(it.first) + ": " + std::to_string(it.second) + ", ";
    }
    msg.pop_back();

    msg += "total input bytes: " + std::to_string(ci.stats.total_input_bytes) + ", " + "total output bytes: " + std::to_string(ci.stats.total_output_bytes) + ", time: " + std::to_string(ci.stats.elapsed_micros) + " us";

    logger_->Log(COMPACTION_END, msg);
  }

  virtual void OnCompactionBegin(rocksdb::DB* db, const rocksdb::CompactionJobInfo& ci) override {
    logger_->Log(COMPACTION_START, std::to_string(ci.job_id));
  }

  DynamicTestListener(DynamicTestLogger* logger) : logger_(logger) {}
};