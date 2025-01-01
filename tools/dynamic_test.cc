#include "gflags/gflags.h"

#include "dynamic_test_util.h"
#include "rocksdb/statistics.h"

#include "ruskey/db.h"

#include <iostream>
#include <string>
#include <sstream>

DEFINE_uint64(buffer_size, 2 * (1<<20), "buffer size");
DEFINE_string(compaction_style, "dynamic", "compaction style");
DEFINE_uint64(window_size, 10000, "number of operations in a window");
DEFINE_uint64(window_num, 100, "number of windows");
DEFINE_uint64(intensity, 200, "intensity of operations (in us)");
DEFINE_string(size_ratios, "", "size ratios for Moose");
DEFINE_string(run_numbers, "", "run numbers for Moose");
DEFINE_int32(search_depth, 5, "search depth for DynamicCompaction");
DEFINE_int32(walk_depth, 20, "walk depth for DynamicCompaction");
DEFINE_double(gamma, 0.8, "gamma for DynamicCompaction");

template <typename T>
std::vector<T> ParseStringToNumbers(const std::string& src) {
  std::vector<T> res;
  T val;
  std::string tmp = src;
  size_t pos = 0;
  while ((pos = tmp.find(",")) != std::string::npos) {
    std::stringstream ss(tmp.substr(0, pos));
    ss >> val;
    res.push_back(val);
    tmp.erase(0, pos + 1);
  }
  std::stringstream ss(tmp);
  ss >> val;
  res.push_back(val);
  return res;
}

rocksdb::Options GetBasedOptions() {
  rocksdb::Options opt;
  opt.statistics = rocksdb::CreateDBStatistics();
  opt.write_buffer_size = FLAGS_buffer_size * 2;
  opt.create_if_missing = true;
  opt.num_levels = 4;
  opt.force_consistency_checks = false;
  opt.compression = rocksdb::kNoCompression;
  opt.use_direct_io_for_flush_and_compaction = true;
  opt.use_direct_reads = true;
  opt.comp_controller = new rocksdb::AtomicCompactionController(FLAGS_window_size, FLAGS_intensity);
  return opt;
}

rocksdb::Options GetMooseOptions() {
  rocksdb::Options opt = GetBasedOptions();
  std::vector<double> size_ratios = ParseStringToNumbers<double>(FLAGS_size_ratios);
  std::vector<uint64_t> run_numbers = ParseStringToNumbers<uint64_t>(FLAGS_run_numbers);
  uint64_t prev_capacity = FLAGS_buffer_size * size_ratios[0];
  std::vector<uint64_t> run_sizes{prev_capacity};
  for (size_t i = 1; i < size_ratios.size(); i++) {
    uint64_t cur_capacity = prev_capacity * size_ratios[i];
    uint64_t cur_run_size = cur_capacity / run_numbers[i];
    run_sizes.push_back(cur_run_size);
    prev_capacity = cur_capacity;
  }
  opt.num_levels = 64;
  opt.compaction_style = rocksdb::kCompactionStyleMoose;
  opt.level0_slowdown_writes_trigger = size_ratios[0] + 2;
  opt.level0_stop_writes_trigger = size_ratios[0] + 4;
  opt.comp_controller->InitForMoose(size_ratios, run_numbers, run_sizes);
  return opt;
}

rocksdb::Options GetDynamicOptions() {
  rocksdb::Options opt = GetBasedOptions();
  opt.compaction_style = rocksdb::kCompactionStyleDynamic;

  opt.level0_stop_writes_trigger = 0x7fffffff;
  opt.level0_slowdown_writes_trigger = 0x7fffffff;
  // opt.max_write_buffer_number = 10;

  return opt;
}

rocksdb::Options GetLevelingOptions() {
  rocksdb::Options opt = GetBasedOptions();
  opt.use_direct_io_for_flush_and_compaction = true;
  opt.use_direct_reads = true;
  opt.level_compaction_dynamic_level_bytes = false;

  return opt;
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  rocksdb::DB* db;
  rocksdb::Options opt;

  std::shared_ptr<DynamicTestLogger> logger = std::make_shared<DynamicTestLogger>();
  if (FLAGS_compaction_style == "dynamic") {
    opt = GetDynamicOptions();
    opt.comp_controller->InitForDynamicCompaction(FLAGS_search_depth, FLAGS_buffer_size, FLAGS_gamma, FLAGS_walk_depth);
  } else if (FLAGS_compaction_style == "leveling") {
    opt = GetLevelingOptions();
  } else if (FLAGS_compaction_style == "moose") {
    opt = GetMooseOptions();
  } else {
    std::cout << "unknown compaction style: " << FLAGS_compaction_style << std::endl;
    return 0;
  }
  opt.listeners.emplace_back(new DynamicTestListener(logger.get()));
  auto s = rocksdb::DB::Open(opt, "/tmp/db", &db);
  if (!s.ok()) {
    std::cout << "fail to open db: " << s.ToString() << std::endl;
    return 0;
  }
  WorkloadManager mng(opt.comp_controller, logger.get());
  mng.InitWorkload(FLAGS_window_num, FLAGS_window_size, FLAGS_intensity);
  mng.StartProcessing(db);
  std::this_thread::sleep_for(std::chrono::seconds(20));

  std::cout << "stats: " << opt.statistics->ToString() << std::endl;
  db->Close();
  return 0;
}