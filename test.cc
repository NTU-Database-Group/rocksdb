#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include <algorithm>
#include <vector>
#include <string>
#include <iostream>

std::string pad_num(uint64_t num, int size) {
  std::string n = std::to_string(num);
  if (n.size() < size) {
    std::string padding = std::string("0", size - n.size());
    n = padding + n;
  }
  return n;
}

int main() {
  rocksdb::DB* db;
  rocksdb::Options options;
  options.compression = rocksdb::kNoCompression;
  options.level_compaction_dynamic_level_bytes = false;
  options.create_if_missing = true;
  rocksdb::Status s;
  // s = rocksdb::DB::Open(options, "/tmp/db", &db);
  // if (!s.ok()) {
  //   std::cout << s.ToString() << std::endl;
  //   exit(0);
  // }
  std::vector<uint64_t> nums;
  for (int i = 0; i < 20000; i++) {
    nums.push_back(i);
  }
  std::random_shuffle(nums.begin(), nums.end());
  // for (auto num : nums) {
  //   auto s = db->Put(rocksdb::WriteOptions(), pad_num(num, 24), pad_num(num, 3048));
  //   if (!s.ok()) {
  //     std::cout << s.ToString() << std::endl;
  //     exit(0);
  //   }
  // }
  // s = db->Flush(rocksdb::FlushOptions());
  // db->Close();
  options.statistics = rocksdb::CreateDBStatistics();
  options.use_direct_reads = true;
  rocksdb::BlockBasedTableOptions table_options;
  table_options.no_block_cache = true;
  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
  s = rocksdb::DB::Open(options, "/tmp/db", &db);
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
    exit(0);
  }
  for (auto num : nums) {
    std::string val;
    s = db->Get(rocksdb::ReadOptions(), pad_num(num, 24), &val);
    if (!s.ok()) {
      std::cout << s.ToString() << std::endl;
      exit(0);
    }
  }
  // std::cout << options.statistics->ToString() << std::endl;
  delete db;
  return 0;
}