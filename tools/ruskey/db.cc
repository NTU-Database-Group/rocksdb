#include "./db.h"

#include <unistd.h>
#include <algorithm>
#include <cstdio>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <math.h>
#include <fstream>
#include <sstream>
#include "rocksdb/advanced_options.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"

using namespace ROCKSDB_NAMESPACE;
using namespace std;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\playground";
#else
std::string kDBPath = "/tmp/rocksdb_exp";
#endif

// Default settings
int indicator = 0;
int read_indicator = -1;
int KEY_SIZE = 128;
int VALUE_SIZE = 896;
int ENTRY_SIZE = KEY_SIZE + VALUE_SIZE;
int T = 10;
int BITS_PRE_KEY = 10;
int MISSION_LENGTH = 100000;
int INITIAL_SIZE = 999999;
int INITIAL_POLICY = 10;
int EPOCHS = 1000;
int NUM_ACTIVE_LEVELS = 4;
// 0 for changing policy, 1 for not changing policy
int IS_BASELINE = 0;
long KEY_RANGE = 999999;
// 0 for uniform in range, other remains to be specified
int READ_DISTRIBUTION = 0;
// 0 for random in range, other remains to be specified
int WRITE_DISTRIBUTION = 0;
bool SYNC_POLICY = true;
int USE_MONKEY = 0;
int IS_YCSB = 0;
int GREEDY_APPROACH = 0;
int POLICY_CHECK_LENGTH = 200;
bool IS_LAZYLEVELING = false;
std::string INITIAL_FILE_PATH = "./initial.dat";
std::string QUERY_FILE_PATH = "./query.dat";

uint64_t *last_read_io = nullptr;
uint64_t *last_write_io = nullptr;
uint64_t *last_level_read_time = nullptr;
uint64_t *last_level_write_time = nullptr;
uint64_t *last_bloom_positive = nullptr;
uint64_t *last_bloom_true_positive = nullptr;

int *bit_flag = nullptr;
vector<double> current_state;
vector<double> next_state;

double read_time_spent = 0;
double write_time_spent = 0;
double action_time_spent = 0;

Options options;
DB* db = nullptr;

void load_state(DB* db, Options* option, uint64_t *last_bloom_positive, uint64_t *last_bloom_true_positive, uint64_t *last_write_io, int num_levels, long read_length, long write_length, long mission_length, vector<double> &current_state) {
    double buffer_size = (double)(option->max_bytes_for_level_base * option->max_bytes_for_level_multiplier) / ENTRY_SIZE;
    // Workload Statistics
    // 0, 1, 2
    current_state[0] = read_length / buffer_size;
    current_state[1] = write_length / buffer_size;
    current_state[2] = (double)(read_length) / mission_length;

    vector<uint64_t> compact_bytes_read_, compact_bytes_write_, compact_micros_;
    // TODO: Get by EventListener
    // db->GetStates(compact_bytes_read_, compact_bytes_write_, compact_micros_);
  
    
    // Write Statistics
    // 3, 4, 5, 6
    int idx = 3;
    for (int i = 0; i < num_levels; i++) {
        double diff = (double)(compact_bytes_write_[i+1] - last_write_io[i]) / buffer_size;
        current_state[idx++] = diff / ENTRY_SIZE / 10;
        last_write_io[i] = compact_bytes_write_[i+1];
    }

    // Read Statistics - Bloom Positive
    // 7, 8, 9, 10
    for (int i = 0; i < num_levels; i++) {
        uint64_t positive_stat = 0;
        for (int l = i * option->max_bytes_for_level_multiplier + 1; l < (i + 1) * option->max_bytes_for_level_multiplier + 1; l++) {
            positive_stat += get_positive()[l];
        }
        double diff = (double)(positive_stat - last_bloom_positive[i]) * 10.0;
        current_state[idx++] = diff / read_length;
        last_bloom_positive[i] = positive_stat;
    }

    // Read Statistics - Bloom True Positive
    // 11, 12, 13, 14
    for (int i = 0; i < num_levels; i++) {
        uint64_t true_positive_stat = 0;
        for (int l = i * option->max_bytes_for_level_multiplier + 1; l < (i + 1) * option->max_bytes_for_level_multiplier + 1; l++) {
            true_positive_stat += get_true_positive()[l];
        }
        double diff = (double)(true_positive_stat - last_bloom_true_positive[i]);
        current_state[idx++] = diff / read_length;
        last_bloom_true_positive[i] = true_positive_stat;
    }

    //Policy Statistics
    //15, 16, 17, 18
    for (int i = 0; i < num_levels; i++) {
        current_state[idx++] = option->compaction_options_flex.merge_policy[i + 1];
    }

    // FPR of bloom filter of each levels
    // 19, 20, 21, 22
    for (int i = 0; i < num_levels; i++) {

        if (fabs(current_state[3+num_levels+i]) > 1e-3)
            current_state[idx++] = (current_state[3+num_levels+i] - current_state[3+num_levels*2+i]) / current_state[3+num_levels+i];
        else
            current_state[idx++] = 0.0;
    }
    
    ColumnFamilyMetaData cf_meta;
    db->GetColumnFamilyMetaData(&cf_meta);

    int largest_used_level = 0;
    vector<int> level_num_run(num_levels, 0);
    for (auto level: cf_meta.levels) {
        if (level.files.size() > 0) {
            largest_used_level = (level.level - 1) / (int)(option->max_bytes_for_level_multiplier) + 1;
            if(largest_used_level > num_levels)
                break;
            level_num_run[largest_used_level - 1] += 1;
        }
    }

    // Largest Level
    // 23
    //largest_used_level = (largest_used_level - 1) / (int)(option->max_bytes_for_level_multiplier) + 1;
    current_state[idx++] = (double)largest_used_level;

    // Storage of each levels
    // 24, 25, 26, 27
    vector<long> level_storage(num_levels, 0);
    for (auto level: cf_meta.levels) {
      if(level.level == 0)
        continue;
      long level_size = 0;
      for (auto file: level.files) {
        level_size += file.size;
      }
      int n_level = (level.level - 1) / (int)(option->max_bytes_for_level_multiplier);
      if(n_level >= num_levels)
        break;
      level_storage[n_level] += level_size;
    }
   
    for (int i = 0; i < num_levels; i++) {
      uint64_t capacity = option->max_bytes_for_level_base * pow(option->max_bytes_for_level_multiplier, double(i + 1));
      current_state[idx++] = (double)level_storage[i] / capacity;
    }
    
    // Num of runs of each levels
    // 28, 29, 30, 31
    for (int i = 0; i < num_levels; i++) {
      current_state[idx++] = (double)level_num_run[i];
    }
}

rocksdb::DB* InitForRuskey(int key_size, int value_size, int t, int bpk, int mission_length, int initial_size, int initial_policy, int level_num, int use_monkey, int greedy_approach) {
  KEY_SIZE = key_size;
  VALUE_SIZE = value_size;
  ENTRY_SIZE = KEY_SIZE + VALUE_SIZE;
  T = t;
  BITS_PRE_KEY = bpk;
  MISSION_LENGTH = mission_length;
  INITIAL_SIZE = initial_size;
  INITIAL_POLICY = initial_policy;
  // EPOCHS = atoi(argv[8]);
  NUM_ACTIVE_LEVELS = level_num;
  // KEY_RANGE = atoll(argv[11]);
  // READ_DISTRIBUTION = atoi(argv[12]);
  // WRITE_DISTRIBUTION = atoi(argv[13]);
  USE_MONKEY = use_monkey;
  GREEDY_APPROACH = greedy_approach;
  
  // ifstream query_file, initial_file;
  char str[20];
  int actions[NUM_ACTIVE_LEVELS];
  long STATE_DIM = 4 + NUM_ACTIVE_LEVELS*7;
  current_state = vector<double>(STATE_DIM, 0);
  next_state = vector<double>(STATE_DIM, 0);
  last_read_io = (uint64_t *)malloc(NUM_ACTIVE_LEVELS * sizeof(uint64_t));
  last_write_io = (uint64_t *)malloc(NUM_ACTIVE_LEVELS * sizeof(uint64_t));
  last_level_read_time = (uint64_t *)malloc(NUM_ACTIVE_LEVELS * sizeof(uint64_t));
  last_level_write_time = (uint64_t *)malloc(NUM_ACTIVE_LEVELS * sizeof(uint64_t));
  last_bloom_positive = (uint64_t *)malloc(NUM_ACTIVE_LEVELS * sizeof(uint64_t));
  last_bloom_true_positive = (uint64_t *)malloc(NUM_ACTIVE_LEVELS * sizeof(uint64_t));

  srand((unsigned)time(NULL));

  options.create_if_missing = true;
  // TODO: add compaction style
  // options.compaction_style = kCompactionStyleFlex;
  options.compaction_pri = kOldestSmallestSeqFirst;
  options.write_buffer_size = 2 * 1024 * 1024;
  auto processor_count = std::thread::hardware_concurrency();
  if (processor_count > 16) {
      processor_count = 16;
  }
  options.max_background_jobs = 1;
  options.max_subcompactions = 1;
  options.max_background_compactions = 1;
  options.max_background_flushes = 1;
  options.level0_slowdown_writes_trigger = 4;
  options.level0_file_num_compaction_trigger = 4;
  options.level0_stop_writes_trigger = 8;
  options.max_bytes_for_level_multiplier = T;
  options.level_compaction_dynamic_level_bytes = false;
  options.max_bytes_for_level_base = options.write_buffer_size * options.max_bytes_for_level_multiplier;
  options.target_file_size_base = 8 * 1024 * 1024;
  options.use_direct_io_for_flush_and_compaction = true;
  options.use_direct_reads = true;
  options.compression = kNoCompression;
  options.bottommost_compression = kNoCompression;  // kSnappyCompression;
  options.max_open_files = 200000;
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(BITS_PRE_KEY, false));
  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
  options.statistics = rocksdb::CreateDBStatistics();
  options.statistics.get()->set_stats_level(kExceptHistogramOrTimers);
  options.statistics.get()->ToString();
  options.compaction_options_flex.merge_policy = std::vector<int>(options.num_levels, INITIAL_POLICY);
  if(INITIAL_POLICY == 0){
    IS_LAZYLEVELING = true;
    options.compaction_options_flex.merge_policy = std::vector<int>(options.num_levels, 10);
    options.compaction_options_flex.merge_policy[NUM_ACTIVE_LEVELS] = 1;
  }
  if(USE_MONKEY)
    options.compaction_options_flex.use_monkey_layout = true;

  get_perf_context()->Reset();
  SetPerfLevel(rocksdb::PerfLevel::kEnableCount);
  get_perf_context()->EnablePerLevelPerfContext();

  DestroyDB(kDBPath, options);
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());
  assert(db);

  int ACTION_SPACE_SIZE = NUM_ACTIVE_LEVELS * 2 + 1;
  bit_flag = (int *)calloc(ACTION_SPACE_SIZE, sizeof(int));
  int T = (int)(options.max_bytes_for_level_multiplier);
  
  FILE *fp_write_current, *fp_write_next, *fp_read;
  fp_read = fopen("./action.txt", "w");
  fprintf(fp_read, "-1\n");
  fclose(fp_read);

  double ratio = 0.5, true_ratio = 0.5;
  long *read_length = (long *)malloc(sizeof(long));
  long *write_length = (long *)malloc(sizeof(long));
  *read_length = (long)(true_ratio * MISSION_LENGTH);
  *write_length = MISSION_LENGTH - *read_length;
  double *read_time_diff = (double *)malloc(sizeof(double));
  double *write_time_diff = (double *)malloc(sizeof(double));
  double total_read_time = 0, total_write_time = 0;

  int unlock[NUM_ACTIVE_LEVELS];
  int unlock_pre[NUM_ACTIVE_LEVELS];
  vector <int> policy_buffer;

  for (int i = 0; i < NUM_ACTIVE_LEVELS; i++) {
    last_read_io[i] = 0;
    last_write_io[i] = 0;
    last_level_read_time[i] = 0;
    last_level_write_time[i] = 0;
    last_bloom_positive[i] = 0;
    last_bloom_true_positive[i] = 0;
  }
  load_state(db, &options, 
                last_bloom_positive, last_bloom_true_positive, 
                last_write_io, NUM_ACTIVE_LEVELS, 
                *read_length, *write_length, 
                MISSION_LENGTH, current_state);
}

void PrepareForWorkload() {
  // set stat to zero
  read_time_spent = 0, write_time_spent = 0, action_time_spent = 0;
}

void PostProcessForWorkload(long read_length, long write_length) {
  load_state(db, &options, 
            last_bloom_positive, last_bloom_true_positive, 
            last_write_io, NUM_ACTIVE_LEVELS, 
            read_length, write_length, 
            MISSION_LENGTH, next_state);

  FILE *fp_write_next;
  long STATE_DIM = 4 + NUM_ACTIVE_LEVELS*7;
  int ACTION_SPACE_SIZE = NUM_ACTIVE_LEVELS * 2 + 1;
  fp_write_next = fopen("./next_state.txt", "w");
  fprintf(fp_write_next, "%d\n", indicator);
  indicator += 1;
  for (int i = 0; i < STATE_DIM; i++) {
      fprintf(fp_write_next, "%f\n", next_state[i]);
  }
  for (int i = 0; i < ACTION_SPACE_SIZE; i++) {
      fprintf(fp_write_next, "%f\n", (double)(bit_flag[i]));
  }
  fprintf(fp_write_next, "%f\n", write_time_spent);
  fprintf(fp_write_next, "%f\n", read_time_spent);
  fprintf(fp_write_next, "%f\n", action_time_spent);
  vector<uint64_t> read_nanos(options.num_levels, 0);
  for(int level = 0; level < options.num_levels; level++){
      for (int l = level * options.max_bytes_for_level_multiplier + 1;
          l < (level + 1)*options.max_bytes_for_level_multiplier + 1; l++){
          read_nanos[level] += (*(get_perf_context()->level_to_perf_context))[l]
              .get_from_table_nanos;
      }
  }
  // TODO: Get from EventListener
  // for (int i = 0; i < NUM_ACTIVE_LEVELS; i++) {
  //     double diff = (double)(compact_micros_[i+1] - last_level_write_time[i]);
  //     diff /= 1000000.0;
  //     fprintf(fp_write_next, "%f\n", (double)(diff));
  //     last_level_write_time[i] = compact_micros_[i+1];
  // }
  for (int i = 0; i < NUM_ACTIVE_LEVELS; i++) {
      double diff = (double)(read_nanos[i] - last_level_read_time[i]);
      diff /= 1000000000.0;
      fprintf(fp_write_next, "%f\n", (double)(diff));
      last_level_read_time[i] = read_nanos[i];
  }
  fclose(fp_write_next);

  current_state = next_state;
  next_state.assign(next_state.size(), 0);
}