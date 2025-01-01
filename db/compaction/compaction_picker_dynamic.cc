#include "db/compaction/compaction_picker_dynamic.h"
#include "rocksdb/advanced_options.h"
#include "db/compaction/dynamic_state.h"
#include "logging/logging.h"

#include <string>
#include <unordered_map>
#include <iostream>
#include <chrono>

namespace ROCKSDB_NAMESPACE {
bool DynamicCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  return true;
}

namespace {
class DynamicCompactionBuilder {
 public:
  DynamicCompactionBuilder(const std::string& cf_name, VersionStorageInfo* vstorage,
                       CompactionPicker* compaction_picker,
                       LogBuffer* log_buffer,
                       const MutableCFOptions& mutable_cf_options,
                       const ImmutableOptions& ioptions,
                       const MutableDBOptions& mutable_db_options):
                        cf_name_(cf_name),
                        vstorage_(vstorage),
                        compaction_picker_(compaction_picker),
                        log_buffer_(log_buffer),
                        mutable_cf_options_(mutable_cf_options),
                        ioptions_(ioptions),
                        mutable_db_options_(mutable_db_options) {}
  std::shared_ptr<State> GetCurrentState() {
    std::shared_ptr<State> cur_state = std::make_shared<State>();
    cur_state->level_files.resize(ioptions_.num_levels);
    cur_state->comp_controller = mutable_cf_options_.comp_controller;
    cur_state->timestamp = 0;
    cur_state->max_path_id = 0;
    for (int i = 0; i < ioptions_.num_levels; i++) {
      auto& files = vstorage_->LevelFiles(i);
      for (auto& file : files) {
        cur_state->level_files[i].push_back(FileMeta{.file_id=file->fd.GetNumber(), .file_size=file->fd.file_size});
        if (file->fd.GetNumber() > cur_state->max_path_id) {
          cur_state->max_path_id = file->fd.GetNumber();
        }
      }
    }
    cur_state->InitEncoding();

    return cur_state;
  }

  uint32_t GetPathId(
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, int level) {
    uint32_t p = 0;
    assert(!ioptions.cf_paths.empty());

    // size remaining in the most recent path
    uint64_t current_path_size = ioptions.cf_paths[0].target_size;

    uint64_t level_size;
    int cur_level = 0;

    // max_bytes_for_level_base denotes L1 size.
    // We estimate L0 size to be the same as L1.
    level_size = mutable_cf_options.max_bytes_for_level_base;

    // Last path is the fallback
    while (p < ioptions.cf_paths.size() - 1) {
      if (level_size <= current_path_size) {
        if (cur_level == level) {
          // Does desired level fit in this path?
          return p;
        } else {
          current_path_size -= level_size;
          if (cur_level > 0) {
            if (ioptions.level_compaction_dynamic_level_bytes) {
              // Currently, level_compaction_dynamic_level_bytes is ignored when
              // multiple db paths are specified. https://github.com/facebook/
              // rocksdb/blob/main/db/column_family.cc.
              // Still, adding this check to avoid accidentally using
              // max_bytes_for_level_multiplier_additional
              level_size = static_cast<uint64_t>(
                  level_size * mutable_cf_options.max_bytes_for_level_multiplier);
            } else {
              level_size = static_cast<uint64_t>(
                  level_size * mutable_cf_options.max_bytes_for_level_multiplier *
                  mutable_cf_options.MaxBytesMultiplerAdditional(cur_level));
            }
          }
          cur_level++;
          continue;
        }
      }
      p++;
      current_path_size = ioptions.cf_paths[p].target_size;
    }
    return p;
  }

  Compaction* PickCompaction() {
    auto cur_state = GetCurrentState();
    ROCKS_LOG_BUFFER(log_buffer_, "The current state: %s", cur_state->encoding.c_str());
    auto start_time = std::chrono::high_resolution_clock::now();
    cur_state->UpdatePolicy();
    auto end_time = std::chrono::high_resolution_clock::now();
    ROCKS_LOG_BUFFER(log_buffer_, "Update policy time: %ld", std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count());
    Action act = cur_state->GetBestAction();
    ROCKS_LOG_BUFFER(log_buffer_, "The best action: %s", act.ToString().c_str());
    ROCKS_LOG_BUFFER(log_buffer_, "All next states: %s", cur_state->ToString().c_str());

    if (act.start_level < 0) {
      return nullptr;
    }
    start_level_ = act.start_level;
    if (start_level_ != cur_state->level_files.size() - 1) {
      output_level_ = act.start_level + 1;
    } else {
      // compact the last level
      output_level_ = act.start_level;
    }

    auto& files = vstorage_->LevelFiles(act.start_level);
    CompactionInputFiles first_level;
    first_level.level = start_level_;
    for (auto& file : files) {
      first_level.files.push_back(file);
    }
    compaction_inputs_.push_back(first_level);
    if (!act.create_new) {
      CompactionInputFiles second_level;
      second_level.level = output_level_;
      auto& files = vstorage_->LevelFiles(output_level_);
      for (auto& file : files) {
        if (file->fd.GetNumber() == act.file_id) {
          second_level.files.push_back(file);
          break;
        }
      }
      compaction_inputs_.push_back(second_level);
    }

    auto c = new Compaction(
      vstorage_, ioptions_, mutable_cf_options_, mutable_db_options_,
      std::move(compaction_inputs_), output_level_,
      /* max file size */100UL * (1<<30),
      mutable_cf_options_.max_compaction_bytes,
      GetPathId(ioptions_, mutable_cf_options_, output_level_),
      GetCompressionType(vstorage_, mutable_cf_options_, output_level_,
                         vstorage_->base_level()),
      GetCompressionOptions(mutable_cf_options_, vstorage_, output_level_),
      Temperature::kUnknown,
      /* max_subcompactions */ 0, std::move(grandparents_), is_manual_,
      /* trim_ts */ "", /* start_level_score*/ 1, false /* deletion_compaction */,
      /* l0_files_might_overlap */ true,
      compaction_reason_);
    return c;
  }
 private:
  const std::string& cf_name_;
  VersionStorageInfo* vstorage_;
  CompactionPicker* compaction_picker_;
  LogBuffer* log_buffer_;
  int start_level_ = -1;
  int output_level_ = -1;
  int parent_index_ = -1;
  int base_index_ = -1;
  double start_level_score_ = 0;
  bool is_manual_ = false;
  bool is_l0_trivial_move_ = false;
  CompactionInputFiles start_level_inputs_;
  std::vector<CompactionInputFiles> compaction_inputs_;
  CompactionInputFiles output_level_inputs_;
  std::vector<FileMetaData*> grandparents_;
  CompactionReason compaction_reason_ = CompactionReason::kUnknown;

  const MutableCFOptions& mutable_cf_options_;
  const ImmutableOptions& ioptions_;
  const MutableDBOptions& mutable_db_options_;
};
} // namespace

Compaction* DynamicCompactionPicker::PickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer) {
  if (!mutable_cf_options.comp_controller->Get()) {
    // no need compaction
    return nullptr;
  }

  mutable_cf_options.comp_controller->HandleCompaction();
  DynamicCompactionBuilder builder(cf_name, vstorage, this, log_buffer,
                               mutable_cf_options, ioptions_,
                               mutable_db_options);
  log_buffer->FlushBufferToLog();
  return builder.PickCompaction();
}
} // namespace ROCKSDB_NAMESPACE