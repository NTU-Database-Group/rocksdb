#pragma once

#include <unordered_map>
#include <string>
#include <memory>
#include <cmath>
#include <iostream>
#include <vector>

#include "rocksdb/advanced_options.h"

namespace ROCKSDB_NAMESPACE {
// typedef std::unordered_map<std::string, double> PolicyMap;

const uint64_t block_size = 4096;
// const double gamma = 0.9;
// TODO: should be relevant to the workload
const double mu = 1e-6; // no transition cost is required
const int entry_size = 1024;
// const uint64_t buffer_size = 2UL * (1<<20);

// TODO: intensity relevant
const double slope = 0.0032;
const double intercept = 24251.22;

struct Action {
  int start_level;
  bool create_new;
  uint64_t file_id;

  std::string ToString() {
    std::string ret;
    ret += "Action: start_level: " + std::to_string(start_level) + ", create_new: " + std::to_string(create_new);
    if (!create_new) {
      ret += ", file_id: " + std::to_string(file_id);
    }
    return ret;
  }
};

struct FileMeta {
  uint64_t file_id;
  uint64_t file_size;
};

struct State {
  // level_files[i] => the files at level i
  // level_files[i][j] => the number of entries in the jth file at level i
  std::vector<std::vector<FileMeta>> level_files;
  double transit_cost;
  // cost of the current state + its successors'
  double total_cost;

  std::string encoding;
  // PolicyMap policy;
  std::string best_nxt_state;
  // next_state.encoding -> next_state
  std::unordered_map<std::string, std::shared_ptr<State>> nxts;
  // next_state.encoding -> Action
  std::unordered_map<std::string, Action> acts;

  AtomicCompactionController* comp_controller;
  int timestamp;
  uint32_t max_path_id;

  void InitEncoding() {
    std::string ret;
    for (size_t i = 0; i < level_files.size(); i++) {
      auto files = level_files[i];
      ret += std::to_string(i) + ":";
      for (auto& file : files) {
        ret += std::to_string(file.file_size) + ",";
      }
      if (ret.back() == ',') {
        ret.pop_back();
      }
      ret += ";";
    }
    this->encoding = ret;
  }

  Action GetMergeAction(int start_level, bool create_new) {
    Action act = Action{.start_level = start_level, .create_new = create_new};
    int target_level = start_level + 1;
    if (create_new || start_level == level_files.size() - 1) {
      return act;
    }
    // find the file with the minimum size
    auto& files = level_files[target_level];
    if (files.size() == 0) {
      act.create_new = true;
      return act;
    }
    int idx = 0;
    uint64_t min_size = files[idx].file_size;
    for (size_t i = 1; i < files.size(); i++) {
      if (min_size > files[i].file_size) {
        min_size = files[i].file_size;
        idx = i;
      }
    }
    act.file_id = files[idx].file_id;
    return act;
  }

  static double GetFlushCost(const State* prev, const std::shared_ptr<State> cur) {
    int start_ts = prev->timestamp;
    int end_ts = cur->timestamp;
    double cost = 0;
    for (int i = start_ts; i < end_ts; i++) {
      cost += (double)prev->comp_controller->GetUpdateOp(i) * entry_size / prev->comp_controller->buffer_size / block_size * mu 
      * std::pow(prev->comp_controller->gamma, i);
    }
    return cost;
  }

  static double GetRangeLookupCost(const State* prev, const std::shared_ptr<State> cur) {
    int start_ts = prev->timestamp;
    int end_ts = cur->timestamp;
    double cost = 0;
    // get the number of sorted run
    int num_sorted_run = 0;
    for (size_t i = 0; i < prev->level_files.size(); i++) {
      num_sorted_run += prev->level_files[i].size();
    }
    for (int i = start_ts; i < end_ts; i++) {
      int new_incoming_run = std::ceil(prev->comp_controller->GetUpdateOp(i) * entry_size / prev->comp_controller->buffer_size);
      num_sorted_run += new_incoming_run;
      cost += (double)num_sorted_run * prev->comp_controller->GetRangeLookupOp(i) * std::pow(prev->comp_controller->gamma, i);
    }
    return cost;
  }

  std::shared_ptr<State> GetNextState(const Action& act) {
    std::shared_ptr<State> new_state = std::make_shared<State>();
    int target_level = act.start_level + 1;
    new_state->level_files = this->level_files;
    new_state->comp_controller = this->comp_controller;
    new_state->timestamp = this->timestamp;
    new_state->max_path_id = this->max_path_id;
    uint64_t total_read_bytes = 0;
    for (size_t i = 0; (int)act.start_level >= 0 && i < new_state->level_files[act.start_level].size(); i++) {
      total_read_bytes += new_state->level_files[act.start_level][i].file_size;
    }
    auto erase_it = new_state->level_files[target_level].end();
    for (size_t i = 0; !act.create_new && i < new_state->level_files[target_level].size(); i++) {
      if (new_state->level_files[target_level][i].file_id == act.file_id) {
        total_read_bytes += new_state->level_files[target_level][i].file_size;
        erase_it = new_state->level_files[target_level].begin() + i;
      }
    }
    new_state->transit_cost = (double)2 * total_read_bytes / block_size * mu;
    if (act.start_level >= 0) {
      new_state->level_files[act.start_level].clear();
      if (erase_it != new_state->level_files[target_level].end()) {
        new_state->level_files[target_level].erase(erase_it);
      }
      new_state->level_files[target_level].push_back(FileMeta{.file_id=++new_state->max_path_id, .file_size=total_read_bytes});
    }
    int estimate_compaction_time = 0;
    if (total_read_bytes != 0) {
      estimate_compaction_time = slope * total_read_bytes * 2 + intercept;
    }
    int window_duration = comp_controller->GetWindowDuration();
    // 1. if estimate_compaction_time < window_duration, only forward one window
    // 2. if estimate_compaction_time > window_duration, forward ceil(estimate_compaction_time / window_duration) windows
    // 3. if advanced_window == 0, forward one window
    int advanced_window = std::ceil((double)estimate_compaction_time / window_duration);
    advanced_window = std::max(1, advanced_window);
    new_state->timestamp = this->timestamp + advanced_window;

    new_state->InitEncoding();
    return new_state;
  }

  std::shared_ptr<State> GetLastLevelState(const Action& act) {
    // only merge the last level
    if (act.start_level != level_files.size() - 1) {
      return nullptr;
    }
    auto new_state = std::make_shared<State>();
    new_state->level_files = this->level_files;
    new_state->comp_controller = this->comp_controller;
    new_state->timestamp = this->timestamp;
    new_state->max_path_id = this->max_path_id;
    uint64_t total_read_bytes = 0;
    for (size_t i = 0; i < new_state->level_files[act.start_level].size(); i++) {
      total_read_bytes += new_state->level_files[act.start_level][i].file_size;
    }
    new_state->transit_cost = (double)2 * total_read_bytes / block_size * mu;
    new_state->level_files[act.start_level].clear();
    // create a new file at the last level
    new_state->level_files[act.start_level].push_back(FileMeta{.file_id=++new_state->max_path_id, .file_size=total_read_bytes});
    int estimate_compaction_time = 0;
    if (total_read_bytes != 0) {
      estimate_compaction_time = slope * total_read_bytes * 2 + intercept;
    }
    int window_duration = comp_controller->GetWindowDuration();
    int advanced_window = std::ceil((double)estimate_compaction_time / window_duration);
    advanced_window = std::max(1, advanced_window);
    new_state->timestamp = this->timestamp + advanced_window;
    new_state->InitEncoding();
    return new_state;
  }

  void InitNextStates() {
    for (size_t i = 0; i < level_files.size() - 1; i++) {
      if (level_files[i].size() == 0) {
        continue;
      }
      Action act = GetMergeAction(i, true);
      auto next_state = GetNextState(act);
      nxts[next_state->encoding] = next_state;
      acts[next_state->encoding] = act;

      act = GetMergeAction(i, false);
      next_state = GetNextState(act);
      nxts[next_state->encoding] = next_state;
      acts[next_state->encoding] = act;
    }
    // no compaction at this state
    Action act = Action{.start_level=-1, .create_new=true};
    auto next_state = GetNextState(act);
    nxts[next_state->encoding] = next_state;
    acts[next_state->encoding] = act;
    // compact the whole last level
    if (level_files.back().size() > 1) {
      act = Action{.start_level=(int)level_files.size() - 1, .create_new=true};
      next_state = GetLastLevelState(act);
      nxts[next_state->encoding] = next_state;
      acts[next_state->encoding] = act;
    }
    // init policy
    // total_cost = std::numeric_limits<double>::max();
    // for (auto& it : nxts) {
    //   double cur_cost = it.second->total_cost * std::pow(gamma, it.second->timestamp - timestamp) + 
    //     GetFlushCost(this, it.second) +
    //     GetRangeLookupCost(this, it.second);
    //   if (total_cost > cur_cost) {
    //     total_cost = cur_cost;
    //     best_nxt_state = it.first;
    //   }
    // }
  }

  void UpdatePolicy() {
    if (nxts.size() == 0) {
      InitNextStates();
    }
    if (timestamp >= comp_controller->searchDepth) {
      RandomWalk(this);
      total_cost = std::numeric_limits<double>::max();
      for (auto& it : nxts) {
        double cur_cost = it.second->total_cost * std::pow(comp_controller->gamma, it.second->timestamp - timestamp) + 
          GetFlushCost(this, it.second) +
          GetRangeLookupCost(this, it.second);
        if (total_cost > cur_cost) {
          total_cost = cur_cost;
          best_nxt_state = it.first;
        }
      }
      return;
    }
    for (auto& it : nxts) {
      it.second->UpdatePolicy();
    }
    std::unordered_map<std::string, double> cost_map;
    for (auto& it : nxts) {
      cost_map[it.first] = it.second->total_cost * std::pow(comp_controller->gamma, it.second->timestamp - timestamp) +
        GetFlushCost(this, it.second) +
        GetRangeLookupCost(this, it.second);
    }
    // recompute the total cost
    total_cost = 0;
    double min_cost = std::numeric_limits<double>::max();
    for (auto& it : nxts) {
      if (min_cost > cost_map[it.first]) {
        min_cost = cost_map[it.first];
        best_nxt_state = it.first;
      }
    }
    total_cost = min_cost;
  }

  Action GetBestAction() {
    return acts[best_nxt_state];
  }

  static void RandomWalk(State* cur) {
    if (cur->timestamp >= cur->comp_controller->GetWorkloadSize() || cur->timestamp >= cur->comp_controller->walkDepth) {
      return;
    }
    // from -1 to level_files.size() - 1
    int start_level = rand() % (cur->level_files.size() + 1) - 1;
    bool create_new = rand() % 2;
    Action act = cur->GetMergeAction(start_level, create_new);
    if (start_level == cur->level_files.size() - 1 || start_level == -1) {
      act.create_new = true;
    }
    std::shared_ptr<State> next_state;
    if (act.start_level == cur->level_files.size() - 1) {
      next_state = cur->GetLastLevelState(act);
    } else {
      next_state = cur->GetNextState(act);
    }
    cur->nxts[next_state->encoding] = next_state;
    cur->acts[next_state->encoding] = act;
    cur->best_nxt_state = next_state->encoding;
    RandomWalk(next_state.get());
    cur->total_cost = next_state->total_cost * std::pow(cur->comp_controller->gamma, next_state->timestamp - cur->timestamp) +
      GetFlushCost(cur, next_state) +
      GetRangeLookupCost(cur, next_state);
  }

  std::string ToString(int depth=0) {
    std::string ret;
    std::string prefix(4, ' ');
    ret += "State: " + encoding + 
      ", nxts size: " + std::to_string(nxts.size()) + ", timestamp: " + std::to_string(timestamp);
    for (auto& it : nxts) {
      ret += ". " + it.first + " => [" + std::to_string(it.second->total_cost * std::pow(comp_controller->gamma, it.second->timestamp - timestamp) +
        GetFlushCost(this, it.second) +
        GetRangeLookupCost(this, it.second)) + ", timestamp diff: " + std::to_string(it.second->timestamp - timestamp) + "]";
    }
    return ret;
  }
};
}