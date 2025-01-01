#!/bin/bash
export LD_LIBRARY_PATH=../build:/usr/lib/x86_64-linux-gnu:/usr/local/lib:$LD_LIBRARY_PATH

# window_num=100

# winds=(600 800 1000)
winds=(800)

for window_num in ${winds[@]}
do
  mkdir -p result/win_num_$window_num

  rm -rf /tmp/db
  ../build/tools/dynamic_test --compaction_style=dynamic --window_num=$window_num > log/dynamic_${window_num}.log 2>&1
  cp /tmp/db/LOG log/dynamic_LOG
  python3 log_analysis.py --log=log/dynamic_${window_num}.log --style=dynamic
  mv result/dynamic* result/win_num_$window_num/

  # rm -rf /tmp/db
  # ../build/tools/dynamic_test --compaction_style=leveling  --window_num=$window_num > log/leveling.log 2>&1
  # python3 log_analysis.py --log=log/leveling.log --style=leveling
  # mv result/leveling* result/win_num_$window_num/

  # # tiering
  # rm -rf /tmp/db
  # ../build/tools/dynamic_test --compaction_style=moose --window_num=$window_num \
  #   --run_numbers="1,10,10,10" --size_ratios="10,10,10,10" > log/tiering.log 2>&1
  # python3 log_analysis.py --log=log/tiering.log --style=tiering
  # mv result/tiering* result/win_num_$window_num/

  # # lazyleveling
  # rm -rf /tmp/db
  # ../build/tools/dynamic_test --compaction_style=moose --window_num=$window_num \
  #   --run_numbers="1,10,10,1" --size_ratios="10,10,10,10" > log/lazyleveling.log 2>&1
  # python3 log_analysis.py --log=log/lazyleveling.log --style=lazyleveling
  # mv result/lazyleveling* result/win_num_$window_num/
done