#!/bin/bash
export LD_LIBRARY_PATH=../build:/usr/lib/x86_64-linux-gnu:/usr/local/lib:$LD_LIBRARY_PATH

window_num=500
# ints=(150 100 50 10)
ints=(10)
# ints=(50)
# ints=(500)
# gammas=(0.9 0.8 0.7 0.6 0.5)
gammas=(0.9)

for intensity in ${ints[@]}
do
  result_path=result/intensity/int_$intensity
  mkdir -p $result_path
  for gamma in ${gammas[@]}
  do
    echo "testing gamma: $gamma, intensity: $intensity ..."
    rm -rf /tmp/db
    ../build/tools/dynamic_test --compaction_style=dynamic --search_depth=8 --walk_depth=100 --intensity=$intensity --gamma=0.99 --window_num=$window_num > log/dynamic_${window_num}_${intensity}.log 2>&1
    cp /tmp/db/LOG log/dynamic_LOG
    python3 log_analysis.py --log=log/dynamic_${window_num}_${intensity}.log --style=dynamic
    mv result/dynamic* $result_path
  done
  # rm -rf /tmp/db
  # ../build/tools/dynamic_test --compaction_style=leveling  --intensity=$intensity  --window_num=$window_num > log/leveling.log 2>&1
  # python3 log_analysis.py --log=log/leveling.log --style=leveling
  # mv result/leveling* $result_path

  # # tiering
  # rm -rf /tmp/db
  # ../build/tools/dynamic_test --compaction_style=moose  --intensity=$intensity --window_num=$window_num \
  #   --run_numbers="1,10,10,10" --size_ratios="10,10,10,10" > log/tiering.log 2>&1
  # python3 log_analysis.py --log=log/tiering.log --style=tiering
  # mv result/tiering* $result_path

  # # lazyleveling
  # rm -rf /tmp/db
  # ../build/tools/dynamic_test --compaction_style=moose  --intensity=$intensity --window_num=$window_num \
  #   --run_numbers="1,10,10,1" --size_ratios="10,10,10,10" > log/lazyleveling.log 2>&1
  # python3 log_analysis.py --log=log/lazyleveling.log --style=lazyleveling
  # mv result/lazyleveling* $result_path

  echo "-----------------------"
done