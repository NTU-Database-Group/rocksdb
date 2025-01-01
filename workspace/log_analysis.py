import argparse
import re
import matplotlib.pyplot as plt
import math
import matplotlib as mpl
mpl.rcParams['agg.path.chunksize'] = 10000

num_re = re.compile(r'\d+')

parser = argparse.ArgumentParser(description='Log Analysis')
parser.add_argument('--log', type=str, help='Path to log file')
parser.add_argument('--style', type=str, help='Compaction style')

run_numbers = []
cur_levels_for_leveling = [0]

range_lookup_latencies = []
update_latencies = []

compactions = []
flushes = []

workloads = []

def process_compaction(comp_line, args):
  global cur_levels_for_leveling
  global run_numbers
 
  log_infos = comp_line.split(']: ')
  ts = int(num_re.findall(log_infos[0])[0])
  file_infos = log_infos[1].split(', ')
  reduce_files = 0
  new_files = 0
  has_l0_input = False
  for file_info in file_infos:
    nums = num_re.findall(file_info)
    nums = [int(i) for i in nums]
    if 'input level' in file_info:
      if args.style == 'dynamic':
        reduce_files += nums[1]
      elif args.style == 'leveling':
        if nums[0] == 0:
          has_l0_input = True
          reduce_files += nums[1]
    elif 'output level' in file_info:
      if args.style == 'dynamic':
        new_files += nums[1]
      elif args.style == 'leveling':
        if nums[0] not in cur_levels_for_leveling:
          cur_levels_for_leveling.append(nums[0])
          new_files += 1
        if nums[0] > 1 and has_l0_input:
          # skip multi-level compaction
          return
        if nums[0] == 0:
          new_files += nums[1]

  last_run = run_numbers[-1][1]
  run_numbers.append((ts, last_run - reduce_files + new_files))

def analyse_log(args):
  with open(args.log, 'r') as f:
    for line in f:
      if '[' not in line:
        continue
      idx1 = line.index('[')
      idx2 = line.index(']')
      op_type  = line[idx1+1:idx2]
      ts = int(line[:idx1])
      if op_type == 'FLUSH':
        flushes.append(ts)
        if len(run_numbers) > 0:
          last = run_numbers[-1]
          run_numbers.append((ts, last[1] + 1))
        else:
          run_numbers.append((ts, 1))
      elif op_type == 'COMPACTION_END':
        process_compaction(line, args)
        compactions.append(ts)
      
      elif op_type == 'COMPACTION_START':
        compactions.append(ts)

      elif op_type == 'RANGE_LOOKUP':
        content = line[idx2:]
        nums = num_re.findall(content)
        range_lookup_latencies.append((ts, int(nums[0])))
      elif op_type == 'UPDATE':
        content = line[idx2:]
        nums = num_re.findall(content)
        update_latencies.append((ts, int(nums[0])))
      elif op_type == 'WINDOW_START':
        content = line[idx2:]
        nums = num_re.findall(content)
        update_cnt = int(nums[-1])
        range_cnt = int(nums[-2])
        workloads.append((ts, range_cnt / (range_cnt + update_cnt)))

def plot_workloads(args):
  x = [i[0] for i in workloads]
  y = [i[1] for i in workloads]
  plt.figure(figsize=(20, 5))
  plt.plot(x, y)
  plt.xlabel('Time')
  plt.ylabel('Range Lookup Ratio')
  plt.savefig(f'result/{args.style}_workload.png')
  plt.close()

def plot_run_number_changes(args):
  x = [i[0] for i in run_numbers]
  y = [i[1] for i in run_numbers]
  total_cnt = len(y)
  plt.figure(figsize=(20, 5))
  plt.plot(x, y)
  plt.text(x[-1] + 100000, y[-1], f'Total: {total_cnt}')
  plt.xlabel('Time')
  plt.ylabel('Number of runs')
  plt.savefig(f'result/{args.style}_run_number.png')
  plt.close()


def plot_range_lookup_latencies(args):
  x = [i[0] for i in range_lookup_latencies]
  y = [i[1] for i in range_lookup_latencies]
  avg_latency = sum(y) / len(y)
  total_cnt = len(y)
  print(f'avg range lookup: {avg_latency}')
  plt.figure(figsize=(20, 5))
  plt.plot(x, y)
  plt.text(x[-1] + 100000, avg_latency, f'Avg: {avg_latency:.2f}\nTotal: {total_cnt}')
  plt.ylim((0, 1000))
  plt.xlabel('Time')
  plt.ylabel('Range lookup latency')
  plt.savefig(f'result/{args.style}_range_lookup_latency.png')
  plt.close()


def plot_update_latencies(args):
  x = [i[0] for i in update_latencies]
  y = [i[1] for i in update_latencies]
  avg_latency = sum(y) / len(y)
  total_cnt = len(y)
  print(f'avg update latency: {avg_latency}')
  plt.figure(figsize=(20, 5))
  plt.plot(x, y)
  plt.text(x[-1] + 100000, avg_latency, f'Avg: {avg_latency:.2f}\nTotal: {total_cnt}')
  plt.ylim((0, 1000))
  plt.xlabel('Time')
  plt.ylabel('Update latency')
  plt.savefig(f'result/{args.style}_update_latency.png')
  plt.close()

def plot_run_number_compaction(args):
  x = [i[0] for i in run_numbers]
  y = [i[1] for i in run_numbers]
  
  avg_y = sum(y) / len(y)
  comp_x = compactions
  comp_y = [avg_y if i % 2 == 0 else 0 for i in range(len(compactions))]
  plt.figure(figsize=(20, 5))
  plt.plot(x, y)
  plt.plot(comp_x, comp_y)
  plt.text(x[-1] + 100000, avg_y, f'total compaction: {len(compactions)}')
  plt.xlabel('Time')
  plt.ylabel('Number of runs')
  plt.savefig(f'result/{args.style}_run_number_compaction.png')
  plt.close()

def plot_range_lookup_latencies_per_window(args):
  win_size = 5000
  x = [i[0] for i in range_lookup_latencies]
  y = [i[1] for i in range_lookup_latencies]

  avg_x = []
  for i in range(0, len(x), win_size):
    avg_x.append(sum(y[i:i+win_size]) / win_size)
  
  avg_y = [i for i in range(len(avg_x))]
  avg_y_y = sum(y) / len(y)
  plt.figure(figsize=(20, 5))
  plt.text(x[-1] + 100000, avg_y_y, f'Avg: {avg_y_y:.2f}\nTotal: {len(avg_y)}')
  plt.plot(avg_y, avg_x)
  plt.xlabel('Time')
  plt.ylabel('Range lookup latency')
  plt.savefig(f'result/{args.style}_range_lookup_latency_per_window.png')

def plot_flush_compactions(args):
  flush_x = []
  flush_y = []
  for ts in flushes:
    flush_x.append(ts-1)
    flush_x.append(ts)
    flush_x.append(ts+1)
    flush_y.append(0)
    flush_y.append(1)
    flush_y.append(0)
  
  comp_x = []
  comp_y = []
  for ts in compactions:
    if len(comp_x) == 0:
      comp_x.append(ts)
      comp_y.append(1)
    elif comp_y[-1] == 1:
      comp_x.append(ts-1)
      comp_y.append(1)
      comp_x.append(ts)
      comp_y.append(0)
    else:
      comp_x.append(ts-1)
      comp_y.append(0)
      comp_x.append(ts)
      comp_y.append(1)
  plt.figure(figsize=(30, 5))
  plt.plot(flush_x, flush_y)
  plt.plot(comp_x, comp_y)
  plt.xlabel('Time')
  plt.ylabel('Flush/Compaction')
  plt.savefig(f'result/{args.style}_flush_compaction.png')

if __name__ == '__main__':
  args = parser.parse_args()
  analyse_log(args)
  plot_run_number_changes(args)
  plot_range_lookup_latencies(args)
  plot_update_latencies(args)
  plot_run_number_compaction(args)
  plot_range_lookup_latencies_per_window(args)
  plot_flush_compactions(args)