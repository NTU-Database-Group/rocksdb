levels = []

with open('log/dynamic_LOG', 'r') as f:
  for line in f:
    if 'The current state: ' not in line:
      continue
    content = line.split('The current state: ')[1]
    levels_content = content.split(';')
    cur_levels = []
    for l in levels_content:
      arr = l.split(':')
      # print(arr)
      if len(arr) <= 1 or len(arr[1]) <= 1:
        cur_levels.append(0)
      else:
        cur_levels.append(len(l.split(',')))
    
    levels.append(cur_levels)

import matplotlib.pyplot as plt

x = [i for i in range(len(levels))]
l0 = [l[0] for l in levels]
l1 = [l[1] for l in levels]
l2 = [l[2] for l in levels]
l3 = [l[3] for l in levels]

plt.plot(x, l0, label='Level 0')
plt.plot(x, l1, label='Level 1')
plt.plot(x, l2, label='Level 2')
plt.plot(x, l3, label='Level 3')
plt.legend()
plt.savefig('result/dynamic_levels.png')