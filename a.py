# import re
# with open('/tmp/db/LOG', 'r') as f:
#   line = f.readline()
#   pattern = re.compile(r' write-amplify\(.*\)')
#   num_pattern = re.compile(r'[0-9]+\.[0-9]*')
#   while line:
#     if ' compacted to: ' in line:
#       num = num_pattern.findall(pattern.findall(line)[0])[0]
#       print(num)
#     line = f.readline()

import re

with open('a.log', 'r') as f:
  line = f.readline()
  pattern = re.compile(r'\d+')
  while line:
    print(pattern.findall(line)[0])
    line = f.readline()
