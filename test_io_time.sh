#!/bin/bash

cat <<EOF > tmp.py
f=open('tmp.log')
line=f.readline()
sum=0
a=[]
while line:
  a.append(float(line))
  line = f.readline()
for i in range(1,len(a)):
  sum += a[i]-a[i-1]
print(sum/(len(a)-1))
EOF

for i in $(seq 1 40);
do
  size=$(python3 -c "print($i * 512)")
  sudo rm -f trace.dat
  cp test_reader.cc tmp.cc
  sed -i "s/512/$size/g" tmp.cc
  g++ tmp.cc -o tmp.out -std=c++17
  ./tmp.out
  # sudo trace-cmd record -e block:block_rq_issue -e block:block_rq_complete -e block:block_bio_bounce -e block:block_bio_complete -- ./tmp.out 2>/dev/null
  # sudo trace-cmd report | grep tmp | awk '{print $3}' | sed "s/://g" > tmp.log
  # a=$(sudo python3 tmp.py)
  # lpb=$(python3 -c "print($a / $size)")
  # echo "$size latency: $a, latency per byte: $lpb"
done

rm -f tmp.cc
rm -f tmp.py
rm -f tmp.out