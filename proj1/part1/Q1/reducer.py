#!/usr/bin/python
import sys
from operator import itemgetter

time_count = {}

for line in sys.stdin:
    line = line.strip()
    time, number = line.split('\t')

    try:
        number = int()
        time_count[time] = time_count.get(time, 0) + number

    except ValueError:
        pass

sorted_time_count = sorted(time_count.items(), key = lambda x:-x[1])
for time, count in sorted_time_count[0:10]:
    print(time, '\t', count)
