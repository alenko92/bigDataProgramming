import re
import sys

pat = re.compile('(?P<ip>\d+.\d+.\d+.\d+).*?\d{4}:(?P<hour>\d{2}):\d{2}.*? ')
for line in sys.stdin:
    match = pat.search(line)
    if match:
        print('%s\t%s' % ('[' + match.group('hour') + ':00' + ']' + match.group('ip'), 1))

#!/usr/bin/python
# --*-- coding:utf-8 --*--
import re
import sys

pat = re.compile('(?P<ip>\d+.\d+.\d+.\d+).*?\d{4}:(?P<hour>\d{2}):\d{2}.*? ')
a = input("input time range such as 0-1: ")
a_list = a.split("-")
#for i in range(int(a_list[0]), int(a_list[-1])):
#    print(i)

for line in sys.stdin:
    match = pat.search(line)
    if match and match.group('hour') in range[int(a_list[0]), int(a_list[-1])]:
        print('%s\t%s' % ('[' + match.group('hour') + ':00' + ']' + match.group('ip'), 1))
