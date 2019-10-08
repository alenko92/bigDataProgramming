#!/usr/bin/python
from operator import itemgetter
import sys
from collections import Counter

dict_ip_count = {}

for line in sys.stdin:
    line = line.strip()
    ip, num = line.split('\t')
    try:
        num = int(num)
        dict_ip_count[ip] = dict_ip_count.get(ip, 0) + num

    except ValueError:
        pass


#sorted_dict_ip_count = sorted(dict_ip_count.items(), key=itemgetter(1))
#for ip, count in sorted_dict_ip_count:
#    print('%s\t%s' % (ip, count))
count_dict = Counter(dict_ip_count)
top_dict = count_dict.most_common(3)
for i in top_dict:
    print(i[0] , ':', i[1])
