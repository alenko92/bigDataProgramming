import re
import sys


for line in sys.stdin:
    line = line.strip()
    violation = line.split(',')
    t = violation[19]
    if t == 'Violation Time':
        continue
    if t == '':
        t = 'Violation Not found'

    # print('%s\t%s' % (t, 1))
    print(t, '--')
