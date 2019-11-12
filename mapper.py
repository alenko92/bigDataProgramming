import re
import sys


for line in sys.stdin:
    line = line.strip()
    violations = line.split(',')
    body_type = violations[6]
    make = violations[7]
    year = violations[35]
    if body_type == '':
        body_type = 'Missing'

    if make == '':
        make = 'Missing'

    if year == '':
        year = 'Missing'

    # print('%s\t%s' % (make+'_'+body_type+'_'+year, 1))
    print(make, '/', body_type, '/', year)
