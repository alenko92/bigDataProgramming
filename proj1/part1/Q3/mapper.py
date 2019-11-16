import re
import sys

for line in sys.stdin:
    line = line.strip()
    violations = line.split(',')
    location = violations[24]
    
    if location == '':
        location = 'Location is Missing'
    print(location, '--')
