import re
import sys

for line in sys.stdin:
    line = line.strip()
    violations = line.split(',')
    car_color = violations[33]
    
    if car_color == '':
        car_color = 'Record for car color Missing'
    print(car_color, '-')
