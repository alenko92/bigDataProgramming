import sys


count = {}

for line in sys.stdin:
    line = line.strip()
    car_color, number = line.split('\t')
    try:
        number = int(number)
        count[car_color] = count.get(car_color, 0) + number

    except ValueError:
        pass

sorted_count = sorted(count.items(), key=lambda x:-x[1])
for car_color, count in sorted_count[0:5]:
    # print('%s\t%s' % (car_color, count))
    print(car_color, count)
