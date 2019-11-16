import sys

count = {}

for line in sys.stdin:
    line = line.strip()
    year_type, number = line.split('\t')
    try:
        number = int()
        count[year_type] = count.get(year_type, 0) + number

    except ValueError:
        pass

sorted_count = sorted(count.items(), key = lambda x:-x[1])
for car, count in sorted_count[0:10]:
    print(car, '-', count)
