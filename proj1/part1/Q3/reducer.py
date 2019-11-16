import sys

count = {}

for line in sys.stdin:
    line = line.strip()
    location, number = line.split('\t')
    try:
        number = int()
        count[location] = count.get(location, 0) + number

    except ValueError:
        pass

sorted_count = sorted(count.items(), key=lambda x:-x[1])
for location, count in sorted_count[0:10]:
    print(location, '->', count)
