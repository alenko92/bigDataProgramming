from __future__ import print_function
import sys
from pyspark.sql import SparkSession
import math
from math import sqrt
from collections import Counter
from collections import defaultdict
from operator import itemgetter

# Function Definitions
# calcCentroid() Definition - calculates new centroid
def calcCentroid(data):
	key, val = data[0], data[1]
	n = len(val)
	updatedList = [0.] * 3

	for i in val:
		updatedList[0] += float(i[0])
		updatedList[1] += float(i[1])
		updatedList[2] += float(i[2])
	newCenter = [round(x / n, 4) for x in updatedList]

	return newCenter

# closestCenter() Definition - finds the closest center of the centroid
def closestCenter(data, center):
	distanceList = [] 
	
	for point in center:
		val = 0.
		for i in range(3):
			val += (data[i] - point[i]) ** 2
		distance = sqrt(val)   
		distanceList.append(distance)   
	closestCenter = float('inf')
	index = -1

	for i, nxt in enumerate(distanceList):
		if nxt < closestCenter:
			closestCenter = nxt
			index = i

	return int(index), data

### Main Program ###
spark = SparkSession.builder.appName("kmeans").getOrCreate()

dataframe = spark.read.format("csv").load(sys.argv[1], header = "true", inferSchema = "true")
blackList = ['BK', 'BLK', 'BK/', 'BK.', 'BLK.', 'BLAC', 'Black', 'BCK', 'BC', 'B LAC']
black = dataframe.filter(dataframe['Vehicle Color'].isin(blackList))

dataPoints = black.select(black['Street Code1'], black['Street Code2'], black['Street Code3']).na.drop()
RDD = dataPoints.rdd.map(lambda r: (r[0], r[1], r[2]))

k = 4
intCentroid = RDD.takeSample(False, k)
iterations = 0 
oldCentroid = intCentroid

for m in range(40):
	map1 = RDD.map(lambda x: closestCenter(x, oldCentroid))
	reduce1 = map1.groupByKey()
	map2 = reduce1.map(lambda x: cal_centroid(x)).collect()
	newCentroid = map2
	convergence = 0 

	for i in range(k):
		if newCentroid[i] == oldCentroid[i]:
			convergence += 1

		else:
			diff = 0.0009 
			closeDiff = [round((a - b)**2, 6) for a, b in zip(newCentroid[i], oldCentroid[i])]
			if all(v <= diff for v in closeDiff):
				convergence += 1

	if convergence >= 4:
		print("Convergence in iteration # %s \n" %(iterations))
		print("\nFinal Centroids are: %s" %(newCentroid))
		break

	else:
		iterations += 1
		print("--Iteration Number %s --" %(iterations))
		oldCentroid = newCentroid
		print('-Update-',oldCentroid,'\n')

streetCodes = [34510, 10030, 34050]
closestStreet = closestCenter(streetCodes, newCentroid)
map3 = RDD.filter(lambda x: closestCenter(x, newCentroid)[0] == closestStreet[0]).collect()
count = len(map3)
pointer = dict(Counter(map3))
counter = len(pointer)
maximum = max(pointer.items(),key = itemgetter(1))[1]
probability = round(count/(maximum * counter), 6)
print ("-- Probability of getting a ticket --\n")
print (probability)

spark.stop()