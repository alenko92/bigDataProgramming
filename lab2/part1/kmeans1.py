from __future__ import print_function
import math
from math import sqrt
from pyspark.sql import SparkSession
# from importlib import reload
import sys

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

### Main Program ###
spark = SparkSession.builder.appName("kmeans1").getOrCreate()

dataframe = spark.read.format("csv").load(sys.argv[1], header = "true", inferSchema = "true")
dataPoints = dataframe.filter(dataframe.player_name == 'james harden').select('SHOT_DIST','CLOSE_DEF_DIST', 'SHOT_CLOCK').na.drop()
RDD = dataPoints.rdd.map(lambda r: (r[0], r[1], r[2]))

k = 4
intCentroid = RDD.takeSample(False, k)
iterations = 0 
oldCentroid = intCentroid

for points in range(40):
	map1 = RDD.map(lambda x: closestCenter(x, oldCentroid))
	reduce1 = map1.groupByKey()
	map2 = reduce1.map(lambda x: calcCentroid(x)).collect() # collect a list	
	newCentroid = map2
	convergence = 0 

	for i in range(k):
		if newCentroid[i] == oldCentroid[i]:
			convergence += 1
		else:
			difference = 0.0009 
			closeDifference = [round((a - b)**2, 6) for a, b in zip(newCentroid[i], oldCentroid[i])]
			if all(nxt <= difference for nxt in closeDifference):
				convergence += 1

	if convergence >= 4:
		print("Convergence in iteration # %s \n" %(iterations))
		print("\nFinal Centroids are: %s" %(newCentroid))
		break

	else:
		iterations += 1
		print("--Iteration Number %s --" %(iterations))
		oldCentroid = newCentroid
		print('-Update-', oldCentroid, '\n')

spark.stop()
