from __future__ import print_function
import sys
import math as math
from math import sqrt
from random import uniform as rand
from collections import defaultdict
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql import *
from pyspark.sql.types import *
# from importlib import reload

# Function Definitions    
# calcDistance() Definition - Calculate Euclidean Distance
def calcDistance(point, centroid):
    value = 0.

    for i in range(len(point)):
        value += (point[i] - centroid[i]) ** 2
    dist = math.sqrt(value)

    return dist

# closestCentroid() Definition - returns index of the closet centroid
def closestCentroid(col1, col2, col3):
	points = [col1, col2, col3]
	distanceList = []
	
	for c in newCenter:
		distanceList.append(calcDistance(points, c))
	closest = float('inf')
	index = -1

	for i, v in enumerate(distanceList):
		if v < closest:
			closest = v
			index = i

	return int(index)

# calculateCentroid() Definition - calculates a new centroid
def calculateCentroid(dataframe, col):
	sumOfValues = round(dataframe.select(F.sum(col)).collect()[0][0], 2)
	n = dataframe.count()
	if n > 0: 
		value = round(sumOfValues / n,  2)
		
	return value


### Main Program ###
minCenter = udf(closestCentroid, int())
spark = SparkSession.builder.appName("kmeans2").getOrCreate()

dataframe = spark.read.format("csv").load(sys.argv[1], header = "true", inferSchema = "true")
dataPoints = dataframe.filter(dataframe.player_name == 'james harden').select('SHOT_DIST','CLOSE_DEF_DIST', 'SHOT_CLOCK').na.drop()
RDD = dataPoints.rdd.map(lambda r: (r[0], r[1], r[2]))

k = 4
intCentroid = dataPoints.takeSample(False, k)
newCenter = intCentroid
oldCenter = intCentroid
newCentroid = []
iterations = 0 

while True: 
	RDDCluster = dataPoints.drop('Cluster')
	RDDCluster = dataPoints.withColumn('Center', minCenter(dataPoints.SHOT_DIST, dataPoints.CLOSE_DEF_DIST, dataPoints.SHOT_CLOCK))
	converge = 0 

	for i in range(k):
		kCluster = RDDCluster.filter(RDDCluster.Center == i)
		n = kCluster.count()
		sumOf = [0] * 3
		sumOf[0] = calculateCentroid(kCluster, 'SHOT_DIST')
		sumOf[1] = calculateCentroid(kCluster, 'CLOSE_DEF_DIST')
		sumOf[2] = calculateCentroid(kCluster, 'SHOT_CLOCK')
		sumOfCols = [round(x / n, 4) for x in sumOf]
		newCenter[i] = sumOfCols
		print(newCenter)
        
		if (newCenter[i] == oldCenter[i]):
			converge += 1
		elif (newCenter[i] != oldCenter[i]):
    			diff = 0.0009 
			closeDiff = [round((a - b)**2, 6) for a, b in zip(newCenter[i], oldCenter[i])]
			if all(v <= diff for v in closeDiff):
				converge += 1
	iterations += 1
	print("--Iteration Number %s --" %(iterations))

	if converge >= 4:
		print("Convergence in iteration # %s \n" %(iterations))
		print("\nFinal Centroids are: %s" %(newCentroid))
		break

	else:
		iterations += 1
		print("--Iteration Number %s --" %(iterations))
		old_centroid = newCentroid
		print('-Update-', old_centroid,'\n')

spark.stop()
