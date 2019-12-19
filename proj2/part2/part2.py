# Proj2
# Part3: Logistic Regression Classifier on Census Income Data
# Alexey Sanko
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import print_function
import sys
import os
import math
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, udf, sum
from pyspark.sql import types as T
from pyspark.ml.linalg import Vectors, VectorUDT, SparseVector, DenseVector
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.sql import *

# Function Definitions
# sparseToDenseConvert() Method to convert sparse vector into a dense vector
def sparseToDenseConvert(vect):
    vect = DenseVector(vect)
    newVector = list([float(x) for x in vect])
    return newVector

# oneHotEncodeStages() Method returns a dataframe consisting of the categorical Columns selected
def oneHotEncodeStages(dataFrame):
    cols = dataFrame.columns
    caegoricalCols = ['workclass', 'marital-status', 'occupation', 'relationship', 'race', 'sex', 'native-country']
    stages = []
    for categCol in caegoricalCols:
        stringIndx = StringIndexer(inputCol = categCol, outputCol = categCol + 'Index')
        encoder = OneHotEncoderEstimator(inputCols = [stringIndx.getOutputCol()], outputCols = [categCol + "classVec"])
        stages += [stringIndx, encoder]

    label_stringIdx = StringIndexer(inputCol = 'income', outputCol = 'label')
    stages += [label_stringIdx]
    numericCols = ["age", "education-num", "capital-gain", "capital-loss", "hours-per-week"]
    assemblerInputs = [c + "classVec" for c in caegoricalCols] + numericCols
    assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="initial_features")
    stages += [assembler]
    pipeline = Pipeline().setStages(stages)
    pipelineModel = pipeline.fit(dataFrame)
    dataFrame = pipelineModel.transform(dataFrame)
    selectedCols = ['label', 'initial_features'] + cols
    dataFrame = dataFrame.select(selectedCols)
    sparse_to_array_udf = udf(sparseToDenseConvert, T.ArrayType(T.FloatType()))
    dataFrame = dataFrame.withColumn('dense_vector_features', sparse_to_array_udf('initial_features'))
    ud_f = udf(lambda r : Vectors.dense(r), VectorUDT())
    dataFrame = dataFrame.withColumn('features', ud_f('dense_vector_features'))
    return dataFrame

# cleanDataFrame() Method returns a cleaned DataFrame after dropping respective columns and unexpected values
def cleanDataFrame(dataFrame, frequentItems):
    columnsToDrop = ['education', 'fnlwgt']
    dataFrame = dataFrame.drop(*columnsToDrop)
    dataFrame = dataFrame.withColumn('native-country', when(dataFrame['native-country'] == '?', frequentItems[0][0][0]).otherwise(
        dataFrame['native-country']))
    dataFrame = dataFrame.withColumn('workclass', when(dataFrame['workclass'] == '?', frequentItems[0][1][0]).otherwise(
        dataFrame['workclass']))
    dataFrame = dataFrame.withColumn('occupation', when(dataFrame['occupation'] == '?', frequentItems[0][2][0]).otherwise(
        dataFrame['occupation']))
    dataFrame = dataFrame.withColumn('native-country', when(dataFrame['native-country'] != 'United-States', 'Not-US').otherwise(
        dataFrame['native-country']))
    dataFrame = oneHotEncodeStages(dataFrame)
    return dataFrame

# getAccuracyRate() Method that computes the Accuracy Rate of the Prediction DataFrame
def getAccuracyRate(predDataFrame):
    accurRate = 0.0
    numPredictions = predDataFrame.count()
    predDataFrame = predDataFrame.withColumn('isSame', when(predDataFrame['label'] == predDataFrame['prediction'], 1.0).otherwise(0.0))
    correctPredictions = predDataFrame.select(sum('isSame')).collect()[0][0]
    accurRate = (float(correctPredictions) / float(numPredictions)) * 100.0
    return accurRate


### Main Program ###
reload(sys)
sys.setdefaultencoding('utf8')
if __name__:
    conf = SparkConf().setAppName("Project2Part3")
    sparkContxt = SparkContext(conf = conf)
    sqlContext = SQLContext(sparkContxt)
    directPath = sys.argv[1]
    trainFilePath = directPath + 'adult.data.csv'
    testFilePath = directPath + 'adult.test.csv'
    trainingDataFrame = sqlContext.read.load(trainFilePath, format = 'com.databricks.spark.csv', header = 'true', 
        inferSchema = 'true', ignoreLeadingWhiteSpace='true', ignoreTrailingWhiteSpace='true')
    nRows = trainingDataFrame.count()
    nColumns = len(trainingDataFrame.columns)
    trainingDataFrame.show(5, False)
    print('# Initial Training Rows:', nRows, '\t# Initial Training Columns:', nColumns, '\n')
    testDataFrame = sqlContext.read.load(testFilePath, format = 'com.databricks.spark.csv', header = 'true', 
        inferSchema = 'true', ignoreLeadingWhiteSpace='true', ignoreTrailingWhiteSpace='true')
    nRows = testDataFrame.count()
    nColumns = len(testDataFrame.columns)
    testDataFrame.show(5, False)
    print('# Initial Test Rows :', nRows, '\t# Initial Test Columns :', nColumns, '\n')

    # Clean Training and Test DataFrames by finding frequent items and fill them in the test DataFrame
    frequentItems = trainingDataFrame.freqItems(['native-country', 'workclass', 'occupation'], support = 0.6).collect()
    trainingDataFrame = cleanDataFrame(trainingDataFrame, frequentItems)
    trainingDataFrame = trainingDataFrame.withColumn('income', when(trainingDataFrame['income'] == '<=50K', 0).otherwise(1))
    nRows = trainingDataFrame.count()
    nColumns = len(trainingDataFrame.columns)
    trainingDataFrame.show(5, False)
    trainingDataFrame.printSchema()
    print('\n# Final Training Rows:', nRows, '\t# Final Training Columns:', nColumns, '\n')
    testDataFrame = cleanDataFrame(testDataFrame, frequentItems)
    testDataFrame = testDataFrame.withColumn('income', when(testDataFrame['income'] == '<=50K.', 0).otherwise(1))
    nRows = testDataFrame.count()
    nColumns = len(testDataFrame.columns)
    testDataFrame.show(5, False)
    testDataFrame.printSchema()
    print('\n# Final Test Rows:', nRows, '\t# Final Test Columns:', nColumns, '\n')

    # New DataFrame with two columns: features and income(label)
    vTrainDataFrame = trainingDataFrame.select(['label', 'features'])
    vTrainDataFrame.show(5, False)
    vTrainDataFrame.printSchema()
    print('\n\tFinal Training Dataframe for Logistic Regression\n')
    vTestDataFrame = testDataFrame.select(['label', 'features'])
    vTestDataFrame.show(5, False)
    vTestDataFrame.printSchema()
    print('\n\tFinal Test Dataframe for Logistic Regression\n')

    # Start Logistic Regression Model
    logisticReg = LogisticRegression(featuresCol = 'features', labelCol = 'label', maxIter = 10)
    logisticRegModel = logisticReg.fit(vTrainDataFrame)
    logisticRegCoeff = logisticRegModel.coefficients
    print('\nCoefficients: ')
    print([round(i, 3) for i in logisticRegCoeff])
    print('\nIntercept: ', logisticRegModel.intercept, '\n')

    # Calculate Train Predictions, Accuracy Rate, Area under ROC and Area unde PR
    evaluator = BinaryClassificationEvaluator()
    trainPredict = logisticRegModel.transform(vTrainDataFrame)
    trainPredict.show(5, False)
    print('\nTraining Predictions DataFrame\n')
    trainROC = evaluator.setMetricName('areaUnderROC').evaluate(trainPredict)
    trainPR = evaluator.setMetricName('areaUnderPR').evaluate(trainPredict)
    condenseTrainPredict = trainPredict.select(['label', 'prediction'])
    trainACC = round(getAccuracyRate(condenseTrainPredict), 2)
    condenseTrainPredict.show(5, False)
    print('\nTraining Predictions DataFrame\n')
    print('Training Accuracy Rate:', trainACC)
    print('Training Area under ROC:', round(trainROC, 4))
    print('Training Area under PR:', round(trainPR, 4), '\n')

    # Calculate Test Predictions and Accuracy Rate
    testPredict = logisticRegModel.transform(vTestDataFrame)
    testPredict.show(5, False)
    print('\nTest Predictions DataFrame\n')
    testROC = evaluator.setMetricName('areaUnderROC').evaluate(testPredict)
    testPR = evaluator.setMetricName('areaUnderPR').evaluate(testPredict)
    condenseTestPredict = testPredict.select(['label', 'prediction'])
    testACC = round(getAccuracyRate(condenseTestPredict), 2)
    condenseTestPredict.show(5, False)
    print('\nCondensed Test Predictions DataFrame\n')
    print('Test Accuracy Rate:', testACC)
    print('Test Area under ROC:', round(testROC, 4))
    print('Test Area under PR:', round(testPR, 4), '\n')
