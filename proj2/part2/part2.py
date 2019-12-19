# Proj2
# Part2: Heart Disease Prediction using Logistic Regression
# Alexey Sanko
#
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when,udf
from pyspark.sql import types as T
from pyspark.ml.linalg import Vectors, VectorUDT, SparseVector, DenseVector
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.feature import ChiSqSelector
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluate import BinaryClassificationEvaluator

# Function Definitions
# sparseToDenseConvert() Method to convert sparse vector into a dense vector
def sparseToDenseConvert(vect):
    vect = DenseVector(vect)
    newVector = list([float(x) for x in vect])
    return newVector

# oneHotEncodeStages() Method returns a dataframe consisting of the categorical Columns selected
def oneHotEncodeStages(dataFrame):
    categoricalCols = ['education','currentSmoker', 'BPMeds', 'prevalentStroke', 'prevalentHyp', 'diabetes']
    stages = []
    for categCol in categoricalCols:
        stringIndx = StringIndexer(inputCol = categCol, outputCol = categCol + 'Index')
        encoder = OneHotEncoderEstimator(inputCols=[stringIndx.getOutputCol()], outputCols=[categCol + "classVec"])
        stages += [stringIndx, encoder]

    numCols = ['male', 'age', 'cigsPerDay', 'totChol', 'sysBP', 'diaBP', 'BMI', 'heartRate', 'glucose']
    assembInput = [c + "classVec" for c in categoricalCols] + numCols
    assembler = VectorAssembler(inputCols=assembInput, outputCol="features")
    stages += [assembler]
    pipeline = Pipeline().setStages(stages)
    pipelineModel = pipeline.fit(dataFrame)
    dataFrame = pipelineModel.transform(dataFrame)
    selectedCols = ['TenYearCHD', 'features'] + cols
    dataFrame = dataFrame.select(selectedCols)
    sparseToDenseUDF = udf(sparseToDenseConvert, T.ArrayType(T.FloatType()))
    dataFrame = dataFrame.withColumn('dense_vector_features', sparseToDenseUDF('features'))
    udF = udf(lambda r : Vectors.dense(r), VectorUDT())
    dataFrame = dataFrame.withColumn('features', udF('dense_vector_features'))
    return dataFrame

# evaluate() Method Evaluates Confusion Matrix for The DataSets
def evaluate(TP, TN, FP, FN):
    print('\n\n', 'Confusion Matrix: \n\n', '[[', TN, FP, ']\n\n', '[', FN, TP, ']]')
    sensitivity = TP/float(TP+FN)
    specificity = TN/float(TN+FP)
    print('\n\nAccuracy of the Model = (TP+TN)/(TP+TN+FP+FN) = ', (TP+TN)/float(TP+TN+FP+FN), '\n\n',
    'Miss-classification = 1-Accuracy = ', 1-((TP+TN)/float(TP+TN+FP+FN)), '\n\n',
    'True Positive Rate = TP/(TP+FN) = ', TP/float(TP+FN),'\n\n',
    'True Negative Rate = TN/(TN+FP) = ', TN/float(TN+FP),'\n\n',
    'Positive Predictive Value = TP/(TP+FP) = ', TP/float(TP+FP),'\n\n',
    'Negative Predictive Value = TN/(TN+FN) = ', TN/float(TN+FN),'\n\n',
    'Positive Likelihood Ratio = Positive Rate/(1-Negative Rate) = ', sensitivity/(1-specificity), '\n\n',
    'Negative likelihood Ratio = (1-Positive Rate)/Negative Rate = ', (1-sensitivity)/specificity, '\n\n')


### Main Program ###
reload(sys)
sys.setdefaultencoding('utf8')
if __name__:
    spark = SparkSession.builder.appName('lr-predic').getOrCreate()
    dataFrame = spark.read.csv(sys.argv[1], header = True, inferSchema = True)
    dataFrame.show()
    cols = dataFrame.columns
    dataFrame.printSchema()

    # Ignore all 'NA' Values
    dataFrame = dataFrame.filter((dataFrame.cigsPerDay != 'NA')& (dataFrame.BPMeds != 'NA') & (dataFrame.totChol != 'NA') & (dataFrame.BMI != 'NA') & (dataFrame.heartRate != 'NA') & (dataFrame.glucose != 'NA'))
    stringColumns=['cigsPerDay', 'BPMeds','totChol','BMI', 'heartRate', 'glucose']
    for col_name in stringColumns:
        dataFrame = dataFrame.withColumn(col_name, col(col_name).cast('float'))
    
    dataFrame.groupby('TenYearCHD').count().show()
    cols.remove('education')
    cols.insert(0, 'Summary')
    dataFrame.describe().select(cols[:len(cols)//2]).show()
    dataFrame.describe().select(cols[len(cols)//2:-1]).show()
    cols.remove('TenYearCHD')
    cols.remove('Summary')
    print(cols)
    dataFrame = oneHotEncodeStages(dataFrame)
    dataFrame.printSchema()
    
    # Standarize DataFrame
    standardscaler = StandardScaler().setInputCol("features").setOutputCol("Scaled_features")
    dataFrame = standardscaler.fit(dataFrame).transform(dataFrame)
    dataFrame.select("features", "Scaled_features").show(5)
    
    train, test = dataFrame.randomSplit([0.8, 0.2], seed=12345)

    total = float(train.select("TenYearCHD").count())
    positiveNum = train.select("TenYearCHD").where('TenYearCHD == 1').count()
    pOnes = (float(positiveNum)/float(total))*100
    negativeNum = float(total-positiveNum)
    print('\n\nThe number of Class 1 are {}'.format(positiveNum))
    print('\n\nPercentage of Class 1 are {}'.format(pOnes))

    BalancingRatio= negativeNum/total
    print('\n\nBalancingRatio = {}'.format(BalancingRatio))
    train = train.withColumn("classWeights", when(train.TenYearCHD==1, BalancingRatio).otherwise(1-BalancingRatio))
    train.select("classWeights", "TenYearCHD").show(5)
    
    # Feature selection
    css = ChiSqSelector(featuresCol = 'features', outputCol='Aspect', labelCol='TenYearCHD', fpr = 0.05)
    train = css.fit(train).transform(train)
    test = css.fit(test).transform(test)
    test.select("Aspect").show(5, truncate = False)

    # Training Model
    lr = LogisticRegression(labelCol="TenYearCHD", featuresCol="Aspect", weightCol="classWeights", maxIter=10)
    model = lr.fit(train)
    predict_train = model.transform(train)
    predict_test = model.transform(test)
    predict_train.select("TenYearCHD","prediction").show(5)
    predict_test.select("TenYearCHD","prediction").show(5) 
    TP_train = predict_train.filter((predict_train.TenYearCHD == 1) & (predict_train.prediction == 1.0)).count()
    TN_train = predict_train.filter((predict_train.TenYearCHD == 0) & (predict_train.prediction == 0.0)).count()
    FP_train = predict_train.filter((predict_train.TenYearCHD == 0) & (predict_train.prediction == 1.0)).count()
    FN_train = predict_train.filter((predict_train.TenYearCHD == 1) & (predict_train.prediction == 0.0)).count()    
    print('\n\nEvaluation Results for Training Dataset')
    evaluate(TP_train, TN_train, FP_train, FN_train)
    
    # Testing Model
    TP_test = predict_test.filter((predict_test.TenYearCHD == 1) & (predict_test.prediction == 1.0)).count()
    TN_test = predict_test.filter((predict_test.TenYearCHD == 0) & (predict_test.prediction == 0.0)).count()
    FP_test = predict_test.filter((predict_test.TenYearCHD == 0) & (predict_test.prediction == 1.0)).count()
    FN_test = predict_test.filter((predict_test.TenYearCHD == 1) & (predict_test.prediction == 0.0)).count()
    print('\n\nEvaluation Results for Test Dataset')
    evaluate(TP_test, TN_test, FP_test, FN_test)

    # ROC for Training and Test DataSets
    evaluator = BinaryClassificationEvaluator(rawPredictionCol = "rawPrediction", labelCol = "TenYearCHD")
    predict_test.select("TenYearCHD", "rawPrediction", "prediction", "probability").show(5, truncate = False)

    print("\n\nThe area under ROC for train set is {}".format(evaluator.evaluate(predict_train)))
    print("\n\nThe area under ROC for test set is {}".format(evaluator.evaluate(predict_test)))
