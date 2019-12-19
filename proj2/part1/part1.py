# Proj2
# Part1: Toxic Comment Classification
# Alexey Sanko
#
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import print_function
import os
import sys
import pandas as panda
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.types import StructType

### Main Program ####
spark = (SparkSession.builder.appName('Toxic Comment Classification').getOrCreate())
dirPath = sys.argv[1]
trainingFPath = dirPath + 'train.csv'
testFPath = dirPath + 'test.csv'

# Load data as pandas and convert to spark dataframe
dataFrame = panda.read_csv(trainingFPath)
dataFrame.fillna("", inplace = True)
dataFrame = spark.createDataFrame(dataFrame)

# DataFrame cleanup
data = dataFrame.select('id', (lower(regexp_replace('comment_text', "[^a-zA-Z\\s]", "")).alias('text')), 'toxic', 'severe_toxic', 
    'obscene', 'threat', 'insult', 'identity_hate')
data = data.select('id', (regexp_replace('text', "[\r\n]+", "").alias('text')), 'toxic', 'severe_toxic', 'obscene', 'threat', 
    'insult', 'identity_hate')
data.na.drop()
data.na.fill(0)
cols = ['toxic', 'severe_toxic', 'obscene', 'threat', 'insult', 'identity_hate']
cleanData = data.where(col('toxic').isNotNull()).where(col('severe_toxic').isNotNull()).where(col(
    'obscene').isNotNull()).where(col('threat').isNotNull()).where(col('insult').isNotNull()).where(col('identity_hate').isNotNull())

# Token Parser
tokenParser = Tokenizer(inputCol = "text", outputCol = "words")
wordToken = tokenParser.transform(cleanData)

# Delete Stop Words
remover = StopWordsRemover(inputCol = 'words', outputCol = 'words_clean')
dataFrameNoStop = remover.transform(wordToken)

# Term Frequency 
hashTermFreq = HashingTF(inputCol = "words_clean", outputCol = "rawFeatures")
termFreq = hashTermFreq.transform(dataFrameNoStop)

# Term Frequency ID Frequency
idf = IDF(inputCol = "rawFeatures", outputCol = "features")
idfModel = idf.fit(termFreq) 
tfidf = idfModel.transform(termFreq).select('features', 'toxic', 'severe_toxic', 'obscene', 'threat', 'insult', 'identity_hate')

# Logistic Regression Setting
REG = 0.1
logistRegr = LogisticRegression(featuresCol = "features", labelCol = 'toxic', regParam = REG)
logistRegrModel = logistRegr.fit(tfidf.limit(5000))
logistRegrTrain = logistRegrModel.transform(tfidf)
logistRegrTrain.select("toxic", "probability", "prediction").show(10)

# Testing
test = panda.read_csv(testFPath)
test.fillna("", inplace = True)
test = spark.createDataFrame(test)
test1 = test.select('id', (lower(regexp_replace('comment_text', "[^a-zA-Z\\s]", "")).alias('text')))
test2 = test1.select('id', (regexp_replace('text', "[\r\n]+", "").alias('text')))
test2.na.drop()
test2.na.fill(0)
test_tokens = tokenParser.transform(test2)
test_words_no_stopw = remover.transform(test_tokens)
test_tf = hashTermFreq.transform(test_words_no_stopw)
test_tfidf = idfModel.transform(test_tf)
extract_prob = F.udf(lambda x: float(x[1]), T.FloatType())
out_cols = [i for i in logistRegrTrain.columns if i not in ["id", "comment_text"]]
test_res = test.select('id')

# Model fiting to each leabel of the test DataFrame
for col in out_cols:
    print(col)
    lr_test = LogisticRegression(featuresCol="features", labelCol = col, regParam = REG)
    print("---- Performing Data Fitting -----")
    model = lr_test.fit(tfidf.limit(1000))
    print("---- Predicting ----")
    res = model.transform(test_tfidf)
    print("---- Appending Results -----")
    test_res = test_res.join(res.select('id', 'probability'), on = "id")
    print("---- Calculating Probability ----")
    test_res = test_res.withColumn(col, extract_prob('probability')).drop("probability")
    test_res.show(10)
