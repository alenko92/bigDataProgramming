# Proj2
# Part1: Toxic Comment Classification

from __future__ import print_function
import sys
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.ml.classification import LogisticRegression

### Main Program ####
spark = (SparkSession.builder.appName('Toxic Comment Classification').getOrCreate())
directory_path = sys.argv[1]
train_file_path = directory_path + '/train.csv'
test_file_path = directory_path + '/test.csv'

# Training
df = spark.read.format("csv").load(train_file_path, format = 'com.databricks.spark.csv', header = 'true', 
    inferSchema = 'true', ignoreLeadingWhiteSpace='true', ignoreTrailingWhiteSpace='true')

# change class values to int
df1 = df.withColumn('toxic', col('toxic').cast(IntegerType()))
df2 = df1.withColumn('severe_toxic', col('severe_toxic').cast(IntegerType()))
df3 = df2.withColumn('obscene', col('obscene').cast(IntegerType()))
df4 = df3.withColumn('threat', col('threat').cast(IntegerType()))
df5 = df4.withColumn('insult', col('insult').cast(IntegerType()))
df6 = df5.withColumn('identity_hate', col('identity_hate').cast(IntegerType()))

# DataFrame cleanup
data = df6.select('id', (lower(regexp_replace('comment_text', "[^a-zA-Z\\s]", "")).alias('text')), 'toxic', 'severe_toxic', 'obscene', 'threat', 'insult', 'identity_hate')
data = data.select('id', (regexp_replace('text', "[\r\n]+", "").alias('text')), 'toxic', 'severe_toxic', 'obscene', 'threat', 'insult', 'identity_hate')
data.na.drop()
data.na.fill(0)
clean = data.where(col('toxic').isNotNull()).where(col('severe_toxic').isNotNull()).where(col('obscene').isNotNull()).where(col('threat').isNotNull()).where(col('insult').isNotNull()).where(col('identity_hate').isNotNull())

# Token Parser
tokenizer = Tokenizer(inputCol="text", outputCol="words")
wordToken = tokenizer.transform(clean)

# Delete Stop Words
remover = StopWordsRemover(inputCol='words', outputCol='words_clean')
dataFrameNoStop = remover.transform(wordToken)

# Term Frequency 
hashTermFreq = HashingTF(inputCol="words_clean", outputCol="rawFeatures")
termFreq = hashTermFreq.transform(dataFrameNoStop)

# Term Frequency ID Frequency
idf = IDF(inputCol = "rawFeatures", outputCol = "features")
idfModel = idf.fit(termFreq) 
tfidf = idfModel.transform(termFreq).select('features', 'toxic', 'severe_toxic', 'obscene', 'threat', 'insult', 'identity_hate')

# Logistic Regression Setting
REG = 0.1
logistRegr = LogisticRegression(featuresCol="features", labelCol='toxic', regParam=REG)
logistRegrModel = logistRegr.fit(tfidf.limit(5000))
logistRegrTrain = logistRegrModel.transform(tfidf)
logistRegrTrain.select("toxic", "probability", "prediction").show(20)

# Testing
test = spark.read.format("csv").load(test_file_path, format = 'com.databricks.spark.csv', header = 'true', inferSchema = 'true', ignoreLeadingWhiteSpace='true', ignoreTrailingWhiteSpace='true')
test1 = test.select('id', (lower(regexp_replace('comment_text', "[^a-zA-Z\\s]", "")).alias('text')))
test2 = test1.select('id', (regexp_replace('text', "[\r\n]+", "").alias('text')))
test2.na.drop()
test2.na.fill(0)
test_tokens = tokenizer.transform(test2)
test_words_no_stopw = remover.transform(test_tokens)
test_tf = hashTermFreq.transform(test_words_no_stopw)
test_tfidf = idfModel.transform(test_tf)
extract_prob = F.udf(lambda x: float(x[1]), T.FloatType())
out_cols = [i for i in df.columns if i not in ["id", "comment_text"]]
test_res = test.select('id')

# Model fiting to each leabel of the test DataFrame
for col in out_cols:
    print(col)
    lr_test = LogisticRegression(featuresCol="features", labelCol=col, regParam=REG)
    print("---- Performing Data Fitting -----")
    model = lr_test.fit(tfidf)
    print("---- Predicting ----")
    res = model.transform(test_tfidf)
    print("---- Appending Results -----")
    test_res = test_res.join(res.select('id', 'probability'), on = "id")
    print("---- Calculating Probability ----")
    test_res = test_res.withColumn(col, extract_prob('probability')).drop("probability")
    test_res.show(20)
