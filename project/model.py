import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'
os.environ["PYSPARK_PYTHON"]='/usr/bin/python3'

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark import SparkContext
sc = SparkContext("local", "students app")


spark = SparkSession.builder.appName("Classification with Spark").getOrCreate()


from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

def load_and_get_table_df(keys_space_name, table_name):
    try:
        table_df = sqlContext.read\
            .format("org.apache.spark.sql.cassandra")\
            .options(table=table_name, keyspace=keys_space_name)\
            .load()
        return table_df
    except:
        print("Data doesnt read sucessfully")

trans = load_and_get_table_df("transactions_db", "transactions1")
print(trans.show())

#checking the countof is_fraud variable labels
try:
    count_lab = trans.groupBy('is_fraud').count().show()
    print(count_lab)
except:
    print("Data doesnt found")

def change_type(trans):
    try:
        from pyspark.sql.types import IntegerType, TimestampType, DoubleType

        trans = trans.select('cc_num', 'trans_num', 'trans_date', 'category', 'merchant', 'amt', 'is_fraud')
        trans.na.fill(0)
        trans = trans.withColumn('is_fraud', trans['is_fraud'].cast(DoubleType()))
        trans = trans.withColumn('amt', trans['amt'].cast(IntegerType()))
        trans = trans.withColumn('trans_date', trans['trans_date'].cast(TimestampType()))
        return trans
    except:
        print("syntax error")
data1 =change_type(trans)
print(data1.printSchema())

# Using stringIndex we are indexing the string columns
def strng_indx(data1):
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import StringIndexer
    #create a list of the columns that are string typed
    categoricalColumns = [item[0] for item in data1.dtypes if item[1].startswith('string') ]

    #define a list of stages in your pipeline. The string indexer will be one stage
    stages = []

    #iterate through all categorical values
    for categoricalCol in categoricalColumns:
        #create a string indexer for those categorical values and assign a new name including the word 'Index'
        stringIndexer = StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + 'Index')

        #append the string Indexer to our list of stages
        stages += [stringIndexer]

    #Create the pipeline. Assign the satges list to the pipeline key word stages
    pipeline = Pipeline(stages = stages)
    #fit the pipeline to our dataframe
    pipelineModel = pipeline.fit(data1)
    #transform the dataframe
    data1= pipelineModel.transform(data1)
    return data1
indexed_data = strng_indx(data1)
print(indexed_data.show())


df1 = indexed_data.select('cc_numIndex', 'trans_numIndex',
                'categoryIndex', 'merchantIndex', 'amt', 'is_fraud')
print(df1.show())

#dataset contains value 1
is_fraud = df1.filter("is_fraud=1")
print(is_fraud.show())
#dataset contains value 0
nonfraud = df1.filter("is_fraud=0")
print(nonfraud.show())

def under_sample(df1):
    # Random sample non fraud records
    sampleRatio = is_fraud.count() / df1.count()
    nonFraudSampleDf = nonfraud.sample(False, sampleRatio)
    sampled_data = is_fraud.union(nonFraudSampleDf)
    return sampled_data
df2 = under_sample(df1)
print(df2.show())

print(df2.groupBy('is_fraud').count().show())

try:
    from pyspark.sql.functions import col, count, isnan, when
    print(df2.select([count(when(col(c).isNull(), c)).alias(c) for c in df2.columns]).show())
except:
    print("syntax error")

def vctr_assmblr(df2):
    try:
        cols = df2.columns
        cols.remove("is_fraud")
        assembler = VectorAssembler(inputCols=cols, outputCol="features")

        # Now let us use the transform method to transform our dataset
        data = assembler.transform(df2)

        data.select("features", 'is_fraud').show(truncate=False)

        standardscaler = StandardScaler().setInputCol("features").setOutputCol("Scaled_features")
        data = standardscaler.fit(data).transform(data)
        return data.select("features", 'is_fraud', 'Scaled_features')
    except:
        print("Data not found")

df3 = vctr_assmblr(df2)
print(df3.show())


assembled_data = df3.select("Scaled_features","is_fraud")
print(assembled_data.show())


##Training and testing the dataset
train, test = assembled_data.randomSplit([0.7, 0.3])
#test data
print(train.show())
#test data
print(test.show())

#applying algorithm
log_reg = LogisticRegression(labelCol="is_fraud", featuresCol="Scaled_features",maxIter=40)
model=log_reg.fit(train)
prediction_test=model.transform(test)
print(prediction_test.show())

print(prediction_test.select("is_fraud","prediction").show(1000))

# Compute raw scores on the test set
predictionAndLabels = prediction_test.select("is_fraud","prediction").rdd

print(predictionAndLabels.collect())


#Area under curve ROC
metrics = BinaryClassificationMetrics(predictionAndLabels)

# Area under ROC curve
print("Area under ROC = %s" % metrics.areaUnderROC)

#finding the accuracy
evaluator = MulticlassClassificationEvaluator(labelCol="is_fraud", predictionCol="prediction", metricName="accuracy")
accuracy_LR = evaluator.evaluate(prediction_test)
print("Accuracy = " ,accuracy_LR)

# saving the model
model.save("Fraud_detection_model")
