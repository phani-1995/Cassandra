import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField

spark = SparkSession \
    .builder \
    .appName("myApp") \
    .getOrCreate()
sc = spark.sparkContext

schema = StructType([StructField("cc_num", StringType(), True),
                     StructField("first", StringType(), True),
                     StructField("last", StringType(), True),
                     StructField("trans_num", StringType(), True),
                     StructField("trans_date", StringType(), True),
                     StructField("trans_time", StringType(), True),
                     StructField("unix_time", StringType(), True),
                     StructField("category", StringType(), True),
                     StructField("merchant", StringType(), True),
                     StructField("amt", StringType(), True),
                     StructField("merch_lat", StringType(), True),
                     StructField("merch_long", StringType(), True),
                     StructField("is_fraud", StringType(), True)])
def read_data():
    #Read data from HDFS
    input_path = "hdfs://localhost:54310/user/data/transcations/*.json"
    trans = spark\
        .read\
        .schema(schema)\
        .json(input_path)
    return trans
df = read_data()
print(df.show())

def write_data():
    #Writing data to cassandra
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="transactions", keyspace="transactions_db") \
        .save()


if __name__=="__main__":
    read_data()
    write_data()