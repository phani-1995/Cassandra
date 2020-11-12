import os
from pyspark.sql.types import StructType, StringType,StructField
from pyspark.sql import SparkSession


os.environ["PYSPARK_PYTHON"] = '/usr/bin/python3'

spark = SparkSession \
    .builder \
    .appName("SSKafka") \
    .getOrCreate()

inputPath = "/home/phani/PycharmProjects/Cassandra/project/jsondata/"
# bootstrap_servers = "localhost:9092"
# topic = "transactions"

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
# Construct a streaming DataFrame that reads from testtopic
inputDF = (
  spark
    .read
    .schema(schema)
    .option("maxFilesPerTrigger", 1)
    .csv(inputPath)
)
print(inputDF.show())


# query = (
#   df3
#     .writeStream
#     .format("console")
#     .outputMode("complete")
#     .start()
# )


# query = (
#   df3
#     .writeStream
#     .format("console")
#     .queryName("counts")
#     .outputMode("complete")
#     .start()
# )

