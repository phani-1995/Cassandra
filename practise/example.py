from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .appName("myApp") \
    .getOrCreate()
sc = spark.sparkContext

df = spark.read.option('header', True).csv('/home/phani/PycharmProjects/Cassandra/project/transactions.csv')
    # Dropping null values
df = df.na.fill(0)
print(df.printSchema())
print(df.show())

df.write.json("/home/phani/PycharmProjects/Cassandra/project/jsondata", mode='append')
