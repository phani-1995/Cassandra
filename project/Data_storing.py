from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .appName("myApp") \
    .getOrCreate()
sc = spark.sparkContext

def write_data():
    df = spark.read.option('header', True).csv('transactions.csv')
    # Dropping null values
    df = df.dropna()
    print(df.printSchema())
    print(df.show())
    #stroing data in hdfs
    df.write.json("hdfs://localhost:54310/user/data/transcations", mode='append')

if __name__=="__main__":
    write_data()