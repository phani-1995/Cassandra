from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .appName("myApp") \
    .getOrCreate()
sc = spark.sparkContext

def write_data():
    df = spark.read.option('header', True).csv('transactions.csv')
    # Dropping null values
    df = df.na.fill(0)
    print(df.printSchema())
    print(df.show())
    #stroing data in hdfs
    df.write.json("hdfs://localhost:54310/user/data/transcations1", mode='append')

def write_cust():
    df1 = spark.read.option('header', True).csv('customer.csv')
    # Dropping null values
    df1 = df1.na.fill(0)
    print(df1.printSchema())
    print(df1.show())
    # stroing data in hdfs
    df1.write.json("hdfs://localhost:54310/user/data/customers1", mode='append')


if __name__=="__main__":
    write_data()
    write_cust()