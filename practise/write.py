import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'

from pyspark import SparkContext
sc = SparkContext("local", "write app")

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


df = sqlContext.read.csv("/home/phani/PycharmProjects/pythonProject/es_practise/file1.csv", header=True, inferSchema=True)
print(df.show())

def create_table():
    from cassandra.cluster import Cluster
    clstr = Cluster()
    session = clstr.connect('students')
    try:
        qry = 'create table emp (Id int, County text, Salary int, primary key(Id));'
        session.execute(qry)
    except:
        print("Table not created sucessfully")

def write_data():
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="emp", keyspace="students") \
        .save()

if __name__ == "__main__":
    # create_table()
    write_data()