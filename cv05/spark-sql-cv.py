from pyspark.sql import SparkSession

spark = SparkSession.builder.master("spark://spark-master:7077").appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv("/files/fakefriends-header.csv")
    
print("Here is our inferred schema:")
people.printSchema()
people.groupBy("age").avg().select("age","avg(friends)").sort("age", ascending=True).show()

spark.stop()

