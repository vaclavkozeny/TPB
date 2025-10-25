from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from pyspark.sql.functions import sum, round
spark = SparkSession.builder.master("spark://spark-master:7077").appName("SparkSQL").getOrCreate()
scheme = StructType([\
        StructField("customerId", IntegerType(), True), \
        StructField("itemId", IntegerType(), True), \
        StructField("ammount", FloatType(), True)])

orders = spark.read.option("header", "false").schema(scheme).csv("/files/customer-orders.csv")

print("Here is our inferred schema:")
orders.printSchema()
orders.groupBy("customerId").agg(round(sum("ammount"), 2).alias("total")).sort("total", ascending=False).show()

spark.stop()

