from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql import functions as func
spark = SparkSession.builder.master("spark://spark-master:7077").appName("SparkSQL").getOrCreate()
schema = StructType([ \
    StructField("id", IntegerType(), True), \
    StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("/files/marvel-names.txt")
lines = spark.read.text("/files/marvel-graph.txt")


connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0].cast(IntegerType())) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

connections = connections.filter(func.col("connections") > 0)
minConnections = connections.select(func.min("connections")).first()[0]
print(minConnections)
leastPopular = connections.filter(func.col("connections") == minConnections)
leastPopularWithNames = leastPopular.join(names, "id").select("name","connections").sort("name").show()


spark.stop()

