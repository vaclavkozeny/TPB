from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, lower, regexp_replace, col, window, current_timestamp

spark = SparkSession.builder.appName("NetworkWordCount").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

lines = lines.withColumn("timestamp", current_timestamp())
lines = lines.select(regexp_replace(lower(lines.value), "[^a-zěščřžýáíéůú0-9\\s]", "").alias("value"),col("timestamp"))
words = lines.select(explode(split(lines.value," ")).alias("word"),col("timestamp"))
wordCounts = words.groupBy(
    window(col("timestamp"), "30 seconds", "15 seconds"),
    col("word")
    ).count()
output_df = wordCounts.select(
    col("window.start").alias("start"),
    col("window.end").alias("end"),
    col("word"),
    col("count")
).orderBy(col("start").asc(), col("count").desc())
query = output_df.writeStream.outputMode("complete").format("console").start()

query.awaitTermination()