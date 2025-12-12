from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, lower, regexp_replace, col, window, current_timestamp

spark = SparkSession.builder.appName("NetworkWordCount").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
lines = spark.readStream.text("/files/fakestream/")

#lines = lines.withColumn("timestamp", current_timestamp())
lines = lines.select(regexp_replace(lower(lines.value), "[^a-zěščřžýáíéůú0-9\\s]", "").alias("value"))
words = lines.select(explode(split(lines.value,"\\s+")).alias("word"))
words = words.filter(col("word") != "")
wordCounts = words.groupBy(col("word")).count()
output_df = wordCounts.orderBy(col("count").desc())
query = output_df.writeStream.outputMode("complete").format("console").start()

query.awaitTermination()