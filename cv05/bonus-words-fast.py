from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, lower, col
import re

spark = SparkSession.builder.appName("JsonlWordCountDF").getOrCreate()
df = spark.read.json("/files/articles-clear.jsonl")
content_rdd_rows = df.select("content").rdd
content_rdd = content_rdd_rows.map(lambda row: row.content if row.content else "").filter(lambda text: text != "")

words = content_rdd.flatMap(lambda text: filter(None, re.split(r'\W+', text.lower())))
wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
top20 = wordCounts.filter(lambda item: len(item[0]) >= 6).sortBy(lambda item: item[1], ascending=False).take(20)

print("Top 20 nejpoužívanějších slov z pole 'content' (DataFrame metoda):")
for word, count in top20:
    print(f"{word}: {count}")

spark.stop()