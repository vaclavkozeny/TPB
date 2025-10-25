from pyspark import SparkConf, SparkContext
import re
import json
conf = SparkConf().setMaster("spark://spark-master:7077").setAppName("WordCount")
# conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("/files/customer-orders.csv")
pairs = input.map(lambda x: (x.split(",")[0], float(x.split(",")[2]))).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1], ascending=False).collect()

for customer_id, total_amount in pairs:
    print(f'{customer_id}: {total_amount:.2f}')