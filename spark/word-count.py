from pyspark import SparkConf, SparkContext
import re
conf = SparkConf().setMaster("spark://spark-master:7077").setAppName("WordCount")
# conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("/files/book.txt")
words = input.flatMap(lambda x: filter(None, re.split(r'\W+', x.lower())))
wordCounts = words.countByValue()
sorted_wordCounts = sorted(wordCounts.items(), key=lambda x: x[1], reverse=True)

for word, count in sorted_wordCounts[:20]:
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
