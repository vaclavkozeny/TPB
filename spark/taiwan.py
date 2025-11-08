from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import time


spark = SparkSession.builder.master("spark://spark-master:7077").appName("SparkSQL").getOrCreate()
df = spark.read.option("header", "true").option("inferSchema", "true").csv("/files/realestate.csv")
df.printSchema()
df.show(5)

assembler = VectorAssembler(inputCols=["HouseAge", "DistanceToMRT", "NumberConvenienceStores"], outputCol="features")
data = assembler.transform(df).select("features", "PriceOfUnitArea")
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

dt_classifier = DecisionTreeRegressor(labelCol="PriceOfUnitArea", featuresCol="features")
model = dt_classifier.fit(train_data)

predictions = model.transform(test_data)

print("Porovnání predikovaných a reálných hodnot:")
predictions.select("prediction", "PriceOfUnitArea").show()

evaluator = RegressionEvaluator(labelCol="PriceOfUnitArea", predictionCol="prediction", metricName="r2")
accuracy = evaluator.evaluate(predictions)

print(f"R2: {accuracy:.2f}")
spark.stop()
