from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, VectorAssembler, OneHotEncoder, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import when, col
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.classification import RandomForestClassifier
import time

start = time.time()
spark = SparkSession.builder.master("spark://spark-master:7077").appName("SparkSQL").getOrCreate()
inputData = spark.read.option("header", "true").option("inferSchema", "true").option("nullValue", "NA") .csv("/files/output.csv")
#inputData.printSchema()
#inputData.show(5)

#df = inputData.na.drop("any")

#df = df.select(["Year","Month","DayofMonth","DayOfWeek","CRSDepTime","CRSArrTime","UniqueCarrier","CRSElapsedTime","Origin","Dest","Distance","ArrDelay"])
df = inputData.na.drop(subset=["Year","Month","DayofMonth","DayOfWeek","CRSDepTime","CRSArrTime","UniqueCarrier","CRSElapsedTime","Origin","Dest","Distance","ArrDelay"])
df = df.withColumn("Delay", when(col("ArrDelay") > 0, 1).otherwise(0))

df.show(5)

carrier_indexer = StringIndexer(inputCol="UniqueCarrier",outputCol="UniqueCarrierIndex",handleInvalid="keep")
carrier_encoder = OneHotEncoder(inputCol="UniqueCarrierIndex",outputCol="UniqueCarrierVec")
origin_indexer = StringIndexer(inputCol="Origin",outputCol="OriginIndex",handleInvalid="keep")
origin_encoder = OneHotEncoder(inputCol="OriginIndex",outputCol="OriginVec")
dest_indexer = StringIndexer(inputCol="Dest",outputCol="DestIndex",handleInvalid="keep")
dest_encoder = OneHotEncoder(inputCol="DestIndex",outputCol="DestVec")
assembler = VectorAssembler(inputCols=["Year","Month","DayofMonth","DayOfWeek","CRSDepTime","CRSArrTime","UniqueCarrierVec","CRSElapsedTime","OriginVec","DestVec","Distance"], outputCol="features")
log_reg_plane = LogisticRegression(featuresCol="features",labelCol="Delay")
pipeline = Pipeline(stages=[carrier_indexer,carrier_encoder,origin_indexer,origin_encoder,dest_indexer,dest_encoder,assembler,log_reg_plane])
train_data, test_data = df.randomSplit([0.9, 0.1], seed=42)

eval_accuracy = MulticlassClassificationEvaluator(
    labelCol="Delay", 
    predictionCol="prediction", 
    metricName="accuracy"
)
#paramGrid = ParamGridBuilder() \
#    .addGrid(log_reg_plane.regParam, [0.1, 0.01]) \
#    .addGrid(log_reg_plane.elasticNetParam, [0.0, 0.5, 1.0]) \
#    .build()

#cv = CrossValidator(
#    estimator=pipeline,
#    estimatorParamMaps=paramGrid,
#    evaluator=eval_accuracy,
#    numFolds=3 
#)


model = pipeline.fit(train_data)

results = model.transform(test_data)
results.select(["prediction","Delay"]).show()



accuracy = eval_accuracy.evaluate(results)

print(f"Accuracy: {accuracy * 100:.2f} %")

rf = RandomForestClassifier(featuresCol="features", labelCol="Delay", seed=42)

pipeline_rf = Pipeline(stages=[
    carrier_indexer,
    carrier_encoder,
    origin_indexer,
    origin_encoder,
    dest_indexer,
    dest_encoder,
    assembler,
    rf 
])

model_rf = pipeline_rf.fit(train_data)
results_rf = model_rf.transform(test_data)
accuracy_rf = eval_accuracy.evaluate(results_rf)


# --- SROVNÁNÍ SKÓRE ---
print("\n--- FINÁLNÍ SROVNÁNÍ ---")
print(f"Accuracy (Logistic Regression): {accuracy * 100:.2f} %")
print(f"Accuracy (Random Forest):       {accuracy_rf * 100:.2f} %")


spark.stop()
end = time.time()
print(f"Trvalo to {end - start:.2f} sekund")