from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, DataTypes, Schema
from pyflink.table.expressions import col
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)


td = (TableDescriptor.for_connector("filesystem")
	.schema(Schema.new_builder()
		.column("userId", DataTypes.INT())
		.column("movieId", DataTypes.INT())
		.column("rating", DataTypes.INT())
		.column("timestamp", DataTypes.BIGINT())
		.build())
	.option("path", "/files/u.data")
	.format("csv") 
	.option("csv.field-delimiter", "\t")
	.build())
table_env.create_temporary_table("ratings",td)
rating = table_env.from_path("ratings")

td2 = (TableDescriptor.for_connector("filesystem")
    .schema(Schema.new_builder()
        .column("id", DataTypes.INT())
        .column("name", DataTypes.STRING())
        .build())
    .option("path", "/files/u-mod.item")
    .format("csv") 
    .option("csv.field-delimiter", "|")
    .build())
table_env.create_temporary_table("movies",td2)
movies = table_env.from_path("movies")

top10 = rating \
    .filter(col("rating") == 5) \
    .group_by(col("movieId")) \
    .select(col("movieId"), col("movieId").count.alias("count_5")) \
    .order_by(col("count_5").desc) \
    .limit(10)
ratings = rating\
	.group_by(col("movieId")) \
    .select(col("movieId").alias("ratings_movieId"), col("movieId").count.alias("count_all"))
results = top10\
    .join(movies,col("movieId") == col("id"))\
    .join(ratings, col("movieId") == col("ratings_movieId"))\
    .select(col("name"), col("count_5").alias("5* ratigns"), (col("count_5")/col("count_all").cast(DataTypes.FLOAT())).alias("ratio"))\
    .order_by(col("ratio").desc)\
    .execute()\
    .print()
