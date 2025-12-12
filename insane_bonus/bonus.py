from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, Schema, FormatDescriptor, TableDescriptor, expressions
from pyflink.table.expressions import col, current_timestamp, call, lit
from pyflink.table.window import Slide

# Set up the streaming environment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(environment_settings=env_settings)

# Create the Kafka source table
table_env.create_temporary_table(
    'kafka_source',
    TableDescriptor.for_connector('kafka')
        .schema(Schema.new_builder()
                .column('value', DataTypes.STRING())
                .column_by_expression("received_at", "PROCTIME()")               
                .build())
        .option('topic', 'test-topic')
        .option('properties.bootstrap.servers', 'kafka:9092')
        .option('properties.group.id', 'flink-group')
        .option('scan.startup.mode', 'latest-offset')
        .format(FormatDescriptor.for_format('raw')
                .build())
        .build())


table_env.create_temporary_table(
    'print_sink',
    TableDescriptor.for_connector("print")
        .schema(Schema.new_builder()
                .column('title', DataTypes.STRING())
                .column('comments', DataTypes.INT())
                .column('date', DataTypes.TIMESTAMP_LTZ(3))
                .column('received_at', DataTypes.TIMESTAMP_LTZ(3))
                .build())
        .build())

table_env.create_temporary_table(
    'console_sink',
    TableDescriptor.for_connector("print")
        .schema(Schema.new_builder()
                .column('title', DataTypes.STRING())
                .build())
        .build())

table_env.create_temporary_table(
    'comments_sink',
    TableDescriptor.for_connector("filesystem")
        .schema(Schema.new_builder()
                .column('title', DataTypes.STRING())
                .column('comments', DataTypes.INT())
                .build())
        .option("path", "/files/active_articles.csv")
        .option("format", "csv")
        .option("csv.field-delimiter", ";")
        .build())

table_env.create_temporary_table(
    'window_sink',
    TableDescriptor.for_connector("filesystem")
        .schema(Schema.new_builder()
                .column('window_start', DataTypes.TIMESTAMP_LTZ(3))
                .column('window_end', DataTypes.TIMESTAMP_LTZ(3))
                .column('article_count', DataTypes.BIGINT())
                .column('war_count', DataTypes.BIGINT())
                .build())
        .option("path", "/files/window_analysis.csv")
        .option("format", "csv")
        .option("csv.field-delimiter", ";")
        .build())

data = table_env.from_path("kafka_source")

selected = data.select(
    col("value").json_value("$.title").alias("title"),
    col("value").json_value("$.content").alias("content"),
    col("value").json_value("$.disc").cast(DataTypes.INT()).alias("comments"),
    (call("REPLACE", col("value").json_value("$.time"), "T", " ") + ".000")
        .cast(DataTypes.TIMESTAMP_LTZ(3))
        .alias("date"),
    col("received_at")
)

titles_only = selected.select(col("title"))

active_articles = selected \
        .filter(col("comments") > 100) \
        .select(col("title"), col("comments"))

windowed_data = selected \
    .window(Slide.over(lit(1).minutes).every(lit(10).seconds).on(col("received_at")).alias("w")) \
    .group_by(col("w")) \
    .select(
        col("w").start.alias("window_start"),
        col("w").end.alias("window_end"),
        col("title").count.alias("article_count"),
        col("content").like("%válka%").cast(DataTypes.INT()).sum.alias("war_count")
    )

statement_set = table_env.create_statement_set()
statement_set.add_insert("comments_sink", active_articles)
#statement_set.add_insert("anomalies_sink", anomalies)
statement_set.add_insert("console_sink", titles_only)
statement_set.add_insert("window_sink", windowed_data)

statement_set.execute().wait()


#selected.execute_insert("print_sink").wait()
