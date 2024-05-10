from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize SparkSession

spark: SparkSession = SparkSession.builder \
.appName("Kafka to PostgreSQL") \
.master("spark://spark:7077") \
.config("spark.jars.packages", "org.postgresql:postgresql:42.7.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
.getOrCreate()

# Read data from Kafka topic
kafka_params = {
    "kafka.bootstrap.servers": "kafka:9092",
    "subscribe": "api_data_topic",
    "startingOffsets": "earliest"
}

kafka_df = spark.read.format("kafka") \
    .options(**kafka_params) \
    .load()

schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("body", StringType(), True)
])

# Deserialize JSON data
data_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

data_df.printSchema()
data_df.show(truncate=False)

# Write DataFrame to PostgreSQL
postgres_properties = {
    "url": "jdbc:postgresql://postgresql:5432/postgres",
    "user": "postgres",
    "password": "W0rldP0stgr3s",
    "driver": "org.postgresql.Driver",
    "dbtable": "api_data"
}

data_df.write.format("jdbc") \
    .options(**postgres_properties).save()
