from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests, json

spark: SparkSession = SparkSession.builder \
.appName("API to Kafka") \
.master("spark://spark:7077") \
.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
.getOrCreate()


def read_api(url: str):
    data = requests.get(url).json()
    return json.dumps(data)

api_url = "https://jsonplaceholder.typicode.com/posts"
# Read data into Data Frame
# Create payload rdd
payload = json.loads(read_api(api_url))
payload_rdd = spark.sparkContext.parallelize([payload])
# Read from JSON
api_data = spark.read.json(payload_rdd)
api_data.printSchema()

api_data.show(truncate=False)

# Write data to Kafka topic
kafka_params = {
    "kafka.bootstrap.servers": "kafka:9092",
    "topic": "api_data_topic"
}

api_data.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
    .write \
    .format("kafka") \
    .options(**kafka_params) \
    .save()



# Stop SparkSession
spark.stop()
