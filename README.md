
# Kafka-Spark-Postgres

## Run spark kafka and postgres

```bash
docker-compose up -d
```

## install python dependencies in spark container

Enter the spark container

```bash
docker compose run spark bash
```

install python dependencies

```bash
pip install -r requirements.txt
```

## send data to kafka from rest api

```bash
   spark-submit \
    --master spark://spark:7077 \
    --conf spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
   /script/kafkasend.py
```

## recive data from kafka and save to postgres

```bash

      spark-submit \
    --master spark://spark:7077 \
    --conf spark.jars.packages=org.postgresql:postgresql:42.7.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
   /script/kafkarecive.py
```
