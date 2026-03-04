# Consumes from Kafka topic and writes to MongoDB

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *
from pymongo import MongoClient
import logging
import json
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("KafkaFlightConsumer")

KAFKA_BOOTSTRAP = "broker:29092"
KAFKA_TOPIC = "flight-producer"
_mongo_user = os.environ.get("MONGO_ROOT_USERNAME", "root")
_mongo_pass = os.environ.get("MONGO_ROOT_PASSWORD", "example")
MONGO_URI = f"mongodb://{_mongo_user}:{_mongo_pass}@mongodb:27017"
MONGO_DB = "flightdb"
MONGO_COLLECTION = "flights"


def create_spark_session():
    return SparkSession.builder \
        .appName("KafkaFlightConsumer") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
        .getOrCreate()


def get_flight_schema():
    """
    Schema matching the API response for a single flight.
    Each Kafka message is ONE flight (not wrapped in 'flights' array).
    """
    return StructType([
        StructField("flight_id", IntegerType()),
        StructField("airline", StringType()),
        StructField("departure_city", StringType()),
        StructField("departure_airport", StringType()),
        StructField("arrival_city", StringType()),
        StructField("arrival_airport", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("dest_lat", DoubleType()),
        StructField("dest_lon", DoubleType()),
        StructField("current_altitude_m", DoubleType()),
        StructField("current_speed_km_h", DoubleType()),
        StructField("speed", DoubleType()),
        StructField("direction", DoubleType()),
        StructField("distance_travelled_km", DoubleType()),
        StructField("flight_status", StringType()),
        StructField("scheduled_departure_time", StringType()),
        StructField("actual_departure_time", StringType()),
        StructField("scheduled_arrival_time", StringType()),
        StructField("actual_landed_time", StringType()),
        StructField("start_time", StringType()),
        StructField("current_location", StructType([
            StructField("latitude", DoubleType()),
            StructField("longitude", DoubleType())
        ])),
    ])


def write_to_mongo(records):
    """Write a list of dicts to MongoDB"""
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        if records:
            collection.insert_many(records)
            logger.info(f"Inserted {len(records)} records into MongoDB")
        client.close()
    except Exception as e:
        logger.error(f"MongoDB write failed: {e}")


def process_batch(df, epoch_id):
    """Process each micro-batch from Spark Streaming"""
    if df.isEmpty():
        logger.info(f"[Epoch {epoch_id}] Empty batch, skipping.")
        return

    count = df.count()
    logger.info(f"[Epoch {epoch_id}] Processing {count} records")

    try:
        # Convert Spark DataFrame -> list of dicts -> MongoDB (no pandas needed)
        records = [row.asDict(recursive=True) for row in df.collect()]

        write_to_mongo(records)
        logger.info(f"[Epoch {epoch_id}] Batch processed successfully")
    except Exception as e:
        logger.error(f"[Epoch {epoch_id}] Error: {e}")


def main():
    logger.info("=" * 50)
    logger.info("Starting Kafka Flight Consumer -> MongoDB")
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP} / {KAFKA_TOPIC}")
    logger.info(f"MongoDB: {MONGO_URI}/{MONGO_DB}.{MONGO_COLLECTION}")
    logger.info("=" * 50)

    spark = create_spark_session()
    schema = get_flight_schema()

    # Read from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON from Kafka value
    # Each message is a single flight JSON (not wrapped in array)
    parsed_df = raw_df.select(
        from_json(col("value").cast("string"), schema).alias("flight")
    )

    # Flatten: select all fields from the parsed struct
    flight_df = parsed_df.select("flight.*")

    # Write stream using foreachBatch
    query = flight_df.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoint-flight-consumer") \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()