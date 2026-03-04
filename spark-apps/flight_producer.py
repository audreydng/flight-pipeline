# Fetches data from Flight API and sends to Kafka topic

from pyspark.sql import SparkSession
import json
import logging
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("KafkaFlightProducer")

KAFKA_BOOTSTRAP = 'broker:29092'
KAFKA_TOPIC = 'flight-producer'
API_URL = "http://host.docker.internal:5001/api/flights"

def spark_session():
    return SparkSession.builder \
        .appName("KafkaProducerStreaming") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
        .getOrCreate()


def fetch_data():
    """Fetch flight data from the API"""
    try:
        response = requests.get(API_URL, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"API error {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Request error: {e}")
        return None


def send_to_kafka(spark, data):
    """
    Send each flight as a separate Kafka message.
    This makes downstream consumption easier.
    """
    try:
        flights = data.get("flights", [])
        if not flights:
            logger.warning("No flights in data.")
            return

        # Each flight becomes one Kafka message (JSON string)
        rows = [(json.dumps(flight),) for flight in flights]
        df = spark.createDataFrame(rows, ['value'])

        df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
            .option("topic", KAFKA_TOPIC) \
            .save()

        logger.info(f"Sent {len(rows)} flights to Kafka topic '{KAFKA_TOPIC}'")
    except Exception as e:
        logger.error(f"Error sending to Kafka: {e}")


def process_stream(df, epoch_id):
    """
    Called for each micro-batch.
    Fetches fresh API data and pushes to Kafka.
    """
    spark = spark_session()
    data = fetch_data()
    if not data:
        logger.warning(f"[Epoch {epoch_id}] No data to process.")
        return

    send_to_kafka(spark, data)

    for flight in data.get('flights', []):
        dep = flight.get("departure_city", "?")
        arr = flight.get("arrival_city", "?")
        status = flight.get("flight_status", "?")
        logger.info(f"[Epoch {epoch_id}] {dep} -> {arr} ({status})")


def main():
    logger.info("=" * 50)
    logger.info("Starting Kafka Flight Producer")
    logger.info(f"API: {API_URL}")
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP} / {KAFKA_TOPIC}")
    logger.info("=" * 50)

    spark = spark_session()

    # Use rate source as a trigger mechanism
    # Generates rows at fixed rate -> triggers foreachBatch
    trigger_df = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", 1) \
        .load()

    query = trigger_df.writeStream \
        .foreachBatch(process_stream) \
        .option("checkpointLocation", "/tmp/checkpoint-flight-producer") \
        .trigger(processingTime="5 seconds") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()