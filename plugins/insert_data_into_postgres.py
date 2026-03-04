# plugins/insert_data_into_postgres.py
# Extracts landed flights from MongoDB, inserts into PostgreSQL
# Called by Airflow DAG

from pymongo import MongoClient
from datetime import datetime
import psycopg2
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("MongoToPostgres")

_mongo_user = os.environ.get("MONGO_ROOT_USERNAME", "root")
_mongo_pass = os.environ.get("MONGO_ROOT_PASSWORD", "example")
MONGO_URI = f"mongodb://{_mongo_user}:{_mongo_pass}@mongodb:27017"
MONGO_DB = "flightdb"
MONGO_COLLECTION = "flights"

PG_CONFIG = {
    "dbname": os.environ.get("POSTGRES_DB", "airflow"),
    "user": os.environ.get("POSTGRES_USER", "airflow"),
    "password": os.environ.get("POSTGRES_PASSWORD", "airflow"),
    "host": "postgres",
    "port": "5432"
}


def mongo_connection():
    """Connect to MongoDB and return the flights collection"""
    try:
        logger.info("Connecting to MongoDB...")
        client = MongoClient(MONGO_URI)
        # Test connection
        client.admin.command('ping')
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        logger.info("Connected to MongoDB successfully.")
        return client, collection
    except Exception as e:
        logger.error(f"MongoDB connection failed: {e}")
        raise


def postgres_connection():
    """Connect to PostgreSQL"""
    try:
        logger.info("Connecting to PostgreSQL...")
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()
        logger.info("Connected to PostgreSQL successfully.")
        return cursor, conn
    except Exception as e:
        logger.error(f"PostgreSQL connection failed: {e}")
        raise


def create_table_if_not_exists(cursor, conn):
    """Create the flights table if it doesn't exist"""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS flights (
            id SERIAL PRIMARY KEY,
            flight_id INTEGER,
            airline VARCHAR(100),
            flight_status VARCHAR(20),
            departure_city VARCHAR(100),
            departure_airport VARCHAR(200),
            arrival_city VARCHAR(100),
            arrival_airport VARCHAR(200),
            scheduled_departure_time TIMESTAMP,
            actual_departure_time TIMESTAMP,
            scheduled_arrival_time TIMESTAMP,
            actual_landed_time TIMESTAMP,
            delayed_status VARCHAR(30),
            inserted_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (flight_id, actual_landed_time)
        );
    """)
    conn.commit()
    logger.info("Table 'flights' is ready.")


def get_landed_flights(collection):
    """Fetch only landed flights from MongoDB"""
    try:
        logger.info("Fetching landed flights from MongoDB...")
        query = {"actual_landed_time": {"$ne": None}}
        projection = {
            "_id": 0,
            "flight_id": 1,
            "airline": 1,
            "flight_status": 1,
            "departure_city": 1,
            "departure_airport": 1,
            "arrival_city": 1,
            "arrival_airport": 1,
            "scheduled_departure_time": 1,
            "actual_departure_time": 1,
            "scheduled_arrival_time": 1,
            "actual_landed_time": 1,
        }
        data = list(collection.find(query, projection))
        logger.info(f"Found {len(data)} landed flights.")
        return data
    except Exception as e:
        logger.error(f"Error fetching from MongoDB: {e}")
        raise


def calculate_delay_status(scheduled_arrival, actual_landed):
    """Determine if a flight was on time, slightly delayed, or too late"""
    if not scheduled_arrival or not actual_landed:
        return None

    if isinstance(scheduled_arrival, str):
        scheduled_arrival = datetime.fromisoformat(scheduled_arrival)
    if isinstance(actual_landed, str):
        actual_landed = datetime.fromisoformat(actual_landed)

    delay_minutes = (actual_landed - scheduled_arrival).total_seconds() / 60

    if delay_minutes < 15:
        return "on_time"
    elif delay_minutes < 30:
        return "slightly_delayed"
    else:
        return "too_late"


def insert_into_postgres(cursor, conn, flights):
    """Insert landed flights into PostgreSQL"""
    try:
        logger.info(f"Inserting {len(flights)} records into PostgreSQL...")
        inserted = 0

        for doc in flights:
            delayed_status = calculate_delay_status(
                doc.get("scheduled_arrival_time"),
                doc.get("actual_landed_time")
            )

            cursor.execute("""
                INSERT INTO flights (
                    flight_id, airline, flight_status,
                    departure_city, departure_airport,
                    arrival_city, arrival_airport,
                    scheduled_departure_time, actual_departure_time,
                    scheduled_arrival_time, actual_landed_time,
                    delayed_status
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (flight_id, actual_landed_time) DO NOTHING
            """, (
                doc.get("flight_id"),
                doc.get("airline"),
                doc.get("flight_status"),
                doc.get("departure_city"),
                doc.get("departure_airport"),
                doc.get("arrival_city"),
                doc.get("arrival_airport"),
                doc.get("scheduled_departure_time"),
                doc.get("actual_departure_time"),
                doc.get("scheduled_arrival_time"),
                doc.get("actual_landed_time"),
                delayed_status
            ))
            inserted += 1

        conn.commit()
        logger.info(f"Successfully inserted {inserted} records into PostgreSQL.")
    except Exception as e:
        logger.error(f"PostgreSQL insert failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()
        logger.info("PostgreSQL connection closed.")


def get_and_insert_data():
    """Main function - called by Airflow DAG"""
    try:
        logger.info("=" * 50)
        logger.info("Starting MongoDB -> PostgreSQL transfer")
        logger.info("=" * 50)

        mongo_client, collection = mongo_connection()
        cursor, conn = postgres_connection()

        # Ensure table exists
        create_table_if_not_exists(cursor, conn)
        # Re-open connection after commit
        cursor, conn = postgres_connection()

        flights = get_landed_flights(collection)

        if flights:
            insert_into_postgres(cursor, conn, flights)
        else:
            logger.info("No landed flights to insert.")

        mongo_client.close()
        logger.info("Transfer complete.")
    except Exception as e:
        logger.error(f"Transfer failed: {e}")
        raise


if __name__ == "__main__":
    get_and_insert_data()