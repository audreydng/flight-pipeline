# plugins/daily_report.py
# Queries PostgreSQL and generates CSV reports
# Called by Airflow DAG

import os
import psycopg2
import logging
import pandas as pd

OUTPUT_DIR = "/opt/airflow/outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("DailyReport")

PG_CONFIG = {
    "dbname": os.environ.get("POSTGRES_DB", "airflow"),
    "user": os.environ.get("POSTGRES_USER", "airflow"),
    "password": os.environ.get("POSTGRES_PASSWORD", "airflow"),
    "host": "postgres",
    "port": "5432"
}


def postgres_connection():
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()
        logger.info("Connected to PostgreSQL.")
        return cursor, conn
    except Exception as e:
        logger.error(f"PostgreSQL connection failed: {e}")
        raise


def query_delay_distribution(cursor):
    """How many flights were on_time, slightly_delayed, too_late?"""
    cursor.execute("""
        SELECT delayed_status, COUNT(*) as count
        FROM flights
        WHERE delayed_status IS NOT NULL
        GROUP BY delayed_status
        ORDER BY count DESC;
    """)
    return cursor.fetchall()


def query_top_airlines(cursor):
    """Top 5 airlines by number of flights"""
    cursor.execute("""
        SELECT airline, COUNT(*) as flight_count
        FROM flights
        GROUP BY airline
        ORDER BY flight_count DESC
        LIMIT 5;
    """)
    return cursor.fetchall()


def query_delay_ratio_by_airline(cursor):
    """Top 10 airlines with highest 'too_late' ratio"""
    cursor.execute("""
        SELECT
            airline,
            COUNT(*) as total_flights,
            COUNT(CASE WHEN delayed_status = 'too_late' THEN 1 END) as late_flights,
            ROUND(
                COUNT(CASE WHEN delayed_status = 'too_late' THEN 1 END) * 100.0 / 
                NULLIF(COUNT(*), 0), 2
            ) as late_percentage
        FROM flights
        WHERE delayed_status IS NOT NULL
        GROUP BY airline
        HAVING COUNT(*) >= 2
        ORDER BY late_percentage DESC
        LIMIT 10;
    """)
    return cursor.fetchall()


def write_csv(filename, data, columns):
    """Write query results to CSV using pandas"""
    try:
        filepath = os.path.join(OUTPUT_DIR, filename)
        df = pd.DataFrame(data, columns=columns)
        df.to_csv(filepath, index=False)
        logger.info(f"Written: {filepath} ({len(data)} rows)")
    except Exception as e:
        logger.error(f"Failed to write {filename}: {e}")


def generate_daily_report():
    """Main function - called by Airflow DAG"""
    try:
        logger.info("=" * 50)
        logger.info("Generating Daily Flight Report")
        logger.info("=" * 50)

        cursor, conn = postgres_connection()

        # Report 1: Delay distribution
        data = query_delay_distribution(cursor)
        write_csv("delay_distribution.csv", data,
                  ["delayed_status", "count"])

        # Report 2: Top airlines
        data = query_top_airlines(cursor)
        write_csv("top_5_airlines.csv", data,
                  ["airline", "flight_count"])

        # Report 3: Delay ratio
        data = query_delay_ratio_by_airline(cursor)
        write_csv("top_10_late_ratio_airlines.csv", data,
                  ["airline", "total_flights", "late_flights", "late_percentage"])

        conn.close()
        logger.info("Daily report generation complete.")
        logger.info(f"Reports saved to: {OUTPUT_DIR}")
    except Exception as e:
        logger.error(f"Report generation failed: {e}")
        raise


if __name__ == "__main__":
    generate_daily_report()