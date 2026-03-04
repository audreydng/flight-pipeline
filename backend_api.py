# Backend API for the frontend to fetch data from MongoDB and PostgreSQL
# # Local access: http://localhost:5002

from flask import Flask, jsonify
from flask_cors import CORS
from pymongo import MongoClient
from datetime import datetime
import psycopg2
import logging
import os

app = Flask(__name__)
CORS(app)  # Allows frontend to call API

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("BackendAPI")

# CONFIG — run from local, connect to Docker containers

_mongo_user = os.environ.get("MONGO_ROOT_USERNAME", "root")
_mongo_pass = os.environ.get("MONGO_ROOT_PASSWORD", "example")
MONGO_URI = f"mongodb://{_mongo_user}:{_mongo_pass}@localhost:27017"
MONGO_DB = "flightdb"
MONGO_COLLECTION = "flights"

PG_CONFIG = {
    "dbname": os.environ.get("POSTGRES_DB", "airflow"),
    "user": os.environ.get("POSTGRES_USER", "airflow"),
    "password": os.environ.get("POSTGRES_PASSWORD", "airflow"),
    "host": "localhost",
    "port": "5432"
}


# MONGODB ENDPOINTS — Real-time flight data
@app.route('/api/live-flights', methods=['GET'])
def get_live_flights():
    """Get active flights from MongoDB (real-time data from Spark)"""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        db = client[MONGO_DB]
        col = db[MONGO_COLLECTION]

        # Get latest flights (active ones)
        active = list(col.find(
            {"flight_status": "active"},
            {"_id": 0}
        ).sort("_id", -1).limit(50))

        # Get recently landed
        landed = list(col.find(
            {"flight_status": "landed"},
            {"_id": 0}
        ).sort("_id", -1).limit(20))

        client.close()

        return jsonify({
            "active_flights": active,
            "landed_flights": landed,
            "active_count": len(active),
            "landed_count": len(landed),
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"MongoDB error: {e}")
        return jsonify({
            "active_flights": [],
            "landed_flights": [],
            "active_count": 0,
            "landed_count": 0,
            "timestamp": datetime.now().isoformat(),
            "error": "MongoDB unavailable"
        })


@app.route('/api/mongo-stats', methods=['GET'])
def get_mongo_stats():
    """Get MongoDB collection statistics"""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        db = client[MONGO_DB]
        col = db[MONGO_COLLECTION]

        total = col.count_documents({})
        active = col.count_documents({"flight_status": "active"})
        landed = col.count_documents({"flight_status": "landed"})

        # Get unique airlines
        airlines = col.distinct("airline")

        client.close()

        return jsonify({
            "total_records": total,
            "active_flights": active,
            "landed_flights": landed,
            "unique_airlines": len(airlines),
            "airlines": sorted(airlines) if airlines else []
        })
    except Exception as e:
        logger.error(f"MongoDB stats error: {e}")
        return jsonify({
            "total_records": 0,
            "active_flights": 0,
            "landed_flights": 0,
            "unique_airlines": 0,
            "airlines": [],
            "error": "MongoDB unavailable"
        })


# POSTGRESQL ENDPOINTS - Processed/reported data
# 
def pg_query(query, params=None):
    """Helper to run PostgreSQL queries"""
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    cur.execute(query, params)
    cols = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    conn.close()
    return [dict(zip(cols, row)) for row in rows]


@app.route('/api/pg-flights', methods=['GET'])
def get_pg_flights():
    """Get processed flights from PostgreSQL"""
    try:
        flights = pg_query("""
            SELECT * FROM flights
            ORDER BY inserted_at DESC
            LIMIT 100
        """)
        return jsonify({
            "flights": flights,
            "count": len(flights)
        })
    except Exception as e:
        logger.error(f"PostgreSQL flights error: {e}")
        return jsonify({"flights": [], "count": 0, "error": "PostgreSQL unavailable"})


@app.route('/api/delay-distribution', methods=['GET'])
def get_delay_distribution():
    """Delay status distribution from PostgreSQL"""
    try:
        data = pg_query("""
            SELECT delayed_status, COUNT(*) as count
            FROM flights
            WHERE delayed_status IS NOT NULL
            GROUP BY delayed_status
            ORDER BY count DESC
        """)
        return jsonify({"distribution": data})
    except Exception as e:
        logger.error(f"Delay distribution error: {e}")
        return jsonify({"distribution": [], "error": "PostgreSQL unavailable"})


@app.route('/api/top-airlines', methods=['GET'])
def get_top_airlines():
    """Top airlines by flight count from PostgreSQL"""
    try:
        data = pg_query("""
            SELECT airline, COUNT(*) as flight_count
            FROM flights
            GROUP BY airline
            ORDER BY flight_count DESC
            LIMIT 10
        """)
        return jsonify({"airlines": data})
    except Exception as e:
        logger.error(f"Top airlines error: {e}")
        return jsonify({"airlines": [], "error": "PostgreSQL unavailable"})


@app.route('/api/delay-ratio', methods=['GET'])
def get_delay_ratio():
    """Airlines with highest delay ratio from PostgreSQL"""
    try:
        data = pg_query("""
            SELECT
                airline,
                COUNT(*) as total,
                COUNT(CASE WHEN delayed_status = 'too_late' THEN 1 END) as late,
                ROUND(
                    COUNT(CASE WHEN delayed_status = 'too_late' THEN 1 END) * 100.0 /
                    NULLIF(COUNT(*), 0), 2
                ) as late_pct
            FROM flights
            WHERE delayed_status IS NOT NULL
            GROUP BY airline
            HAVING COUNT(*) >= 2
            ORDER BY late_pct DESC
            LIMIT 10
        """)
        return jsonify({"airlines": data})
    except Exception as e:
        logger.error(f"Delay ratio error: {e}")
        return jsonify({"airlines": [], "error": "PostgreSQL unavailable"})



# PIPELINE STATUS

@app.route('/api/pipeline-status', methods=['GET'])
def get_pipeline_status():
    """Check health of all pipeline components"""
    status = {}

    # Check MongoDB
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        client.admin.command('ping')
        mongo_count = client[MONGO_DB][MONGO_COLLECTION].count_documents({})
        client.close()
        status["mongodb"] = {"status": "connected", "records": mongo_count}
    except Exception as e:
        status["mongodb"] = {"status": "disconnected", "error": str(e)}

    # Check PostgreSQL
    try:
        conn = psycopg2.connect(**PG_CONFIG, connect_timeout=3)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM flights")
        pg_count = cur.fetchone()[0]
        conn.close()
        status["postgresql"] = {"status": "connected", "records": pg_count}
    except Exception as e:
        status["postgresql"] = {"status": "disconnected", "error": str(e)}

    return jsonify({
        "pipeline": status,
        "timestamp": datetime.now().isoformat()
    })


@app.route('/', methods=['GET'])
def index():
    return jsonify({
        "message": "Flight Pipeline Backend API",
        "endpoints": {
            "live_flights": "/api/live-flights",
            "mongo_stats": "/api/mongo-stats",
            "pg_flights": "/api/pg-flights",
            "delay_distribution": "/api/delay-distribution",
            "top_airlines": "/api/top-airlines",
            "delay_ratio": "/api/delay-ratio",
            "pipeline_status": "/api/pipeline-status",
        }
    })


if __name__ == '__main__':
    print("=" * 50)
    print("  Flight Pipeline Backend API")
    print("  http://localhost:5002")
    print("=" * 50)
    app.run(host='0.0.0.0', port=5002, debug=True)