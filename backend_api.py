# Backend API for the frontend to fetch data from MongoDB and PostgreSQL
# Local access: http://localhost:5002

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from datetime import datetime
import psycopg2
import logging
import os

app = FastAPI(title="Flight Pipeline Backend API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("BackendAPI")

# CONFIG — run from local, connect to Docker containers

_mongo_user = os.environ.get("MONGO_ROOT_USERNAME", "root")
_mongo_pass = os.environ.get("MONGO_ROOT_PASSWORD", "example")
_mongo_host = os.environ.get("MONGO_HOST", "localhost")
MONGO_URI = f"mongodb://{_mongo_user}:{_mongo_pass}@{_mongo_host}:27017"
MONGO_DB = "flightdb"
MONGO_COLLECTION = "flights"

PG_CONFIG = {
    "dbname": os.environ.get("POSTGRES_DB", "airflow"),
    "user": os.environ.get("POSTGRES_USER", "airflow"),
    "password": os.environ.get("POSTGRES_PASSWORD", "airflow"),
    "host": os.environ.get("POSTGRES_HOST", "localhost"),
    "port": "5432"
}


# MONGODB ENDPOINTS — Real-time flight data

@app.get("/api/live-flights")
def get_live_flights():
    """Get active flights from MongoDB (real-time data from Spark)"""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        db = client[MONGO_DB]
        col = db[MONGO_COLLECTION]

        active = list(col.find({"flight_status": "active"}, {"_id": 0}).sort("_id", -1).limit(50))
        landed = list(col.find({"flight_status": "landed"}, {"_id": 0}).sort("_id", -1).limit(20))

        client.close()

        return {
            "active_flights": active,
            "landed_flights": landed,
            "active_count": len(active),
            "landed_count": len(landed),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"MongoDB error: {e}")
        return {
            "active_flights": [],
            "landed_flights": [],
            "active_count": 0,
            "landed_count": 0,
            "timestamp": datetime.now().isoformat(),
            "error": "MongoDB unavailable"
        }


@app.get("/api/mongo-stats")
def get_mongo_stats():
    """Get MongoDB collection statistics"""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        db = client[MONGO_DB]
        col = db[MONGO_COLLECTION]

        total = col.count_documents({})
        active = col.count_documents({"flight_status": "active"})
        landed = col.count_documents({"flight_status": "landed"})
        airlines = col.distinct("airline")

        client.close()

        return {
            "total_records": total,
            "active_flights": active,
            "landed_flights": landed,
            "unique_airlines": len(airlines),
            "airlines": sorted(airlines) if airlines else []
        }
    except Exception as e:
        logger.error(f"MongoDB stats error: {e}")
        return {
            "total_records": 0,
            "active_flights": 0,
            "landed_flights": 0,
            "unique_airlines": 0,
            "airlines": [],
            "error": "MongoDB unavailable"
        }


# POSTGRESQL ENDPOINTS — Processed/reported data

def pg_query(query, params=None):
    """Helper to run PostgreSQL queries"""
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    cur.execute(query, params)
    cols = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    conn.close()
    return [dict(zip(cols, row)) for row in rows]


@app.get("/api/pg-flights")
def get_pg_flights():
    """Get processed flights from PostgreSQL"""
    try:
        flights = pg_query("SELECT * FROM flights ORDER BY inserted_at DESC LIMIT 100")
        return {"flights": flights, "count": len(flights)}
    except Exception as e:
        logger.error(f"PostgreSQL flights error: {e}")
        return {"flights": [], "count": 0, "error": "PostgreSQL unavailable"}


@app.get("/api/delay-distribution")
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
        return {"distribution": data}
    except Exception as e:
        logger.error(f"Delay distribution error: {e}")
        return {"distribution": [], "error": "PostgreSQL unavailable"}


@app.get("/api/top-airlines")
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
        return {"airlines": data}
    except Exception as e:
        logger.error(f"Top airlines error: {e}")
        return {"airlines": [], "error": "PostgreSQL unavailable"}


@app.get("/api/delay-ratio")
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
        return {"airlines": data}
    except Exception as e:
        logger.error(f"Delay ratio error: {e}")
        return {"airlines": [], "error": "PostgreSQL unavailable"}


# PIPELINE STATUS

@app.get("/api/pipeline-status")
def get_pipeline_status():
    """Check health of all pipeline components"""
    status = {}

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        client.admin.command('ping')
        mongo_count = client[MONGO_DB][MONGO_COLLECTION].count_documents({})
        client.close()
        status["mongodb"] = {"status": "connected", "records": mongo_count}
    except Exception as e:
        status["mongodb"] = {"status": "disconnected", "error": str(e)}

    try:
        conn = psycopg2.connect(**PG_CONFIG, connect_timeout=3)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM flights")
        pg_count = cur.fetchone()[0]
        conn.close()
        status["postgresql"] = {"status": "connected", "records": pg_count}
    except Exception as e:
        status["postgresql"] = {"status": "disconnected", "error": str(e)}

    return {
        "pipeline": status,
        "timestamp": datetime.now().isoformat()
    }


@app.get("/")
def index():
    return {
        "message": "Flight Pipeline Backend API",
        "endpoints": {
            "live_flights": "/api/live-flights",
            "mongo_stats": "/api/mongo-stats",
            "pg_flights": "/api/pg-flights",
            "delay_distribution": "/api/delay-distribution",
            "top_airlines": "/api/top-airlines",
            "delay_ratio": "/api/delay-ratio",
            "pipeline_status": "/api/pipeline-status",
            "docs": "/docs"
        }
    }


if __name__ == '__main__':
    import uvicorn
    print("=" * 50)
    print("  Flight Pipeline Backend API")
    print("  http://localhost:5002")
    print("  Docs: http://localhost:5002/docs")
    print("=" * 50)
    uvicorn.run("backend_api:app", host="0.0.0.0", port=5002, reload=False)
