# Flight Map API
# Local access: http://localhost:5000/api/flights
# Docker: http://host.docker.internal:5000/api/flights

from flask import Flask, jsonify
from datetime import datetime, timedelta
import random
import math
import threading
import time

app = Flask(__name__)


# FLIGHT DATA

AIRLINES = [
    "Vietnam Airlines", "Qatar Airways", "Emirates", "Lufthansa",
    "Singapore Airlines", "Turkish Airlines", "Japan Airlines",
    "Korean Air", "Delta Air Lines", "United Airlines",
    "British Airways", "Air France", "Cathay Pacific",
    "Thai Airways", "EVA Air", "ANA", "Etihad Airways",
    "Swiss International", "KLM Royal Dutch", "Qantas"
]

AIRPORTS = [
    {"city": "Ho Chi Minh City", "airport": "Tan Son Nhat International Airport", "lat": 10.8188, "lon": 106.6520},
    {"city": "Hanoi", "airport": "Noi Bai International Airport", "lat": 21.2212, "lon": 105.8070},
    {"city": "Dubai", "airport": "Dubai International Airport", "lat": 25.2532, "lon": 55.3657},
    {"city": "Singapore", "airport": "Changi Airport", "lat": 1.3644, "lon": 103.9915},
    {"city": "Tokyo", "airport": "Narita International Airport", "lat": 35.7647, "lon": 140.3864},
    {"city": "Seoul", "airport": "Incheon International Airport", "lat": 37.4602, "lon": 126.4407},
    {"city": "London", "airport": "Heathrow Airport", "lat": 51.4700, "lon": -0.4543},
    {"city": "Paris", "airport": "Charles de Gaulle Airport", "lat": 49.0097, "lon": 2.5479},
    {"city": "New York", "airport": "John F. Kennedy International Airport", "lat": 40.6413, "lon": -73.7781},
    {"city": "Sydney", "airport": "Kingsford Smith Airport", "lat": -33.9461, "lon": 151.1772},
    {"city": "Bangkok", "airport": "Suvarnabhumi Airport", "lat": 13.6900, "lon": 100.7501},
    {"city": "Istanbul", "airport": "Istanbul Airport", "lat": 41.2608, "lon": 28.7418},
    {"city": "Auckland", "airport": "Auckland Airport", "lat": -37.0082, "lon": 174.7850},
    {"city": "Los Angeles", "airport": "LAX International Airport", "lat": 33.9416, "lon": -118.4085},
    {"city": "Frankfurt", "airport": "Frankfurt Airport", "lat": 50.0379, "lon": 8.5622},
]


# HELPER FUNCTIONS

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two points in km"""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

def calculate_bearing(lat1, lon1, lat2, lon2):
    """Calculate bearing between two points"""
    dlon = math.radians(lon2 - lon1)
    lat1, lat2 = math.radians(lat1), math.radians(lat2)
    x = math.sin(dlon) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


# FLIGHT GENERATOR

class FlightSimulator:
    def __init__(self, num_flights=15):
        self.flights = []
        self.next_id = 1
        self.lock = threading.Lock()
        self._init_flights(num_flights)

    def _init_flights(self, count):
        for _ in range(count):
            self._create_flight()

    def _create_flight(self):
        dep = random.choice(AIRPORTS)
        arr = random.choice([a for a in AIRPORTS if a["city"] != dep["city"]])
        
        distance = haversine_distance(dep["lat"], dep["lon"], arr["lat"], arr["lon"])
        speed_kmh = random.randint(800, 950)
        flight_duration_hours = distance / speed_kmh
        flight_duration_minutes = flight_duration_hours * 60

        now = datetime.now()
        # Some flights departed in the past (active), some very recently
        minutes_ago = random.randint(5, int(max(flight_duration_minutes * 0.9, 30)))
        actual_dep = now - timedelta(minutes=minutes_ago)
        scheduled_dep = actual_dep - timedelta(minutes=random.randint(0, 10))
        scheduled_arr = scheduled_dep + timedelta(minutes=flight_duration_minutes)

        # Calculate initial progress
        elapsed = (now - actual_dep).total_seconds() / 3600
        progress = min(elapsed / flight_duration_hours, 1.0)

        flight = {
            "flight_id": self.next_id,
            "airline": random.choice(AIRLINES),
            "departure_city": dep["city"],
            "departure_airport": dep["airport"],
            "arrival_city": arr["city"],
            "arrival_airport": arr["airport"],
            "lat": dep["lat"],
            "lon": dep["lon"],
            "dest_lat": arr["lat"],
            "dest_lon": arr["lon"],
            "current_altitude_m": 0,
            "current_speed_km_h": 0,
            "speed": speed_kmh,
            "direction": calculate_bearing(dep["lat"], dep["lon"], arr["lat"], arr["lon"]),
            "distance_travelled_km": 0,
            "flight_status": "active",
            "scheduled_departure_time": scheduled_dep.isoformat(),
            "actual_departure_time": actual_dep.isoformat(),
            "scheduled_arrival_time": scheduled_arr.isoformat(),
            "actual_landed_time": None,
            "start_time": actual_dep.strftime("%a, %d %b %Y %H:%M:%S GMT"),
            "current_location": {"latitude": dep["lat"], "longitude": dep["lon"]},
            "progress": progress,
            "total_distance_km": distance,
            "flight_duration_hours": flight_duration_hours,
        }
        self.next_id += 1
        self.flights.append(flight)
        return flight

    def update_flights(self):
        """Update all flight positions - called every second"""
        with self.lock:
            now = datetime.now()
            for flight in self.flights:
                if flight["flight_status"] == "landed":
                    continue

                actual_dep = datetime.fromisoformat(flight["actual_departure_time"])
                elapsed_hours = (now - actual_dep).total_seconds() / 3600
                progress = min(elapsed_hours / flight["flight_duration_hours"], 1.0)
                flight["progress"] = progress

                if progress >= 1.0:
                    # Flight has landed
                    flight["flight_status"] = "landed"
                    flight["lat"] = flight["dest_lat"]
                    flight["lon"] = flight["dest_lon"]
                    flight["current_altitude_m"] = 0
                    flight["current_speed_km_h"] = 0
                    # Add some random delay for realism
                    delay_minutes = random.choice([0, 0, 0, 5, 10, 15, 20, 35, 45])
                    scheduled_arr = datetime.fromisoformat(flight["scheduled_arrival_time"])
                    flight["actual_landed_time"] = (scheduled_arr + timedelta(minutes=delay_minutes)).isoformat()
                    flight["current_location"] = {
                        "latitude": flight["dest_lat"],
                        "longitude": flight["dest_lon"]
                    }
                    flight["distance_travelled_km"] = round(flight["total_distance_km"], 1)
                else:
                    # Interpolate position
                    dep_lat, dep_lon = flight["lat"], flight["lon"]
                    # Use original departure coordinates for interpolation
                    orig_dep = AIRPORTS[[a["city"] for a in AIRPORTS].index(flight["departure_city"])]
                    
                    flight["current_location"]["latitude"] = orig_dep["lat"] + (flight["dest_lat"] - orig_dep["lat"]) * progress
                    flight["current_location"]["longitude"] = orig_dep["lon"] + (flight["dest_lon"] - orig_dep["lon"]) * progress
                    flight["lat"] = flight["current_location"]["latitude"]
                    flight["lon"] = flight["current_location"]["longitude"]
                    
                    # Altitude: climb -> cruise -> descend
                    if progress < 0.1:
                        flight["current_altitude_m"] = int(progress / 0.1 * 11000)
                    elif progress > 0.9:
                        flight["current_altitude_m"] = int((1 - progress) / 0.1 * 11000)
                    else:
                        flight["current_altitude_m"] = 11000 + random.randint(-500, 500)
                    
                    flight["current_speed_km_h"] = flight["speed"] + random.randint(-20, 20)
                    flight["distance_travelled_km"] = round(flight["total_distance_km"] * progress, 1)
                    flight["direction"] = calculate_bearing(
                        flight["lat"], flight["lon"],
                        flight["dest_lat"], flight["dest_lon"]
                    )

            # Replace landed flights with new ones
            landed = [f for f in self.flights if f["flight_status"] == "landed"]
            if len(landed) >= 3 or random.random() < 0.1:
                for f in landed[:2]:  # Replace up to 2 at a time
                    self.flights.remove(f)
                    self._create_flight()

    def get_flights(self):
        with self.lock:
            # Return clean data (remove internal fields)
            result = []
            for f in self.flights:
                clean = {k: v for k, v in f.items() 
                        if k not in ("progress", "total_distance_km", "flight_duration_hours")}
                result.append(clean)
            return result


# INITIALIZE

simulator = FlightSimulator(num_flights=15)

def background_updater():
    """Background thread to update flights every second"""
    while True:
        simulator.update_flights()
        time.sleep(1)

# Start background thread
updater_thread = threading.Thread(target=background_updater, daemon=True)
updater_thread.start()

# API ROUTES
@app.route('/api/flights', methods=['GET'])
def get_flights():
    flights = simulator.get_flights()
    return jsonify({
        "flights": flights,
        "count": len(flights),
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/flights/<int:flight_id>', methods=['GET'])
def get_flight(flight_id):
    flights = simulator.get_flights()
    flight = next((f for f in flights if f["flight_id"] == flight_id), None)
    if flight:
        return jsonify(flight)
    return jsonify({"error": "Flight not found"}), 404

@app.route('/', methods=['GET'])
def index():
    return jsonify({
        "message": "Flight Map API",
        "endpoints": {
            "all_flights": "/api/flights",
            "single_flight": "/api/flights/<id>"
        }
    })

# RUN
if __name__ == '__main__':
    print("=" * 50)
    print("  Flight Map API Started")
    print("  Local:  http://localhost:5001/api/flights")
    print("  Docker: http://host.docker.internal:5001/api/flights")
    print("=" * 50)
    app.run(host='0.0.0.0', port=5001, debug=False)