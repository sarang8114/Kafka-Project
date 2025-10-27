from kafka import KafkaProducer, KafkaConsumer
import requests
import json
import time
from datetime import datetime
import threading
from geopy.distance import geodesic

class OSRMRideService:
    """
    Real-time ride simulation using OSRM routing
    Streams taxi position to Kafka step-by-step
    """

    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        self.active_rides = {}
        self.ride_counter = 0

        # Start booking listener
        threading.Thread(target=self.listen_bookings, daemon=True).start()

    def get_route_from_osrm(self, pickup_lon, pickup_lat, dropoff_lon, dropoff_lat):
        """
        Get detailed route from OSRM API
        Returns: route coordinates, distance, duration
        """
        print(f"\n📍 Fetching route from OSRM...")
        print(f"   Pickup: ({pickup_lat}, {pickup_lon})")
        print(f"   Dropoff: ({dropoff_lat}, {dropoff_lon})")

        # OSRM API endpoint (public demo server)
        url = f"http://router.project-osrm.org/route/v1/driving/{pickup_lon},{pickup_lat};{dropoff_lon},{dropoff_lat}"
        params = {
            'overview': 'full',
            'geometries': 'geojson',
            'steps': 'true'
        }

        try:
            response = requests.get(url, params=params, timeout=10)
            data = response.json()

            if data['code'] != 'Ok':
                print(f"❌ OSRM Error: {data.get('message', 'Unknown error')}")
                return None

            route = data['routes'][0]

            # Extract route coordinates (step-by-step points)
            coordinates = route['geometry']['coordinates']  # [[lon, lat], ...]

            # Distance in meters, duration in seconds
            distance_km = route['distance'] / 1000
            duration_sec = route['duration']
            duration_min = duration_sec / 60

            print(f"✅ Route fetched successfully!")
            print(f"   Distance: {distance_km:.2f} km")
            print(f"   Duration: {duration_min:.1f} minutes")
            print(f"   Route points: {len(coordinates)}")

            return {
                'coordinates': coordinates,
                'distance_km': distance_km,
                'distance_miles': distance_km * 0.621371,
                'duration_seconds': duration_sec,
                'duration_minutes': duration_min,
                'total_points': len(coordinates)
            }

        except Exception as e:
            print(f"❌ OSRM API Error: {e}")
            return None

    def calculate_fare(self, distance_miles, duration_minutes):
        """Calculate realistic taxi fare"""
        base_fare = 3.00
        per_mile = 2.50
        per_minute = 0.50

        fare = base_fare + (distance_miles * per_mile) + (duration_minutes * per_minute)
        return round(fare, 2)

    def simulate_ride(self, ride_id, booking_data, route_data):
        """
        Simulate taxi moving along the route
        Send position updates to Kafka in real-time
        """
        print(f"\n🚕 Starting ride simulation: {ride_id}")

        coordinates = route_data['coordinates']
        total_points = len(coordinates)

        # Calculate fare
        fare = self.calculate_fare(
            route_data['distance_miles'],
            route_data['duration_minutes']
        )

        # Send initial ride stats
        stats_message = {
            'ride_id': ride_id,
            'event': 'ride_started',
            'passenger_name': booking_data['passenger_name'],
            'distance_km': route_data['distance_km'],
            'distance_miles': route_data['distance_miles'],
            'estimated_duration_min': route_data['duration_minutes'],
            'estimated_fare': fare,
            'total_route_points': total_points,
            'timestamp': datetime.now().isoformat()
        }

        self.producer.send('ride-stats', value=stats_message)
        print(f"📊 Sent ride stats to Kafka")

        # Calculate delay between points (to match real duration)
        total_duration_sec = route_data['duration_seconds']
        delay_per_point = total_duration_sec / total_points
        delay_per_point = max(0.5, min(delay_per_point, 2))  # Between 0.5-2 seconds

        print(f"⏱️  Delay per point: {delay_per_point:.2f} seconds")
        print(f"🚗 Starting movement along {total_points} points...\n")

        start_time = time.time()

        for i, coord in enumerate(coordinates):
            lon, lat = coord

            # Calculate progress
            progress_percent = (i / total_points) * 100
            elapsed_time = time.time() - start_time
            remaining_points = total_points - i
            estimated_remaining_time = remaining_points * delay_per_point

            # Calculate current speed (rough estimate)
            if i > 0:
                prev_coord = coordinates[i-1]
                dist_to_prev = geodesic(
                    (prev_coord[1], prev_coord[0]),
                    (lat, lon)
                ).kilometers
                speed_kmh = (dist_to_prev / delay_per_point) * 3600 if delay_per_point > 0 else 0
            else:
                speed_kmh = 30  # Initial speed estimate

            # Send position update to Kafka
            position_message = {
                'ride_id': ride_id,
                'event': 'position_update',
                'latitude': lat,
                'longitude': lon,
                'point_index': i,
                'total_points': total_points,
                'progress_percent': round(progress_percent, 2),
                'elapsed_time_sec': round(elapsed_time, 2),
                'remaining_time_sec': round(estimated_remaining_time, 2),
                'current_speed_kmh': round(speed_kmh, 2),
                'timestamp': datetime.now().isoformat()
            }

            self.producer.send('taxi-positions', value=position_message)

            # Print progress every 20% or every 50 points
            if i % max(1, total_points // 5) == 0 or i % 50 == 0:
                print(f"📍 Progress: {progress_percent:.1f}% | Point {i}/{total_points} | "
                      f"Speed: {speed_kmh:.1f} km/h | ETA: {estimated_remaining_time/60:.1f} min")

            time.sleep(delay_per_point)

        # Send completion message
        completion_message = {
            'ride_id': ride_id,
            'event': 'ride_completed',
            'passenger_name': booking_data['passenger_name'],
            'total_distance_km': route_data['distance_km'],
            'total_distance_miles': route_data['distance_miles'],
            'total_duration_sec': time.time() - start_time,
            'total_duration_min': (time.time() - start_time) / 60,
            'final_fare': fare,
            'timestamp': datetime.now().isoformat()
        }

        self.producer.send('ride-stats', value=completion_message)

        print(f"\n✅ Ride completed: {ride_id}")
        print(f"   Distance: {route_data['distance_miles']:.2f} miles")
        print(f"   Duration: {(time.time() - start_time)/60:.1f} minutes")
        print(f"   Fare: ${fare:.2f}\n")

        # Remove from active rides
        if ride_id in self.active_rides:
            del self.active_rides[ride_id]

    def handle_booking(self, booking_data):
        """
        Process a new booking request
        """
        self.ride_counter += 1
        ride_id = f"RIDE_{self.ride_counter:05d}"

        print(f"\n{'='*70}")
        print(f"🆕 NEW BOOKING RECEIVED")
        print(f"{'='*70}")
        print(f"Ride ID: {ride_id}")
        print(f"Passenger: {booking_data.get('passenger_name', 'Unknown')}")
        print(f"Pickup: {booking_data.get('pickup_address', 'N/A')}")
        print(f"Dropoff: {booking_data.get('dropoff_address', 'N/A')}")

        # Get route from OSRM
        route_data = self.get_route_from_osrm(
            booking_data['pickup_lon'],
            booking_data['pickup_lat'],
            booking_data['dropoff_lon'],
            booking_data['dropoff_lat']
        )

        if not route_data:
            print(f"❌ Failed to get route for ride {ride_id}")
            return

        # Store in active rides
        self.active_rides[ride_id] = {
            'booking': booking_data,
            'route': route_data,
            'status': 'active'
        }

        # Start ride simulation in a separate thread
        ride_thread = threading.Thread(
            target=self.simulate_ride,
            args=(ride_id, booking_data, route_data),
            daemon=True
        )
        ride_thread.start()

    def listen_bookings(self):
        """
        Listen for booking requests from Kafka
        """
        print("\n👂 Listening for booking requests on 'ride-bookings' topic...\n")

        consumer = KafkaConsumer(
            'ride-bookings',
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='ride-service-group',
            auto_offset_reset='latest'
        )

        for message in consumer:
            booking_data = message.value
            self.handle_booking(booking_data)

    def run(self):
        """
        Main service loop
        """
        print("\n" + "="*70)
        print("🚖 OSRM TAXI RIDE SERVICE - KAFKA STREAMING")
        print("="*70)
        print("\n🌐 Using OSRM Router: http://router.project-osrm.org")
        print("📡 Kafka Topics:")
        print("   📥 ride-bookings (input)")
        print("   📤 taxi-positions (output - GPS updates)")
        print("   📤 ride-stats (output - distance, ETA, fare)")
        print("\n" + "="*70 + "\n")

        try:
            # Keep main thread alive
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n\n🛑 Shutting down ride service...")
        finally:
            self.producer.close()

if __name__ == "__main__":
    service = OSRMRideService()
    service.run()