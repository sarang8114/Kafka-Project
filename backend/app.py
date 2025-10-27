from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_socketio import SocketIO, emit
from kafka import KafkaProducer, KafkaConsumer
import json
import uuid
from threading import Thread
import time
from datetime import datetime

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Kafka producer for sending messages
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Store active bookings and taxis in memory
active_bookings = {}
active_taxis = {}

# API Routes
@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'NYC Taxi Backend',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/book-taxi', methods=['POST'])
def book_taxi():
    """
    Create new taxi booking
    Expected JSON:
    {
        "pickup_lat": 40.7580,
        "pickup_lon": -73.9855,
        "dropoff_lat": 40.7489,
        "dropoff_lon": -73.9680,
        "pickup_address": "Times Square",
        "dropoff_address": "Grand Central"
    }
    """
    try:
        data = request.json

        # Validate input
        required_fields = ['pickup_lat', 'pickup_lon', 'dropoff_lat', 'dropoff_lon']
        for field in required_fields:
            if field not in data:
                return jsonify({'error': f'Missing required field: {field}'}), 400

        # Create booking
        booking_id = str(uuid.uuid4())[:8]  # Short ID
        booking = {
            'booking_id': booking_id,
            'pickup_lat': float(data['pickup_lat']),
            'pickup_lon': float(data['pickup_lon']),
            'dropoff_lat': float(data['dropoff_lat']),
            'dropoff_lon': float(data['dropoff_lon']),
            'pickup_address': data.get('pickup_address', 'Pickup Location'),
            'dropoff_address': data.get('dropoff_address', 'Dropoff Location'),
            'status': 'pending',
            'timestamp': datetime.now().isoformat(),
            'created_at': time.time()
        }

        # Store in memory
        active_bookings[booking_id] = booking

        # Publish to Kafka for taxi assignment
        producer.send('booking-requests', value=booking)
        producer.flush()

        print(f"✅ Booking created: {booking_id}")

        return jsonify({
            'success': True,
            'booking_id': booking_id,
            'message': 'Booking created! Finding nearest taxi...',
            'booking': booking
        }), 201

    except Exception as e:
        print(f"❌ Booking error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/booking/<booking_id>', methods=['GET'])
def get_booking(booking_id):
    """Get booking details by ID"""
    if booking_id in active_bookings:
        return jsonify({
            'success': True,
            'booking': active_bookings[booking_id]
        })
    return jsonify({'error': 'Booking not found'}), 404

@app.route('/api/active-taxis', methods=['GET'])
def get_active_taxis():
    """Get all active taxis"""
    return jsonify({
        'success': True,
        'count': len(active_taxis),
        'taxis': list(active_taxis.values())
    })

@app.route('/api/bookings', methods=['GET'])
def get_all_bookings():
    """Get all bookings"""
    return jsonify({
        'success': True,
        'count': len(active_bookings),
        'bookings': list(active_bookings.values())
    })

# WebSocket Events
@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    print('✅ Client connected via WebSocket')
    emit('connection_response', {
        'status': 'connected',
        'message': 'Connected to NYC Taxi Tracking Server',
        'timestamp': datetime.now().isoformat()
    })

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    print('❌ Client disconnected')

@socketio.on('request_taxi_updates')
def handle_taxi_update_request():
    """Client requests taxi location updates"""
    emit('taxi_update_enabled', {'status': 'enabled'})

# Kafka Consumer Threads
def consume_taxi_locations():
    """
    Background thread: Consume processed taxi locations from Kafka
    and broadcast to connected WebSocket clients
    """
    print("📡 Starting taxi location consumer...")

    consumer = KafkaConsumer(
        'processed-taxi-locations',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='flask-location-consumer',
        auto_offset_reset='latest'
    )

    for message in consumer:
        try:
            taxi_data = message.value
            taxi_id = taxi_data.get('taxi_id')

            # Update in-memory storage
            active_taxis[taxi_id] = taxi_data

            # Broadcast to all connected clients via WebSocket
            socketio.emit('taxi_location_update', taxi_data, namespace='/')

        except Exception as e:
            print(f"❌ Error processing taxi location: {e}")

def consume_trip_updates():
    """
    Background thread: Consume trip updates (assignments, pickups, completions)
    and broadcast to connected WebSocket clients
    """
    print("📡 Starting trip update consumer...")

    consumer = KafkaConsumer(
        'trip-updates',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='flask-trip-consumer',
        auto_offset_reset='latest'
    )

    for message in consumer:
        try:
            trip_data = message.value
            booking_id = trip_data.get('booking_id')

            # Update booking status
            if booking_id and booking_id in active_bookings:
                active_bookings[booking_id]['status'] = trip_data.get('status')
                active_bookings[booking_id]['taxi_id'] = trip_data.get('taxi_id')
                active_bookings[booking_id].update(trip_data)

            # Broadcast to clients
            socketio.emit('trip_update', trip_data, namespace='/')

            print(f"📢 Trip Update: {trip_data.get('status')} - {booking_id}")

        except Exception as e:
            print(f"❌ Error processing trip update: {e}")

def cleanup_old_data():
    """Background thread: Clean up old bookings and inactive taxis"""
    while True:
        try:
            current_time = time.time()

            # Remove completed bookings older than 1 hour
            old_bookings = [
                bid for bid, booking in active_bookings.items()
                if current_time - booking.get('created_at', current_time) > 3600
                and booking.get('status') == 'completed'
            ]

            for bid in old_bookings:
                del active_bookings[bid]

            if old_bookings:
                print(f"🧹 Cleaned up {len(old_bookings)} old bookings")

        except Exception as e:
            print(f"❌ Cleanup error: {e}")

        time.sleep(300)  # Run every 5 minutes

# Start background threads
def start_background_threads():
    """Start all background Kafka consumer threads"""
    Thread(target=consume_taxi_locations, daemon=True).start()
    Thread(target=consume_trip_updates, daemon=True).start()
    Thread(target=cleanup_old_data, daemon=True).start()
    print("✅ Background threads started")

if __name__ == '__main__':
    print("\n" + "="*60)
    print("🚀 NYC TAXI TRACKING BACKEND")
    print("="*60)
    print("📍 API Server: http://localhost:5000")
    print("🔌 WebSocket: ws://localhost:5000")
    print("📡 Kafka Topics: taxi-locations, trip-updates, booking-requests")
    print("="*60 + "\n")

    # Start background threads
    start_background_threads()

    # Start Flask + SocketIO server
    socketio.run(
        app,
        debug=True,
        host='0.0.0.0',
        port=5000,
        use_reloader=False  # Disable reloader to prevent double thread creation
    )