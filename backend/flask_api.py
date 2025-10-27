from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_socketio import SocketIO, emit
from kafka import KafkaProducer, KafkaConsumer
import json
from threading import Thread
from datetime import datetime

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ============================================
# REST API ENDPOINTS
# ============================================

@app.route('/api/health', methods=['GET'])
def health():
    """Health check"""
    return jsonify({
        'status': 'healthy',
        'service': 'OSRM Taxi API',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/book-ride', methods=['POST'])
def book_ride():
    """
    Book a new ride
    Expected JSON: {
        passenger_name,
        pickup_lat, pickup_lon, pickup_address,
        dropoff_lat, dropoff_lon, dropoff_address
    }
    """
    try:
        data = request.json

        # Validate required fields
        required = ['passenger_name', 'pickup_lat', 'pickup_lon',
                   'dropoff_lat', 'dropoff_lon']

        for field in required:
            if field not in data:
                return jsonify({'error': f'Missing field: {field}'}), 400

        # Create booking message
        booking = {
            'passenger_name': data['passenger_name'],
            'pickup_lat': float(data['pickup_lat']),
            'pickup_lon': float(data['pickup_lon']),
            'pickup_address': data.get('pickup_address', 'Pickup Location'),
            'dropoff_lat': float(data['dropoff_lat']),
            'dropoff_lon': float(data['dropoff_lon']),
            'dropoff_address': data.get('dropoff_address', 'Dropoff Location'),
            'booking_time': datetime.now().isoformat()
        }

        # Send to Kafka
        producer.send('ride-bookings', value=booking)
        producer.flush()

        print(f"✅ Booking received for {booking['passenger_name']}")

        return jsonify({
            'success': True,
            'message': 'Ride booked successfully! Fetching route...',
            'booking': booking
        }), 201

    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================
# WEBSOCKET EVENTS
# ============================================

@socketio.on('connect')
def handle_connect():
    """Client connected"""
    print('✅ WebSocket client connected')
    emit('connected', {'status': 'connected'})

@socketio.on('disconnect')
def handle_disconnect():
    """Client disconnected"""
    print('❌ WebSocket client disconnected')

# ============================================
# KAFKA CONSUMERS (Background Threads)
# ============================================

def consume_taxi_positions():
    """
    Consume real-time taxi position updates
    Forward to connected WebSocket clients
    """
    print("📡 Starting taxi position consumer...")

    consumer = KafkaConsumer(
        'taxi-positions',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='flask-position-consumer',
        auto_offset_reset='latest'
    )

    for message in consumer:
        position_data = message.value

        # Forward to all connected WebSocket clients
        socketio.emit('taxi_position', position_data, namespace='/')

        # Print progress occasionally
        if position_data.get('point_index', 0) % 50 == 0:
            print(f"📍 Position update: {position_data.get('progress_percent', 0):.1f}% | "
                  f"Speed: {position_data.get('current_speed_kmh', 0):.1f} km/h")

def consume_ride_stats():
    """
    Consume ride statistics (distance, ETA, fare)
    Forward to connected WebSocket clients
    """
    print("📡 Starting ride stats consumer...")

    consumer = KafkaConsumer(
        'ride-stats',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='flask-stats-consumer',
        auto_offset_reset='latest'
    )

    for message in consumer:
        stats_data = message.value

        # Forward to WebSocket clients
        socketio.emit('ride_stats', stats_data, namespace='/')

        event = stats_data.get('event', '')
        if event == 'ride_started':
            print(f"🚖 Ride started: {stats_data.get('ride_id')}")
            print(f"   Distance: {stats_data.get('distance_miles', 0):.2f} miles")
            print(f"   ETA: {stats_data.get('estimated_duration_min', 0):.1f} minutes")
            print(f"   Fare: ${stats_data.get('estimated_fare', 0):.2f}")
        elif event == 'ride_completed':
            print(f"✅ Ride completed: {stats_data.get('ride_id')}")

# ============================================
# STARTUP
# ============================================

def start_consumers():
    """Start all Kafka consumer threads"""
    Thread(target=consume_taxi_positions, daemon=True).start()
    Thread(target=consume_ride_stats, daemon=True).start()
    print("✅ All Kafka consumers started\n")

if __name__ == '__main__':
    print("\n" + "="*70)
    print("🚀 OSRM TAXI API SERVER")
    print("="*70)
    print("\n📍 REST API: http://localhost:5000")
    print("🔌 WebSocket: ws://localhost:5000")
    print("\n📡 Kafka Topics:")
    print("   📤 ride-bookings (send bookings)")
    print("   📥 taxi-positions (receive GPS updates)")
    print("   📥 ride-stats (receive distance, ETA, fare)")
    print("\n" + "="*70 + "\n")

    # Start Kafka consumers
    start_consumers()

    # Start Flask server
    socketio.run(
        app,
        debug=False,
        host='0.0.0.0',
        port=5000,
        use_reloader=False
    )