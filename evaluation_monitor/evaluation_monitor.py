#!/usr/bin/env python3
"""
Online evaluation monitor: Kafka consumer + Flask app for Prometheus metrics
Consumes rating events from Kafka and calculates MAE/MSE metrics
Supports automatic model reloading when inference_2 model is updated
"""

from flask import Flask
from kafka import KafkaConsumer
import pickle
import numpy as np
import os
import re
import threading
import time
from datetime import datetime
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

# Flask app
app = Flask(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'movielog1')
INFERENCE_INSTANCE = os.environ.get('INFERENCE_INSTANCE', 'inference_1')
MODEL_PATH = f'/model_files/{INFERENCE_INSTANCE}/als_model.pkl'
MAPPINGS_PATH = f'/model_files/{INFERENCE_INSTANCE}/als_mappings.pkl'
MODEL_RELOAD_CHECK_INTERVAL = int(os.getenv('MODEL_RELOAD_CHECK_INTERVAL', '30'))

# Convert inference instance to label format (inference_1 -> inference-1)
INFERENCE_INSTANCE_LABEL = INFERENCE_INSTANCE.replace('_', '-')

# Prometheus metrics with instance label
mae_gauge = Gauge('model_mae', 'Mean Absolute Error of model predictions', 
                   ['inference_instance'])
mse_gauge = Gauge('model_mse', 'Mean Squared Error of model predictions',
                   ['inference_instance'])
total_predictions = Counter('model_total_predictions', 'Total number of predictions evaluated',
                            ['inference_instance'])
prediction_errors = Histogram('model_prediction_error', 'Prediction error distribution',
                               ['inference_instance'],
                               buckets=[0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0])
model_reloads = Counter('model_reloads_total', 'Total number of model reloads',
                         ['inference_instance'])
model_load_time = Gauge('model_load_timestamp', 'Unix timestamp of last model load',
                        ['inference_instance'])

# Global state for metrics calculation
error_sum = 0.0
squared_error_sum = 0.0
prediction_count = 0
metrics_lock = threading.Lock()

# Model and mappings
model = None
mappings = None
user_to_idx = None
item_to_idx = None
idx_to_item = None
time_period_user_factors = None

# Model file tracking for reload detection
model_file_mtime = None
mappings_file_mtime = None


def get_time_period_index():
    """Get the time period index (0-11) from the current time."""
    hour = datetime.now().hour
    return hour // 2


def load_model_and_mappings():
    """Load the trained model and mappings from pickle files."""
    global model, mappings, user_to_idx, item_to_idx, idx_to_item, time_period_user_factors
    global model_file_mtime, mappings_file_mtime
    
    try:
        # Check file modification times
        if os.path.exists(MODEL_PATH):
            new_model_mtime = os.path.getmtime(MODEL_PATH)
        else:
            print(f"Model file not found: {MODEL_PATH}")
            return False
            
        if os.path.exists(MAPPINGS_PATH):
            new_mappings_mtime = os.path.getmtime(MAPPINGS_PATH)
        else:
            print(f"Mappings file not found: {MAPPINGS_PATH}")
            return False
        
        # If model is already loaded and files haven't changed, skip reload
        if (model is not None and 
            model_file_mtime == new_model_mtime and 
            mappings_file_mtime == new_mappings_mtime):
            return True
        
        print(f"[{datetime.now()}] Loading model from {MODEL_PATH}...")
        start_time = time.time()
        
        # Load model
        with open(MODEL_PATH, 'rb') as f:
            model = pickle.load(f)
        
        # Load mappings
        with open(MAPPINGS_PATH, 'rb') as f:
            mappings = pickle.load(f)
        
        # Extract dictionaries
        user_to_idx = mappings.get('user_to_idx', {})
        item_to_idx = mappings.get('item_to_idx', {})
        idx_to_item = mappings.get('idx_to_item', {})
        time_period_user_factors = mappings.get('time_period_user_factors')
        
        if time_period_user_factors is None:
            raise RuntimeError('Time period user factors not found in mappings')
        
        if time_period_user_factors.shape[0] != 12:
            raise RuntimeError(f'Expected 12 time periods, but found {time_period_user_factors.shape[0]}')
        
        # Update file modification times
        model_file_mtime = new_model_mtime
        mappings_file_mtime = new_mappings_mtime
        
        load_time = time.time() - start_time
        print(f"[{datetime.now()}] Model loaded successfully in {load_time:.2f}s")
        print(f"  Model factors: {model.user_factors.shape[0]} users, {model.item_factors.shape[0]} items")
        print(f"  Time period user factors: {time_period_user_factors.shape}")
        
        # Update Prometheus metrics with instance label
        model_reloads.labels(inference_instance=INFERENCE_INSTANCE_LABEL).inc()
        model_load_time.labels(inference_instance=INFERENCE_INSTANCE_LABEL).set(time.time())
        
        return True
    except FileNotFoundError as e:
        print(f"File not found: {e}")
        return False
    except Exception as e:
        print(f"Error loading model or mappings: {e}")
        import traceback
        traceback.print_exc()
        return False


def check_and_reload_model():
    """Periodically check if model files have changed and reload if needed."""
    while True:
        try:
            time.sleep(MODEL_RELOAD_CHECK_INTERVAL)
            
            # Only reload if monitoring inference_2 (which gets updated by cronjob)
            if INFERENCE_INSTANCE == 'inference_2':
                if load_model_and_mappings():
                    print(f"[{datetime.now()}] Model reload check completed")
                else:
                    print(f"[{datetime.now()}] Model reload check failed (will retry)")
        except Exception as e:
            print(f"Error in model reload check: {e}")
            import traceback
            traceback.print_exc()


def get_prediction(userid, movieid):
    """
    Get prediction score for a specific user-movie pair.
    
    Args:
        userid: User ID (int)
        movieid: Movie ID (string)
    
    Returns:
        float: Prediction score, or None if prediction cannot be computed
    """
    if model is None or item_to_idx is None:
        return None
    
    # Check if movie is in the model
    if movieid not in item_to_idx:
        return None
    
    # Get item factor
    item_idx = item_to_idx[movieid]
    item_factor = model.item_factors[item_idx]
    
    # Get user factor
    is_seen_user = userid in user_to_idx
    if is_seen_user:
        user_idx = user_to_idx[userid]
        user_factor = model.user_factors[user_idx]
    else:
        # Unseen user - use time-period-specific user factor
        if time_period_user_factors is None:
            return None
        period_index = get_time_period_index()
        user_factor = time_period_user_factors[period_index]
    
    # Compute prediction: dot product of user and item factors
    try:
        prediction = np.dot(item_factor, user_factor)
        return float(prediction)
    except Exception as e:
        print(f"Error computing prediction: {e}")
        return None


def parse_rating(timestamp, userid, action):
    """Parse rating event: GET /rate/<movieid>=<rating>"""
    try:
        match = re.search(r'/rate/([^=]+)=(\d+)', action)
        if match:
            movieid, rating = match.groups()
            return {
                'timestamp': timestamp,
                'userid': int(userid),
                'movieid': movieid,
                'rating': int(rating)
            }
    except Exception as e:
        print(f"Error parsing rating: {e}")
    return None


def parse_log_line(line):
    """Parse a log line and extract rating data"""
    try:
        parts = line.split(',', 2)
        if len(parts) < 3:
            return None
        
        timestamp, userid, action = parts
        
        # Only parse rating events
        if 'GET /rate/' in action:
            return parse_rating(timestamp, userid, action)
        
        return None
    except Exception as e:
        print(f"Error parsing line: {line[:100]}... Error: {e}")
        return None


def update_metrics(prediction, actual_rating):
    """
    Update MAE and MSE metrics with a new prediction.
    
    Args:
        prediction: Predicted rating (float)
        actual_rating: Actual rating (int)
    """
    global error_sum, squared_error_sum, prediction_count
    
    if prediction is None:
        return
    
    error = abs(prediction - actual_rating)
    squared_error = (prediction - actual_rating) ** 2
    
    with metrics_lock:
        error_sum += error
        squared_error_sum += squared_error
        prediction_count += 1
        
        # Update Prometheus metrics with instance label
        if prediction_count > 0:
            mae = error_sum / prediction_count
            mse = squared_error_sum / prediction_count
            
            mae_gauge.labels(inference_instance=INFERENCE_INSTANCE_LABEL).set(mae)
            mse_gauge.labels(inference_instance=INFERENCE_INSTANCE_LABEL).set(mse)
            total_predictions.labels(inference_instance=INFERENCE_INSTANCE_LABEL).inc()
            prediction_errors.labels(inference_instance=INFERENCE_INSTANCE_LABEL).observe(error)
        
        # Log periodically (every 100 predictions)
        if prediction_count % 100 == 0:
            print(f"[METRICS] Instance: {INFERENCE_INSTANCE_LABEL}, Prediction: {prediction:.2f}, Actual: {actual_rating}, Error: {error:.2f}, MAE: {mae:.4f}, MSE: {mse:.4f}, Count: {prediction_count}")


def consume_kafka_messages():
    """Consume messages from Kafka and update metrics"""
    bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS.split(',')
    topic = KAFKA_TOPIC
    
    print(f"Connecting to Kafka at {bootstrap_servers[0]}...")
    print(f"Consuming from topic: {topic}")
    print(f"Monitoring instance: {INFERENCE_INSTANCE_LABEL}")
    print("Processing rating events for online evaluation...")
    
    consumer = None
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='latest',  # Start from latest
            enable_auto_commit=True,
            group_id=f'evaluation-monitor-{INFERENCE_INSTANCE_LABEL}',  # Separate consumer groups
            value_deserializer=lambda x: x.decode('utf-8') if x else None
        )
        
        for message in consumer:
            # Parse the log line
            parsed_data = parse_log_line(message.value)
            if parsed_data:
                userid = parsed_data['userid']
                movieid = parsed_data['movieid']
                actual_rating = parsed_data['rating']
                
                # Get prediction from model (model may be reloaded in background)
                prediction = get_prediction(userid, movieid)
                
                # Update metrics
                update_metrics(prediction, actual_rating)
                
    except KeyboardInterrupt:
        print("\nStopping Kafka consumer...")
        if consumer:
            consumer.close()
    except Exception as e:
        print(f"Error in Kafka consumer: {e}")
        import traceback
        traceback.print_exc()
        if consumer:
            consumer.close()


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    if model is None or mappings is None:
        return {'status': 'error', 'message': 'Model or mappings not loaded'}, 500
    
    model_mtime = None
    if os.path.exists(MODEL_PATH):
        model_mtime = datetime.fromtimestamp(os.path.getmtime(MODEL_PATH)).isoformat()
    
    return {
        'status': 'healthy',
        'model_loaded': model is not None,
        'mappings_loaded': mappings is not None,
        'total_predictions': prediction_count,
        'model_file_mtime': model_mtime,
        'inference_instance': INFERENCE_INSTANCE,
        'inference_instance_label': INFERENCE_INSTANCE_LABEL
    }


@app.route('/metrics', methods=['GET'])
def metrics():
    """Prometheus metrics endpoint"""
    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}


def start_kafka_consumer():
    """Start Kafka consumer in a separate thread"""
    # Wait a bit for model to load
    time.sleep(2)
    if model is None:
        print("Warning: Model not loaded, cannot start Kafka consumer")
        return
    consume_kafka_messages()


if __name__ == '__main__':
    print(f"Starting evaluation monitor for {INFERENCE_INSTANCE_LABEL}")
    
    # Load model and mappings at startup
    if not load_model_and_mappings():
        print("Failed to load model. Exiting.")
        exit(1)
    
    # Start model reload checker thread (only for inference_2)
    if INFERENCE_INSTANCE == 'inference_2':
        reload_thread = threading.Thread(target=check_and_reload_model, daemon=True)
        reload_thread.start()
        print(f"Model reload checker started (checks every {MODEL_RELOAD_CHECK_INTERVAL}s)")
    else:
        print(f"Model reload checker disabled (monitoring {INFERENCE_INSTANCE_LABEL} - stable model)")
    
    # Start Kafka consumer in a separate thread
    kafka_thread = threading.Thread(target=start_kafka_consumer, daemon=True)
    kafka_thread.start()
    print("Kafka consumer thread started")
    
    # Start Flask app
    host = os.environ.get('HOST', '0.0.0.0')
    port = int(os.environ.get('PORT', '8084'))
    print(f"Flask app starting on port {port}")
    print(f"Metrics endpoint: http://{host}:{port}/metrics")
    print(f"Health endpoint: http://{host}:{port}/health")
    app.run(host=host, port=port, debug=False, threaded=True)

