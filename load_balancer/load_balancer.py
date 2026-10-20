from flask import Flask, request, Response, jsonify
import requests
import logging
import os
import random
import re
import socket
import pyodbc
import queue
import threading
import time
from datetime import datetime
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

try:
    from kafka import KafkaConsumer
except ImportError:
    KafkaConsumer = None

# Detect if running in Kubernetes
# KUBERNETES_SERVICE_HOST is automatically set by Kubernetes in all pods
IS_KUBERNETES = os.environ.get('KUBERNETES_SERVICE_HOST') is not None

# Only load .env file if running locally (not in Kubernetes)
if not IS_KUBERNETES:
    try:
        from dotenv import load_dotenv
        # Get the directory where this script is located
        script_dir = os.path.dirname(os.path.abspath(__file__))
        env_path = os.path.join(script_dir, '.env')
        
        # DEBUG: Check if file exists and read content
        import os as os_module
        if os_module.path.exists(env_path):
            print(f"DEBUG: .env file exists at: {env_path}")
            try:
                with open(env_path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    print(f"DEBUG: .env file has {len(lines)} lines")
                    for i, line in enumerate(lines, 1):
                        # Show first 6 lines with repr to see hidden chars
                        if i <= 6:
                            print(f"DEBUG: Line {i}: {repr(line)}")
            except Exception as e:
                print(f"DEBUG: Error reading .env file: {e}")
        else:
            print(f"DEBUG: .env file NOT found at: {env_path}")
        
        # Fix BOM issue: read file, strip BOM, write back
        try:
            with open(env_path, 'r', encoding='utf-8-sig') as f:
                content = f.read()
            # Write back without BOM
            with open(env_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print("DEBUG: Removed BOM from .env file")
        except Exception as e:
            print(f"DEBUG: Error fixing BOM: {e}")
        
        result = load_dotenv(dotenv_path=env_path)
        print(f"DEBUG: load_dotenv returned: {result}")
        print(f"DEBUG: AZURE_SERVER after load_dotenv: {os.environ.get('AZURE_SERVER', 'NOT SET')}")
        print(f"DEBUG: AZURE_DATABASE after load_dotenv: {os.environ.get('AZURE_DATABASE', 'NOT SET')}")
        print(f"DEBUG: AZURE_USERNAME after load_dotenv: {os.environ.get('AZURE_USERNAME', 'NOT SET')}")
        
    except ImportError:
        # python-dotenv not installed, but that's OK if env vars are set another way
        pass

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Get service names from environment variables
# For Kubernetes: inference-1-service, inference-2-service
# For Docker Compose: inference-1, inference-2 (defaults)
INFERENCE_1_SERVICE = os.environ.get('INFERENCE_1_SERVICE', 'inference-1')
INFERENCE_2_SERVICE = os.environ.get('INFERENCE_2_SERVICE', 'inference-2')

# Build URLs for inference services
INFERENCE_1 = f"http://{INFERENCE_1_SERVICE}:8082"
INFERENCE_2 = f"http://{INFERENCE_2_SERVICE}:8082"

# Health check URLs (separate port 8083 to avoid queue delays)
INFERENCE_1_HEALTH = f"http://{INFERENCE_1_SERVICE}:8083"
INFERENCE_2_HEALTH = f"http://{INFERENCE_2_SERVICE}:8083"

# Weighted routing configuration
# Get weights from environment variables (default: 80% inference-1, 20% inference-2 for canary)
INFERENCE_1_WEIGHT = float(os.environ.get('INFERENCE_1_WEIGHT', '80'))
INFERENCE_2_WEIGHT = float(os.environ.get('INFERENCE_2_WEIGHT', '20'))

# Normalize weights to sum to 100
total_weight = INFERENCE_1_WEIGHT + INFERENCE_2_WEIGHT
INFERENCE_1_PROBABILITY = INFERENCE_1_WEIGHT / total_weight
INFERENCE_2_PROBABILITY = INFERENCE_2_WEIGHT / total_weight

# Database logging configuration
ENABLE_DB_LOGGING = os.environ.get('ENABLE_DB_LOGGING', 'false').lower() == 'true'
DB_SERVER = os.environ.get('AZURE_SERVER')
DB_DATABASE = os.environ.get('AZURE_DATABASE')
DB_USERNAME = os.environ.get('AZURE_USERNAME')
DB_PASSWORD = os.environ.get('AZURE_PASSWORD')

# Queue for async database logging (non-blocking)
db_log_queue = queue.Queue()
db_log_thread = None

logger.info(f"Load balancer configured with weighted routing: "
            f"inference-1={INFERENCE_1_WEIGHT}% ({INFERENCE_1_PROBABILITY:.2%}), "
            f"inference-2={INFERENCE_2_WEIGHT}% ({INFERENCE_2_PROBABILITY:.2%})")

if ENABLE_DB_LOGGING:
    logger.info("Database logging enabled")
else:
    logger.info("Database logging disabled")

# Online hit@3. Watch lines are per-minute playback
# (GET /data/m/<movieid>/<minute>.mpg). Someone who acts on a list usually
# starts within a few minutes; 30 minutes still drops a later unrelated watch
# of the same title. A hit is counted when the watch arrives, so PromQL ranges
# should be longer than the window:
#   sum by (backend) (rate(online_hits_at_3_total[1h]))
#     / sum by (backend) (rate(online_recs_served_total[1h]))
ONLINE_HIT_ENABLED = os.environ.get('ONLINE_HIT_ENABLED', 'true').lower() == 'true'
ONLINE_HIT_WINDOW_SECONDS = max(1, int(os.environ.get('ONLINE_HIT_WINDOW_SECONDS', '1800')))
ONLINE_HIT_MAX_PENDING = max(1, int(os.environ.get('ONLINE_HIT_MAX_PENDING', '300000')))
ONLINE_HIT_K = 3
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'movielog1')
# Unique per process so this group does not steal partitions from
# evaluation-monitor-* or the ratings consumer, and so another balancer
# replica (pending recs live only in this process) still sees every watch.
_online_hit_host = socket.gethostname().strip().replace(' ', '-') or 'local'
ONLINE_HIT_GROUP_ID = os.environ.get(
    'ONLINE_HIT_GROUP_ID', f'load-balancer-online-hit-{_online_hit_host}')

if ONLINE_HIT_ENABLED:
    logger.info(
        f"Online hit@3 enabled: window={ONLINE_HIT_WINDOW_SECONDS}s, "
        f"max_pending={ONLINE_HIT_MAX_PENDING}, topic={KAFKA_TOPIC}, "
        f"group={ONLINE_HIT_GROUP_ID}")
else:
    logger.info("Online hit@3 disabled")

def get_db_connection():
    """
    Create and return a database connection to Azure SQL Database.
    Tries multiple ODBC driver names to handle different system configurations.
    
    Returns:
        pyodbc.Connection or None if connection fails
    """
    if not all([DB_SERVER, DB_DATABASE, DB_USERNAME, DB_PASSWORD]):
        logger.warning("Database credentials not fully configured")
        return None
    
    # Try multiple ODBC driver names (different versions/names on different systems)
    driver_names = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server"
    ]
    
    for driver_name in driver_names:
        try:
            connection_string = (
                f"DRIVER={{{driver_name}}};"
                f"SERVER={DB_SERVER};"
                f"DATABASE={DB_DATABASE};"
                f"UID={DB_USERNAME};"
                f"PWD={DB_PASSWORD};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
                f"Connection Timeout=30;"
            )
            conn = pyodbc.connect(connection_string)
            return conn
        except pyodbc.Error as e:
            # If it's a driver not found error, try next driver
            error_str = str(e)
            if 'IM002' in error_str or 'driver' in error_str.lower() or 'data source name not found' in error_str.lower():
                continue
            # Other errors (auth, network, etc.) - log and return None
            logger.error(f"Failed to connect to database with driver '{driver_name}': {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error connecting with driver '{driver_name}': {e}")
            return None
    
    # If we get here, none of the drivers worked
    logger.error("Failed to connect: No suitable ODBC driver found. Please install 'ODBC Driver 18 for SQL Server'")
    return None

def log_to_database_worker():
    """
    Background worker thread that processes database logging queue.
    Runs continuously and logs routing decisions to the database.
    """
    while True:
        try:
            # Get log entry from queue (blocks until available)
            log_entry = db_log_queue.get()
            
            # Check for shutdown signal
            if log_entry is None:
                break
            
            # Extract log data
            user_id = log_entry['user_id']
            inference_instance = log_entry['inference_instance']
            response_status = log_entry['response_status']
            response_text = log_entry.get('response_text', '')
            
            # Truncate response_text if too long (max 200 chars)
            if response_text and len(response_text) > 200:
                response_text = response_text[:200]
            
            # Connect to database
            conn = get_db_connection()
            if conn is None:
                logger.warning(f"Failed to log routing decision for user {user_id} (DB connection failed)")
                db_log_queue.task_done()
                continue
            
            try:
                cursor = conn.cursor()
                # Insert routing decision
                cursor.execute("""
                    INSERT INTO routing_decisions 
                    (user_id, inference_instance, response_status, response_text)
                    VALUES (?, ?, ?, ?)
                """, user_id, inference_instance, response_status, response_text)
                conn.commit()
                logger.debug(f"Logged routing decision: user {user_id} -> {inference_instance}")
            except Exception as e:
                logger.error(f"Failed to insert routing decision: {e}")
                conn.rollback()
            finally:
                conn.close()
            
            # Mark task as done
            db_log_queue.task_done()
            
        except Exception as e:
            logger.error(f"Error in database logging worker: {e}")

def log_routing_decision(user_id, inference_instance, response_status, response_text=None):
    """
    Queue a routing decision for async database logging.
    Non-blocking - returns immediately.
    
    Args:
        user_id: User ID
        inference_instance: 'inference-1' or 'inference-2'
        response_status: HTTP response status code
        response_text: Response text (comma-separated movie IDs)
    """
    if not ENABLE_DB_LOGGING:
        return
    
    try:
        log_entry = {
            'user_id': user_id,
            'inference_instance': inference_instance,
            'response_status': response_status,
            'response_text': response_text or ''
        }
        db_log_queue.put(log_entry)
    except Exception as e:
        logger.error(f"Failed to queue routing decision for logging: {e}")

def select_backend():
    """
    Select backend using weighted random selection.
    
    Returns:
        tuple: (backend_url, target_name) where backend_url is the full URL
               and target_name is "inference-1" or "inference-2"
    """
    rand = random.random()
    if rand < INFERENCE_1_PROBABILITY:
        return INFERENCE_1, "inference-1"
    else:
        return INFERENCE_2, "inference-2"

def alternate_backend(target):
    """Return the backend that was not selected."""
    if target == "inference-1":
        return INFERENCE_2, "inference-2"
    return INFERENCE_1, "inference-1"

def forward_recommend(backend_url, target, userid):
    """Forward one /recommend request and record latency and status for this backend."""
    url = f"{backend_url}/recommend/{userid}"
    started = time.perf_counter()
    try:
        # 150s is an upper bound; lb_request_latency_seconds records the actual latency.
        response = requests.get(url, params=request.args, timeout=150)
    except requests.exceptions.RequestException:
        lb_requests_total.labels(backend=target, status="502").inc()
        raise
    finally:
        lb_request_latency_seconds.labels(backend=target).observe(
            time.perf_counter() - started)
    lb_requests_total.labels(backend=target, status=str(response.status_code)).inc()
    return response

def check_inference_health(health_url, instance_name):
    """
    Check health of an inference instance using dedicated health check port.
    
    Args:
        health_url: URL to health check endpoint (port 8083)
        instance_name: Name of the instance for logging
    
    Returns:
        dict: Health status with 'healthy' (bool) and 'details' (dict)
    """
    try:
        response = requests.get(f"{health_url}/health", timeout=5)
        if response.status_code == 200:
            health_data = response.json()
            logger.debug(f"Health check for {instance_name}: healthy")
            return {
                'healthy': True,
                'details': health_data
            }
        else:
            logger.warning(f"Health check for {instance_name}: unhealthy (status {response.status_code})")
            return {
                'healthy': False,
                'details': {'error': f'Status code: {response.status_code}'}
            }
    except requests.exceptions.Timeout:
        logger.warning(f"Health check timeout for {instance_name}")
        return {
            'healthy': False,
            'details': {'error': 'Timeout'}
        }
    except Exception as e:
        logger.warning(f"Health check failed for {instance_name}: {e}")
        return {
            'healthy': False,
            'details': {'error': str(e)}
        }

# Real latency is not known ahead of time, so buckets span milliseconds through the 150s timeout.
lb_request_latency_seconds = Histogram(
    'lb_request_latency_seconds',
    'Seconds to forward a /recommend request to an inference backend',
    ['backend'],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 90, 120, 150])
lb_requests_total = Counter(
    'lb_requests_total',
    'Load-balanced /recommend requests by backend and HTTP status',
    ['backend', 'status'])
online_recs_served_total = Counter(
    'online_recs_served_total',
    'Top-3 recommendation lists recorded for online hit@3, by backend',
    ['backend'])
online_hits_at_3_total = Counter(
    'online_hits_at_3_total',
    'Served top-3 lists whose user watched one of those movies within the window',
    ['backend'])
online_recs_expired_total = Counter(
    'online_recs_expired_total',
    'Served top-3 lists that aged out or were evicted with no matching watch',
    ['backend'])

_WATCH_MOVIE_RE = re.compile(r'/data/m/([^/\s]+)/(\d+)\.mpg')
_online_hit_lock = threading.Lock()
# Insertion order is expiry order: every entry uses the same window.
_online_hit_entries = {}
_online_hit_by_user = {}
_online_hit_next_id = 0


def _top_movie_ids(response_text, k=ONLINE_HIT_K):
    """First k ids from the inference body, which is plain text id1,id2,id3."""
    if not response_text:
        return frozenset()
    text = response_text.strip()
    if not text or text[0] in '{[':
        return frozenset()
    movies = []
    for part in text.split(','):
        movie_id = part.strip()
        if movie_id:
            movies.append(movie_id)
        if len(movies) >= k:
            break
    return frozenset(movies)


def _drop_online_hit_entry_locked(entry_id):
    entry = _online_hit_entries.pop(entry_id, None)
    if entry is None:
        return None
    per_user = _online_hit_by_user.get(entry['user_id'])
    if per_user is not None:
        per_user.pop(entry_id, None)
        if not per_user:
            del _online_hit_by_user[entry['user_id']]
    return entry


def _expire_online_hits_locked(now):
    while _online_hit_entries:
        entry_id = next(iter(_online_hit_entries))
        entry = _online_hit_entries[entry_id]
        if entry['expires_at'] > now:
            break
        _drop_online_hit_entry_locked(entry_id)
        online_recs_expired_total.labels(backend=entry['backend']).inc()


def _evict_online_hits_to_cap_locked():
    evicted = 0
    while len(_online_hit_entries) >= ONLINE_HIT_MAX_PENDING and _online_hit_entries:
        entry_id = next(iter(_online_hit_entries))
        entry = _drop_online_hit_entry_locked(entry_id)
        if entry is not None:
            online_recs_expired_total.labels(backend=entry['backend']).inc()
            evicted += 1
    if evicted:
        logger.warning(
            f"Online hit pending cap {ONLINE_HIT_MAX_PENDING} reached; "
            f"expired {evicted} oldest recommendation(s)")


def record_online_recommendation(user_id, backend, response_text):
    """Remember this backend's top-3 for user_id. Failures are logged only."""
    if not ONLINE_HIT_ENABLED:
        return
    try:
        movies = _top_movie_ids(response_text)
        if not movies:
            return
        now = time.monotonic()
        global _online_hit_next_id
        with _online_hit_lock:
            _expire_online_hits_locked(now)
            _evict_online_hits_to_cap_locked()
            entry_id = _online_hit_next_id
            _online_hit_next_id += 1
            entry = {
                'user_id': user_id,
                'backend': backend,
                'movies': movies,
                'expires_at': now + ONLINE_HIT_WINDOW_SECONDS,
            }
            _online_hit_entries[entry_id] = entry
            _online_hit_by_user.setdefault(user_id, {})[entry_id] = entry
            online_recs_served_total.labels(backend=backend).inc()
    except Exception as e:
        logger.error(f"Failed to record online recommendation for user {user_id}: {e}")


def observe_online_watch(user_id, movie_id):
    """One hit per served list when movie_id is in its top-3 and still in window."""
    if not ONLINE_HIT_ENABLED:
        return
    try:
        now = time.monotonic()
        with _online_hit_lock:
            per_user = _online_hit_by_user.get(user_id)
            if not per_user:
                return
            expired_ids = []
            hit_ids = []
            for entry_id, entry in per_user.items():
                if entry['expires_at'] <= now:
                    expired_ids.append(entry_id)
                elif movie_id in entry['movies']:
                    hit_ids.append(entry_id)
            for entry_id in expired_ids:
                entry = _drop_online_hit_entry_locked(entry_id)
                if entry is not None:
                    online_recs_expired_total.labels(backend=entry['backend']).inc()
            for entry_id in hit_ids:
                entry = _drop_online_hit_entry_locked(entry_id)
                if entry is not None:
                    online_hits_at_3_total.labels(backend=entry['backend']).inc()
    except Exception as e:
        logger.error(f"Failed to score online watch for user {user_id}: {e}")


def parse_watch_event(line):
    """Parse '<time>,<userid>,GET /data/m/<movieid>/<minute>.mpg'. None if not a watch."""
    if not line or '/data/m/' not in line:
        return None
    parts = line.split(',', 2)
    if len(parts) < 3:
        return None
    _timestamp, userid, action = parts
    match = _WATCH_MOVIE_RE.search(action)
    if match is None:
        return None
    try:
        return int(userid.strip()), match.group(1)
    except ValueError:
        return None


def _decode_kafka_value(value):
    if not value:
        return None
    try:
        return value.decode('utf-8')
    except Exception:
        return None


def online_hit_sweep_loop():
    """Drop expired recommendations while the request path and Kafka are idle."""
    while True:
        time.sleep(15)
        if not ONLINE_HIT_ENABLED:
            continue
        try:
            with _online_hit_lock:
                _expire_online_hits_locked(time.monotonic())
        except Exception as e:
            logger.error(f"Online hit sweep error: {e}")


def online_hit_consume_loop():
    """Read movielog1 watches on this process's own group. Never raises to Flask."""
    if KafkaConsumer is None:
        logger.error("kafka-python is not installed; online hit@3 watches will not be counted")
        return
    delay = 5
    while True:
        consumer = None
        try:
            logger.info(
                f"Online hit@3 consuming topic {KAFKA_TOPIC} "
                f"at {KAFKA_BOOTSTRAP_SERVERS} group {ONLINE_HIT_GROUP_ID}")
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[
                    server.strip()
                    for server in KAFKA_BOOTSTRAP_SERVERS.split(',')
                    if server.strip()
                ],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id=ONLINE_HIT_GROUP_ID,
                value_deserializer=_decode_kafka_value,
            )
            delay = 5
            for message in consumer:
                parsed = parse_watch_event(message.value)
                if parsed is None:
                    continue
                user_id, movie_id = parsed
                observe_online_watch(user_id, movie_id)
        except Exception as e:
            logger.error(f"Online hit Kafka consumer error: {e}")
            time.sleep(delay)
            delay = min(delay * 2, 60)
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception as close_error:
                    logger.error(f"Online hit Kafka consumer close failed: {close_error}")


@app.route('/recommend/<userid>', methods=['GET'])
def recommend(userid):
    """
    Route recommendation requests using weighted routing strategy.
    Logs routing decisions to database if enabled.
    A 200 response is remembered for online hit@3.
    """
    # None until select_backend() returns, so errors before that are backend "none".
    target = None
    try:
        # Parse userid to integer
        user_id = int(userid)
        
        # Weighted selection: choose backend based on configured weights
        backend_url, target = select_backend()
        
        logger.info(f"Routing user {user_id} to {target} (weighted: {INFERENCE_1_WEIGHT}/{INFERENCE_2_WEIGHT})")
        
        last_error = None
        response = None
        for attempt in (1, 2):
            try:
                response = forward_recommend(backend_url, target, userid)
                last_error = None
            except requests.exceptions.RequestException as e:
                logger.error(f"Backend error for user {userid} via {target}: {str(e)}")
                last_error = e
                response = None
            
            # 4xx is the backend's answer. 5xx and transport errors try the other backend once.
            if response is not None and response.status_code < 500:
                response_status = response.status_code
                response_text = response.text if response_status == 200 else None
                
                logger.info(f"Response from {target} for user {user_id}: status {response_status}")
                
                # Log routing decision to database (async, non-blocking)
                log_routing_decision(user_id, target, response_status, response_text)
                if response_status == 200:
                    record_online_recommendation(user_id, target, response_text)
                
                # Return response (text for /recommend endpoint)
                return response.text, response_status
            
            if response is not None:
                logger.error(f"Backend {target} returned {response.status_code} for user {userid}")
            if attempt == 1:
                backend_url, target = alternate_backend(target)
                logger.info(f"Retrying user {user_id} on {target}")
        
        detail = str(last_error) if last_error is not None else f"status {response.status_code}"
        logger.error(f"Backend error for user {userid}: {detail}")
        # Log error case
        try:
            log_routing_decision(user_id, "error", 502, detail[:200])
        except:
            pass
        return f"Backend error: {detail}", 502
        
    except ValueError:
        logger.error(f"Invalid user ID format: {userid}")
        lb_requests_total.labels(backend="none" if target is None else target, status="400").inc()
        return "Invalid user ID format", 400
    except Exception as e:
        logger.error(f"Internal error for user {userid}: {str(e)}")
        lb_requests_total.labels(backend="none" if target is None else target, status="500").inc()
        # Log error case
        try:
            user_id_int = int(userid) if userid.isdigit() else 0
            log_routing_decision(user_id_int, "error", 500, str(e)[:200])
        except:
            pass
        return f"Internal error: {str(e)}", 500


@app.route('/health', methods=['GET'])
def health():
    """
    Health check endpoint that monitors both inference containers.
    Uses dedicated health check port (8083) to avoid queue delays.
    Returns the health status of inference-1 and inference-2.
    """
    health_1 = check_inference_health(INFERENCE_1_HEALTH, "inference-1")
    health_2 = check_inference_health(INFERENCE_2_HEALTH, "inference-2")
    
    # Overall status: healthy if both are healthy
    overall_healthy = health_1['healthy'] and health_2['healthy']
    
    response_data = {
        'status': 'healthy' if overall_healthy else 'degraded',
        'inference_1': health_1,
        'inference_2': health_2,
        'routing_weights': {
            'inference_1': INFERENCE_1_WEIGHT,
            'inference_2': INFERENCE_2_WEIGHT
        }
    }
    
    status_code = 200 if overall_healthy else 503  # 503 Service Unavailable if any unhealthy
    return jsonify(response_data), status_code


@app.route('/metrics', methods=['GET'])
def metrics():
    """Prometheus metrics endpoint"""
    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}


# Start database logging worker thread if enabled
if ENABLE_DB_LOGGING:
    db_log_thread = threading.Thread(target=log_to_database_worker, daemon=True)
    db_log_thread.start()
    logger.info("Database logging worker thread started")

if ONLINE_HIT_ENABLED:
    threading.Thread(target=online_hit_sweep_loop, name='online-hit-sweep', daemon=True).start()
    threading.Thread(target=online_hit_consume_loop, name='online-hit-kafka', daemon=True).start()
    logger.info("Online hit@3 threads started")

if __name__ == '__main__':
    # Default port 8080 for load balancer
    app.run(host='0.0.0.0', port=8080)
