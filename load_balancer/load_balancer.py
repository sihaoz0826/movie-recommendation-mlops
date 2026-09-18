from flask import Flask, request, Response, jsonify
import requests
import logging
import os
import random
import pyodbc
import queue
import threading
from datetime import datetime

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
# Get weights from environment variables (default: 90% inference-1, 10% inference-2 for canary)
INFERENCE_1_WEIGHT = float(os.environ.get('INFERENCE_1_WEIGHT', '90'))
INFERENCE_2_WEIGHT = float(os.environ.get('INFERENCE_2_WEIGHT', '10'))

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

@app.route('/recommend/<userid>', methods=['GET'])
def recommend(userid):
    """
    Route recommendation requests using weighted routing strategy.
    Logs routing decisions to database if enabled.
    """
    try:
        # Parse userid to integer
        user_id = int(userid)
        
        # Weighted selection: choose backend based on configured weights
        backend_url, target = select_backend()
        
        logger.info(f"Routing user {user_id} to {target} (weighted: {INFERENCE_1_WEIGHT}/{INFERENCE_2_WEIGHT})")
        
        # Forward request to selected backend
        url = f"{backend_url}/recommend/{userid}"
        response = requests.get(url, timeout=150)  # Match actual response times (80-135s)
        
        response_status = response.status_code
        response_text = response.text if response_status == 200 else None
        
        logger.info(f"Response from {target} for user {user_id}: status {response_status}")
        
        # Log routing decision to database (async, non-blocking)
        log_routing_decision(user_id, target, response_status, response_text)
        
        # Return response (text for /recommend endpoint)
        return response.text, response_status
        
    except ValueError:
        logger.error(f"Invalid user ID format: {userid}")
        return "Invalid user ID format", 400
    except requests.exceptions.RequestException as e:
        logger.error(f"Backend error for user {userid}: {str(e)}")
        # Log error case
        try:
            user_id_int = int(userid) if userid.isdigit() else 0
            log_routing_decision(user_id_int, "error", 502, str(e)[:200])
        except:
            pass
        return f"Backend error: {str(e)}", 502
    except Exception as e:
        logger.error(f"Internal error for user {userid}: {str(e)}")
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


# Start database logging worker thread if enabled
if ENABLE_DB_LOGGING:
    db_log_thread = threading.Thread(target=log_to_database_worker, daemon=True)
    db_log_thread.start()
    logger.info("Database logging worker thread started")

if __name__ == '__main__':
    # Default port 8080 for load balancer
    app.run(host='0.0.0.0', port=8080)
