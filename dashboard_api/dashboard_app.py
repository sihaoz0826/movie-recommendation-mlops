from flask import Flask, jsonify, request
from flask_cors import CORS
import pyodbc
import os
import logging
from datetime import datetime, timedelta

# Detect if running in Kubernetes
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
CORS(app)  # Enable CORS for frontend

# Database configuration
DB_SERVER = os.environ.get('AZURE_SERVER')
DB_DATABASE = os.environ.get('AZURE_DATABASE')
DB_USERNAME = os.environ.get('AZURE_USERNAME')
DB_PASSWORD = os.environ.get('AZURE_PASSWORD')
PORT = int(os.environ.get('PORT', 8086))

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

@app.route('/api/user/<int:user_id>/routing', methods=['GET'])
def get_user_routing(user_id):
    """
    Get routing breakdown for a specific user.
    
    Query parameters:
    - hours (optional, default: 24) - Time range in hours to look back
    
    Returns:
        JSON with user_id and routing_breakdown (inference-1 and inference-2 counts)
    """
    try:
        # Get hours parameter (default: 24)
        hours = int(request.args.get('hours', 24))
        if hours < 1:
            return jsonify({'error': 'hours must be at least 1'}), 400
        
        # Calculate time threshold
        time_threshold = datetime.utcnow() - timedelta(hours=hours)
        
        # Connect to database
        conn = get_db_connection()
        if conn is None:
            return jsonify({'error': 'Database connection failed'}), 500
        
        try:
            cursor = conn.cursor()
            
            # Query routing decisions for this user within the time range
            # Get both counts and response_text
            query = """
                SELECT inference_instance, response_text, timestamp
                FROM routing_decisions
                WHERE user_id = ? AND timestamp >= ?
                ORDER BY timestamp DESC
            """
            cursor.execute(query, user_id, time_threshold)
            
            # Build routing breakdown and collect response texts
            routing_breakdown = {
                'inference-1': 0,
                'inference-2': 0
            }
            responses = {
                'inference-1': [],
                'inference-2': []
            }
            
            for row in cursor.fetchall():
                instance = row[0]
                response_text = row[1] if row[1] else ''
                timestamp = row[2]
                
                # Count routing decisions
                if instance in routing_breakdown:
                    routing_breakdown[instance] += 1
                
                # Collect response texts (only for inference-1 and inference-2, not errors)
                if instance in responses and response_text:
                    responses[instance].append({
                        'response_text': response_text,
                        'timestamp': timestamp.isoformat() if timestamp else None
                    })
            
            return jsonify({
                'user_id': user_id,
                'routing_breakdown': routing_breakdown,
                'responses': responses,
                'hours': hours
            }), 200
            
        except Exception as e:
            logger.error(f"Error querying database: {e}")
            return jsonify({'error': 'Database query failed'}), 500
        finally:
            conn.close()
            
    except ValueError:
        return jsonify({'error': 'Invalid user_id or hours parameter'}), 400
    except Exception as e:
        logger.error(f"Unexpected error in get_user_routing: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/health', methods=['GET'])
def health():
    """
    Health check endpoint.
    """
    # Try to connect to database to verify connectivity
    conn = get_db_connection()
    if conn is None:
        return jsonify({
            'status': 'unhealthy',
            'database': 'disconnected'
        }), 503
    
    conn.close()
    return jsonify({
        'status': 'healthy',
        'database': 'connected'
    }), 200

if __name__ == '__main__':
    logger.info(f"Starting Dashboard API on port {PORT}")
    app.run(host='0.0.0.0', port=PORT)

