from flask import Flask, request, jsonify
import pickle
import numpy as np
from datetime import datetime
import os
import threading

# Fix OpenBLAS thread pool issue for better performance under load
os.environ['OPENBLAS_NUM_THREADS'] = '1'

app = Flask(__name__)

# Configurable paths for model and mappings
# Use INFERENCE_INSTANCE environment variable to load from correct directory
INFERENCE_INSTANCE = os.environ.get('INFERENCE_INSTANCE', 'inference_1')
MODEL_PATH = f'/model_files/{INFERENCE_INSTANCE}/als_model.pkl'
MAPPINGS_PATH = f'/model_files/{INFERENCE_INSTANCE}/als_mappings.pkl'

# Fallback popular movies to use when model prediction fails
FALLBACK_MOVIES = [
    'star+wars+1977',
    'the+godfather+1972',
    'forrest+gump+1994'
]

# Global variables to store loaded model and mappings
model = None
mappings = None
user_to_idx = None  # Cached dictionary (extracted once at startup)
idx_to_item = None  # Cached dictionary (extracted once at startup)
item_to_idx = None  # Cached dictionary (extracted once at startup)
time_period_user_factors = None  # Array of 12 user factors for different time periods (2-hour periods)


def get_fallback_recommendations(userid, N=3):
    """
    Get fallback movie recommendations using popular movies.
    
    This is used when the model fails to generate recommendations due to:
    - Empty item scores
    - Missing mappings
    - Invalid scores (NaN/inf)
    - Other unexpected errors
    
    Args:
        userid: The user ID (for consistency with main function signature)
        N: Number of recommendations to return (default: 3)
    
    Returns:
        dict: Dictionary with 'userid', 'recommendations' (list of dicts with 'movieid' and 'score'), and 'count'
    """
    # Use fallback movies, repeating if N > len(FALLBACK_MOVIES)
    fallback_list = (FALLBACK_MOVIES * ((N // len(FALLBACK_MOVIES)) + 1))[:N]
    
    recommendations = []
    for i, movie_id in enumerate(fallback_list):
        recommendations.append({
            'movieid': movie_id,
            'score': 0.0  # Fallback score is 0.0 to indicate it's not from model
        })
    
    return {
        'userid': userid,
        'recommendations': recommendations,
        'count': len(recommendations)
    }


def get_time_period_index():
    """
    Get the time period index (0-11) from the current time.
    
    The day is divided into 12 periods of 2 hours each:
    - Period 0: 00:00-01:59
    - Period 1: 02:00-03:59
    - Period 2: 04:00-05:59
    - Period 3: 06:00-07:59
    - Period 4: 08:00-09:59
    - Period 5: 10:00-11:59
    - Period 6: 12:00-13:59
    - Period 7: 14:00-15:59
    - Period 8: 16:00-17:59
    - Period 9: 18:00-19:59
    - Period 10: 20:00-21:59
    - Period 11: 22:00-23:59
    
    Returns:
        int: Time period index (0-11)
    """
    hour = datetime.now().hour
    # Each period is 2 hours, so divide by 2
    return hour // 2


def load_model_and_mappings():
    """Load the trained model and mappings from pickle files."""
    global model, mappings, user_to_idx, idx_to_item, item_to_idx
    global time_period_user_factors
    
    try:
        # Load model
        with open(MODEL_PATH, 'rb') as f:
            model = pickle.load(f)
        
        # Load mappings
        with open(MAPPINGS_PATH, 'rb') as f:
            mappings = pickle.load(f)
        
        # Extract dictionaries once (avoid repeated .get() calls on every request)
        user_to_idx = mappings.get('user_to_idx', {})
        idx_to_item = mappings.get('idx_to_item', {})
        item_to_idx = mappings.get('item_to_idx', {})
        
        # Load pre-computed time-period user factors (calculated during training from actual timestamp data)
        time_period_user_factors = mappings.get('time_period_user_factors')
        
        if time_period_user_factors is None:
            raise RuntimeError('Time period user factors not found in mappings. Please re-run training notebook.')
        
        if time_period_user_factors.shape[0] != 12:
            raise RuntimeError(f'Expected 12 time periods, but found {time_period_user_factors.shape[0]}')
        
        print(f"Loaded pre-computed time-period user factors: shape {time_period_user_factors.shape}")
        
        return True
    except FileNotFoundError as e:
        print(f"File not found: {e}")
        return False
    except Exception as e:
        print(f"Error loading model or mappings: {e}")
        return False


def get_recommendations(userid, N=3, time_period=None):
    """
    Get movie recommendations for a user.
    
    Handles both seen and unseen users:
    - Seen user: uses their specific user factor
    - Unseen user: uses time-period-specific user factor based on current time or time_period parameter
    
    All recommendations are from movies that were seen in training (model only knows about seen movies).
    
    Args:
        userid: The user ID to get recommendations for
        N: Number of recommendations to return (default: 3)
        time_period: Direct time period index (0-11). If provided, uses this period; otherwise uses current time.
    
    Returns:
        dict: Dictionary with 'userid', 'recommendations' (list of dicts with 'movieid' and 'score'), and 'count'
    
    Raises:
        RuntimeError: If model or mappings not loaded
        ValueError: If time_period is not in range 0-11
    """
    if model is None or idx_to_item is None:
        raise RuntimeError('Model or mappings not loaded')
    
    # Determine user factor based on whether user is seen or unseen
    is_seen_user = userid in user_to_idx
    period_index = None
    
    if is_seen_user:
        # User is seen - use their specific user factor
        user_idx = user_to_idx[userid]
        user_factor = model.user_factors[user_idx]
    else:
        # User is unseen - use time-period-specific user factor
        if time_period_user_factors is None:
            raise RuntimeError('Time period user factors not calculated')
        
        # Get the time period index (0-11)
        if time_period is not None:
            # Use directly provided time_period
            if not isinstance(time_period, int) or time_period < 0 or time_period > 11:
                raise ValueError('time_period must be an integer between 0 and 11')
            period_index = time_period
        else:
            # Calculate from current time
            period_index = get_time_period_index()
        
        user_factor = time_period_user_factors[period_index]
    
    # Compute scores for all items using the appropriate user factor
    # All items are from seen movies (model.item_factors only contains seen movies)
    try:
        item_scores = np.dot(model.item_factors, user_factor)
    except Exception as e:
        print(f"Error computing item scores: {e}. Using fallback recommendations.")
        return get_fallback_recommendations(userid, N)
    
    # Check for empty item scores
    if len(item_scores) == 0:
        print("Warning: Empty item scores. Using fallback recommendations.")
        return get_fallback_recommendations(userid, N)
    
    # Check for invalid scores (NaN or inf)
    if np.any(np.isnan(item_scores)) or np.any(np.isinf(item_scores)):
        print("Warning: Invalid scores (NaN/inf) detected. Using fallback recommendations.")
        return get_fallback_recommendations(userid, N)
    
    # OPTIMIZED: Use argpartition instead of full sort for better performance
    # This is O(n) instead of O(n log n) when N << total_items
    try:
        if N >= len(item_scores):
            # If N is large, just sort normally
            top_indices = np.argsort(item_scores)[::-1][:N]
        else:
            # Use argpartition for partial sort - much faster for small N
            # Get indices of top N items without fully sorting
            top_indices = np.argpartition(item_scores, -N)[-N:]
            # Sort only the top N items
            top_indices = top_indices[np.argsort(item_scores[top_indices])[::-1]]
            # Ensure we only return exactly N items (safety check)
            top_indices = top_indices[:N]
    except Exception as e:
        print(f"Error sorting item scores: {e}. Using fallback recommendations.")
        return get_fallback_recommendations(userid, N)
    
    # Format recommendations - use direct indexing instead of .get() for speed
    recommendations = []
    for item_idx in top_indices:
        try:
            movie_id = idx_to_item[item_idx]  # Direct access is faster than .get()
            score = float(item_scores[item_idx])
            recommendations.append({
                'movieid': movie_id,
                'score': score
            })
        except KeyError as e:
            print(f"Warning: Missing mapping for item_idx {item_idx}: {e}. Skipping this item.")
            continue
        except Exception as e:
            print(f"Warning: Error processing item_idx {item_idx}: {e}. Skipping this item.")
            continue
    
    # If we don't have enough recommendations, use fallback
    if len(recommendations) < N:
        print(f"Warning: Only generated {len(recommendations)} recommendations, but {N} requested. Using fallback.")
        return get_fallback_recommendations(userid, N)
    
    return {
        'userid': userid,
        'recommendations': recommendations,
        'count': len(recommendations)
    }


@app.route('/recommend/<userid>', methods=['GET'])
def recommend(userid):
    """
    Get movie recommendations for a user.
    
    This endpoint handles recommendations for both seen and unseen users.
    For unseen users, it uses time-period-specific user factors based on current time or time_period parameter.
    All recommended movies are from the training data (seen movies).
    
    Path parameters:
    - userid: The user ID to get recommendations for (required, in URL path)
    
    Query parameters:
    - N: Number of recommendations to return (default: 3)
    - time_period: Direct time period index (0-11). If provided, uses this period; otherwise uses current time. (optional)
    
    Time periods:
    - 0: 00:00-01:59, 1: 02:00-03:59, 2: 04:00-05:59, 3: 06:00-07:59
    - 4: 08:00-09:59, 5: 10:00-11:59, 6: 12:00-13:59, 7: 14:00-15:59
    - 8: 16:00-17:59, 9: 18:00-19:59, 10: 20:00-21:59, 11: 22:00-23:59
    """
    if model is None or mappings is None:
        return jsonify({'error': 'Model or mappings not loaded'}), 500
    
    # Convert userid to integer (userids in training data are integers)
    try:
        userid = int(userid)
    except (ValueError, TypeError):
        return jsonify({'error': 'userid must be a valid integer'}), 400
    
    try:
        N = int(request.args.get('N', 3))
        if N < 1:
            return jsonify({'error': 'N must be at least 1'}), 400
    except (ValueError, TypeError):
        return jsonify({'error': 'N must be a valid integer'}), 400
    
    # Get optional time_period parameter
    time_period = None
    time_period_str = request.args.get('time_period')
    if time_period_str:
        try:
            time_period = int(time_period_str)
            if time_period < 0 or time_period > 11:
                return jsonify({'error': 'time_period must be between 0 and 11'}), 400
        except (ValueError, TypeError):
            return jsonify({'error': 'time_period must be a valid integer between 0 and 11'}), 400
    
    try:
        result = get_recommendations(userid, N, time_period)
        # Extract just the movie IDs from recommendations
        movie_ids = [rec['movieid'] for rec in result['recommendations']]
        print(movie_ids)
        # Return comma-separated list as plain text (not JSON array) - simulator expects this format
        return ','.join(movie_ids), 200, {'Content-Type': 'text/plain'}
    
    except ValueError as e:
        # For ValueError (e.g., invalid time_period), try fallback before returning error
        print(f"ValueError occurred: {e}. Attempting fallback recommendations.")
        try:
            fallback_result = get_fallback_recommendations(userid, N)
            movie_ids = [rec['movieid'] for rec in fallback_result['recommendations']]
            print(f"Using fallback recommendations: {movie_ids}")
            return ','.join(movie_ids), 200, {'Content-Type': 'text/plain'}
        except Exception:
            return jsonify({'error': str(e)}), 400
    except RuntimeError as e:
        # For RuntimeError (e.g., model not loaded), try fallback if model is actually loaded
        if model is not None and mappings is not None:
            print(f"RuntimeError occurred: {e}. Attempting fallback recommendations.")
            try:
                fallback_result = get_fallback_recommendations(userid, N)
                movie_ids = [rec['movieid'] for rec in fallback_result['recommendations']]
                print(f"Using fallback recommendations: {movie_ids}")
                return ','.join(movie_ids), 200, {'Content-Type': 'text/plain'}
            except Exception:
                return jsonify({'error': str(e)}), 500
        else:
            return jsonify({'error': str(e)}), 500
    except Exception as e:
        # For any other exception, try fallback before returning error
        print(f"Unexpected error occurred: {e}. Attempting fallback recommendations.")
        try:
            fallback_result = get_fallback_recommendations(userid, N)
            movie_ids = [rec['movieid'] for rec in fallback_result['recommendations']]
            print(f"Using fallback recommendations: {movie_ids}")
            return ','.join(movie_ids), 200, {'Content-Type': 'text/plain'}
        except Exception as fallback_error:
            return jsonify({'error': f'Error generating recommendations: {str(e)}. Fallback also failed: {str(fallback_error)}'}), 500


# Create a separate lightweight Flask app for health checks on port 8083
health_app = Flask(__name__)

@health_app.route('/health', methods=['GET'])
def health_check():
    """Lightweight health check endpoint on port 8083 (separate from main app)."""
    if model is None or mappings is None:
        return jsonify({'status': 'error', 'message': 'Model or mappings not loaded'}), 500
    return jsonify({
        'status': 'healthy', 
        'model_loaded': model is not None, 
        'mappings_loaded': mappings is not None,
        'time_period_user_factors_loaded': time_period_user_factors is not None
    })


def run_health_server():
    """Run the health check server on port 8083 in a separate thread."""
    health_app.run(host='0.0.0.0', port=8083, debug=False, threaded=True, use_reloader=False)


if __name__ == '__main__':
    # Load model and mappings at startup
    if load_model_and_mappings():
        # Start health check server on port 8083 in a separate thread
        health_thread = threading.Thread(target=run_health_server, daemon=True)
        health_thread.start()
        print("Health check server started on port 8083")
        
        # Use environment variables for host and port (set in docker-compose)
        host = os.environ.get('HOST', '0.0.0.0')
        port = int(os.environ.get('PORT', 8082))
        print(f"Main inference server starting on port {port}")
        app.run(host=host, port=port, debug=False, threaded=True)