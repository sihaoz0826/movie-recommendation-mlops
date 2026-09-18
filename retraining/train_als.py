import implicit
from scipy.sparse import csr_matrix
import pickle
import pandas as pd
import numpy as np
import os
import sys
import time
from datetime import datetime
import mlflow
from pathlib import Path
from kubernetes import client, config
from kubernetes.client.rest import ApiException

# ratings_blob.py is copied next to this script in Docker; locally it lives in ../shared
_here = os.path.dirname(os.path.abspath(__file__))
for _p in (_here, os.path.normpath(os.path.join(_here, "..", "shared"))):
    if _p not in sys.path:
        sys.path.insert(0, _p)
from ratings_blob import blob_configured, download_ratings_lookback, ratings_lookback_days

def restart_inference_deployment(deployment_name="inference-2-deployment", namespace="default"):
    """
    Trigger a rolling restart of the inference deployment to pick up new model files.
    This ensures zero downtime by using Kubernetes rolling update strategy.
    """
    try:
        # Load in-cluster config (works when running in Kubernetes)
        try:
            config.load_incluster_config()
        except config.ConfigException:
            # Fallback to kubeconfig (for local testing)
            config.load_kube_config()
        
        apps_v1 = client.AppsV1Api()
        
        print(f"[{datetime.now()}] Triggering rolling restart of {deployment_name}...")
        
        # Get current deployment
        deployment = apps_v1.read_namespaced_deployment(deployment_name, namespace)
        
        # Annotate the deployment to trigger a rolling restart
        # This is the standard way to restart pods without changing the image
        if deployment.spec.template.metadata.annotations is None:
            deployment.spec.template.metadata.annotations = {}
        
        # Add/update annotation with current timestamp to force pod recreation
        deployment.spec.template.metadata.annotations['kubectl.kubernetes.io/restartedAt'] = datetime.now().isoformat()
        
        # Patch the deployment
        apps_v1.patch_namespaced_deployment(
            name=deployment_name,
            namespace=namespace,
            body=deployment
        )
        
        print(f"[{datetime.now()}] Successfully triggered rolling restart of {deployment_name}")
        print(f"  Kubernetes will gradually replace pods to pick up new model files")
        return True
        
    except ApiException as e:
        print(f"[{datetime.now()}] Error triggering deployment restart: {e}")
        print(f"  Status: {e.status}, Reason: {e.reason}")
        return False
    except Exception as e:
        print(f"[{datetime.now()}] Unexpected error during deployment restart: {e}")
        import traceback
        traceback.print_exc()
        return False

def load_training_ratings():
    """Load ratings from Azure Blob lookback window.

    Blob is required. Lookback is UTC calendar dates: dt >= today - RATINGS_LOOKBACK_DAYS
    (N=1 includes today and yesterday). Parts are concatenated before ALS fit.
    """
    if not blob_configured():
        raise RuntimeError(
            "AZURE_STORAGE_CONNECTION_STRING is required; ratings are loaded from Azure Blob only."
        )

    lookback_days = ratings_lookback_days()
    print(f"[{datetime.now()}] Loading ratings from Azure Blob "
          f"(lookback_days={lookback_days})...")
    ratings = download_ratings_lookback()
    if ratings is None or ratings.empty:
        raise FileNotFoundError(
            "No ratings blobs in the lookback window "
            f"(RATINGS_LOOKBACK_DAYS={lookback_days}). "
            "Expected parts under ratings/incoming/dt=YYYY-MM-DD/."
        )
    print(f"  Loaded {len(ratings):,} ratings from blob parts")
    return ratings

def train_als_model():
    """
    Train ALS model with MLflow integration.
    Saves active model to PVC and logs all versions to MLflow/Azure Blob.
    """
    start_time = time.time()
    
    # Get environment variables
    shared_volume_path = os.environ.get('SHARED_VOLUME_PATH', '/shared-volume')
    inference_instance = os.environ.get('INFERENCE_INSTANCE', 'inference_2')
    mlflow_tracking_uri = os.environ.get('MLFLOW_TRACKING_URI', 'http://mlflow-service:5000')
    
    # Set MLflow tracking URI
    mlflow.set_tracking_uri(mlflow_tracking_uri)
    
    # Set experiment name
    mlflow.set_experiment("movie-recommender")
    
    print(f"[{datetime.now()}] Starting ALS model training")
    print(f"  Shared volume: {shared_volume_path}")
    print(f"  Inference instance: {inference_instance}")
    print(f"  MLflow URI: {mlflow_tracking_uri}")
    
    try:
        # Azure Blob parts in the lookback window are the only training-data source.
        print(f"[{datetime.now()}] Loading ratings data...")
        ratings = load_training_ratings()
        
        # Create integer mappings
        print(f"[{datetime.now()}] Creating user/item mappings...")
        unique_users = sorted(ratings['userid'].unique())
        unique_items = sorted(ratings['movieid'].unique())
        
        user_to_idx = {user: idx for idx, user in enumerate(unique_users)}
        item_to_idx = {item: idx for idx, item in enumerate(unique_items)}
        idx_to_user = {idx: user for user, idx in user_to_idx.items()}
        idx_to_item = {idx: item for item, idx in item_to_idx.items()}
        
        # Map to integers
        ratings['user_idx'] = ratings['userid'].map(user_to_idx)
        ratings['item_idx'] = ratings['movieid'].map(item_to_idx)
        
        # Create sparse matrix (required by implicit)
        print(f"[{datetime.now()}] Creating sparse matrix...")
        matrix = csr_matrix(
            (ratings['rating'].values, 
             (ratings['user_idx'].values, ratings['item_idx'].values)),
            shape=(len(unique_users), len(unique_items))
        )
        
        num_users = len(unique_users)
        num_items = len(unique_items)
        num_ratings = matrix.nnz
        sparsity = 1.0 - (num_ratings / (num_users * num_items))
        
        print(f"  Matrix shape: {matrix.shape}")
        print(f"  Non-zero entries: {num_ratings:,}")
        print(f"  Sparsity: {sparsity:.4f}")
        
        # Start MLflow run with timestamp-based name
        run_name = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        with mlflow.start_run(run_name=run_name):
            # Train model
            print(f"[{datetime.now()}] Training ALS model...")
            model = implicit.als.AlternatingLeastSquares(factors=50, iterations=15, regularization=0.1)
            model.fit(matrix)
            print("  Model trained!")
            
            # Calculate time-period user factors from actual timestamp data
            print(f"[{datetime.now()}] Calculating time-period-specific user factors...")
            ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], format='mixed', errors='coerce')
            ratings['hour'] = ratings['timestamp'].dt.hour
            ratings['time_period'] = ratings['hour'] // 2  # Each period is 2 hours (0-11)
            
            # Initialize array for 12 time periods
            factor_dim = model.user_factors.shape[1]
            time_period_user_factors = np.zeros((12, factor_dim))
            
            # Calculate period-specific averages
            for period in range(12):
                # Find users who rated movies during this time period
                period_ratings = ratings[ratings['time_period'] == period]
                active_userids = period_ratings['userid'].unique()
                
                # Get user indices for active users (only those in training data)
                active_user_indices = []
                for userid in active_userids:
                    if userid in user_to_idx:
                        active_user_indices.append(user_to_idx[userid])
                
                if len(active_user_indices) > 0:
                    # Calculate average user factor for users active in this period
                    period_user_factors = model.user_factors[active_user_indices]
                    time_period_user_factors[period] = np.mean(period_user_factors, axis=0)
                    print(f"  Period {period}: {len(active_user_indices)} active users")
                else:
                    # Fallback to overall average if no users in this period
                    time_period_user_factors[period] = np.mean(model.user_factors, axis=0)
                    print(f"  Period {period}: No active users, using overall average")
            
            # Save mappings including time-period user factors
            mappings = {
                'user_to_idx': user_to_idx,
                'item_to_idx': item_to_idx,
                'idx_to_user': idx_to_user,
                'idx_to_item': idx_to_item,
                'time_period_user_factors': time_period_user_factors
            }
            
            # Save active model to PVC (for inference services)
            output_dir = Path(shared_volume_path) / inference_instance
            output_dir.mkdir(parents=True, exist_ok=True)
            
            model_path = output_dir / 'als_model.pkl'
            mappings_path = output_dir / 'als_mappings.pkl'
            
            print(f"[{datetime.now()}] Saving active model to {output_dir}...")
            with open(model_path, 'wb') as f:
                pickle.dump(model, f)
            with open(mappings_path, 'wb') as f:
                pickle.dump(mappings, f)
            
            # Calculate model size
            model_size_mb = (model_path.stat().st_size + mappings_path.stat().st_size) / (1024 * 1024)
            
            training_time = time.time() - start_time
            
            # Log parameters to MLflow
            mlflow.log_params({
                'factors': 50,
                'iterations': 15,
                'regularization': 0.1,
                'data_size': len(ratings),
                'num_users': num_users,
                'num_items': num_items
            })
            
            # Log metrics to MLflow
            mlflow.log_metrics({
                'training_time_seconds': training_time,
                'model_size_mb': model_size_mb,
                'matrix_sparsity': sparsity,
                'num_users': num_users,
                'num_items': num_items
            })
            
            # Log tags to MLflow
            mlflow.set_tags({
                'model_type': 'ALS',
                'inference_instance': inference_instance,
                'deployment_status': 'canary',
                'training_trigger': 'scheduled',
                'training_timestamp': datetime.now().isoformat()
            })
            
            # Log artifacts to MLflow (stored in Azure Blob)
            print(f"[{datetime.now()}] Logging artifacts to MLflow...")
            mlflow.log_artifact(str(model_path), "model")
            mlflow.log_artifact(str(mappings_path), "model")
            
            print(f"[{datetime.now()}] Training completed successfully!")
            print(f"  Training time: {training_time:.2f} seconds")
            print(f"  Model size: {model_size_mb:.2f} MB")
            print(f"  Active model saved to: {output_dir}")
            print(f"  Model version logged to MLflow")
            
            # Trigger rolling restart of inference-2 deployment
            if inference_instance == "inference_2":
                restart_success = restart_inference_deployment("inference-2-deployment")
                if restart_success:
                    print(f"[{datetime.now()}] Inference pods will gradually restart to load new model")
                else:
                    print(f"[{datetime.now()}] WARNING: Failed to trigger deployment restart. Pods may not pick up new model.")
            
            return True
            
    except Exception as e:
        print(f"[{datetime.now()}] Error during training: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    success = train_als_model()
    exit(0 if success else 1)

