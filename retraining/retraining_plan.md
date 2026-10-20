# Retraining System Plan with MLflow Integration

## Overview

This document outlines the complete retraining system for the movie recommendation service. The system includes:
- **Scheduled model retraining** using Kubernetes CronJob
- **MLflow integration** for model tracking and versioning
- **Canary release strategy** (inference-1 stable, inference-2 gets updated models)
- **Azure Blob Storage** for MLflow artifact storage (unlimited, scalable)
- **Model versioning** via MLflow (all historical models stored in Azure Blob)

## Architecture

```
┌─────────────────────┐
│  MLflow Server      │  ← Tracking & Model Registry
│  (Deployment)       │
└─────────┬───────────┘
          │
          │ Stores artifacts
          ▼
┌─────────────────────┐
│  Azure Blob Storage │  ← All model versions (unlimited)
│  (External)         │
└─────────────────────┘
          ▲
          │ Logs metrics/artifacts
          │
┌─────────────────────┐
│  Trainer CronJob   │  ← Runs training on schedule
│  (Scheduled Job)   │
└─────────┬───────────┘
          │
          │ Writes active models
          ▼
┌─────────────────────┐
│  PersistentVolume   │
│  (PVC: model-pvc)   │
│                     │
│  /model_files/      │
│  ├── inference_1/  │  ← Stable (never changes)
│  └── inference_2/  │  ← Canary (updated by trainer)
│                     │
│  /mlflow-db/        │  ← SQLite metadata (small)
└─────────────────────┘
          │
          │ Reads models
          ▼
┌─────────────────────┐
│  Inference Services │
│  - inference-1     │  ← Reads from inference_1/
│  - inference-2      │  ← Reads from inference_2/
└─────────────────────┘
```

## Components

### 1. MLflow Tracking Server
- **Purpose**: Tracks model versions, metrics, parameters, and artifacts
- **Type**: Kubernetes Deployment
- **Image**: Official `mlflow/mlflow:latest`
- **Storage**: 
  - Artifacts: Azure Blob Storage (external, unlimited)
  - Metadata: SQLite on PVC (small, ~10-50MB)
- **Access**: Port-forward to local machine for UI

### 2. Trainer CronJob
- **Purpose**: Executes training script on schedule
- **Type**: Kubernetes CronJob
- **Image**: Custom `sihaoz826/movie_trainer:v1.0.0`
- **Schedule**: Configurable (e.g., daily at 2 AM)
- **Storage**: Mounts PVC for model output

### 3. Training Script
- **Purpose**: Trains ALS model and logs to MLflow
- **File**: `retraining/train_als.py`
- **Features**:
  - Trains ALS model from Azure Blob ratings parts (lookback window)
  - Calculates time-period user factors (12 periods)
  - Saves to `inference_2/` (canary release)
  - Logs to MLflow (params, metrics, artifacts, tags)
  - MLflow stores all historical models in Azure Blob
- **Data Source**: Concatenates blob parts under `ratings/incoming/dt=YYYY-MM-DD/` for the last `RATINGS_LOOKBACK_DAYS` UTC dates (default 1: today and yesterday). Fails if blob creds are unset or no parts match.

### 4. Kafka Consumer (Data Pipeline)
- **Purpose**: Continuously consumes ratings from Kafka stream and uploads them to Azure Blob
- **Type**: Kubernetes Deployment (separate from training)
- **Storage**: Uploads new-row part files to Azure Blob (`ratings` container). Does not write ratings to PVC.
- **Features**:
  - Consumes ratings from Kafka topic
  - Buffers unsaved rows in memory (`SAVE_BATCH_SIZE`)
  - On each flush (batch full or SIGTERM), uploads only new rows to `incoming/dt=YYYY-MM-DD/part-<uuid>.csv`
  - Trainer reads blob parts in the lookback window
- **Implementation Pattern**:
  ```python
  # Blob-only flush pattern
  1. Append parsed ratings to an in-memory unsaved batch
  2. When the batch reaches SAVE_BATCH_SIZE, upload it as a new blob part
  3. Clear the in-memory batch (nothing is persisted to PVC)
  4. On SIGTERM/SIGINT, flush any remaining unsaved rows to Blob
  ```
- **Benefits**:
  - Simple: Blob is the only ratings store
  - Safe: Upload failure is fatal, so batches are not silently dropped
  - Effective: Trainer concatenates immutable part files in the lookback window
  - Efficient: RAM is bounded by SAVE_BATCH_SIZE, not a growing on-disk cache

## Files to Create/Update

### New Files

1. **`retraining/train_als.py`**
   - Training script with MLflow integration
   - Converts notebook logic to Python script
   - Saves active model to PVC, logs all versions to MLflow/Azure Blob

2. **`retraining/Dockerfile.trainer`**
   - Docker image for training
   - Based on Python 3.10-slim
   - Installs dependencies and copies training files

3. **`retraining/requirements.txt`**
   - Python dependencies for training
   - Includes: implicit, scipy, numpy, pandas, mlflow

4. **`kubernetes/mlflow-server-deployment.yaml`**
   - MLflow Tracking Server deployment
   - Uses official MLflow image
   - Mounts PVC for SQLite metadata storage
   - Configures Azure Blob Storage for artifacts
   - Exposes port 5000

5. **`kubernetes/trainer-cronjob.yaml`**
   - CronJob for scheduled training
   - References custom training image
   - Mounts PVC for model output
   - Configurable schedule

### Updated Files

None - all files are new additions to the system.

## Docker Images

### 1. Training Image (Build Required)

**File**: `retraining/Dockerfile.trainer`

**Build Command** (from repository root, so the shared blob helper is included):
```bash
docker build -f retraining/Dockerfile.trainer -t sihaoz826/movie_trainer:v1.0.0 .
```

**Push Command**:
```bash
docker push sihaoz826/movie_trainer:v1.0.0
```

**Contents**:
- Python 3.10-slim base
- Dependencies: implicit, scipy, numpy, pandas, mlflow
- Training script: `train_als.py`
- **Note**: Training data is NOT included in the image — the trainer loads ratings from Azure Blob at runtime

### 2. MLflow Server Image (Use Official)

**Image**: `mlflow/mlflow:latest`

**No build needed** - use directly in Kubernetes deployment.

## Kubernetes Resources

### 1. MLflow Server Deployment

**File**: `kubernetes/mlflow-server-deployment.yaml`

**Components**:
- Deployment: Runs MLflow server
- Service: ClusterIP for internal access
- Volume: Mounts PVC for artifact storage
- Port: 5000 (MLflow UI and API)

**Configuration**:
- Backend: SQLite (file-based, stored on PVC)
- Artifact root: Azure Blob Storage container (external)
- Azure Blob credentials: Provided via Kubernetes Secret
- Default experiment: "movie-recommender"

### 2. Trainer CronJob

**File**: `kubernetes/trainer-cronjob.yaml`

**Components**:
- CronJob: Scheduled training job
- Schedule: Configurable (default: daily at 2 AM)
- Concurrency: Forbid (no overlapping runs)
- Job history: Keep 5 successful, 1 failed
- TTL: Delete jobs after 10 minutes

**Configuration**:
- Image: `sihaoz826/movie_trainer:v1.0.0`
- Volume: Mounts PVC at `/shared-volume` (model pickles only; ratings come from Blob)
- Environment variables:
  - `SHARED_VOLUME_PATH`: `/shared-volume`
  - `INFERENCE_INSTANCE`: `inference_2` (canary)
  - `MLFLOW_TRACKING_URI`: `http://mlflow-service:5000`
  - `AZURE_STORAGE_CONNECTION_STRING`: From Kubernetes Secret (MLflow artifacts + ratings blobs)
  - `RATINGS_BLOB_CONTAINER`: `ratings`
  - `RATINGS_BLOB_PREFIX`: `incoming`
  - `RATINGS_LOOKBACK_DAYS`: `1` (UTC dates: today and yesterday)

## Model Storage Structure

### PVC Storage (Limited - Only Active Models)

```
/shared-volume/ (PVC mount point - ~60MB total)
├── inference_1/              # Stable model (never changes)
│   ├── als_model.pkl         # ~20MB
│   └── als_mappings.pkl       # ~5MB
│
├── inference_2/              # Current active canary model
│   ├── als_model.pkl         # ~20MB (updated by trainer)
│   └── als_mappings.pkl      # ~5MB
│
└── mlflow-db/                # MLflow SQLite metadata
    └── mlflow.db             # ~10-50MB (grows slowly)
```

### Azure Blob Storage (Unlimited - All Historical Models)

```
Azure Blob Container: mlflow-artifacts
└── movie-recommender/        # Experiment name
    └── runs/                 # All training runs
        ├── run-001/
        │   ├── als_model.pkl
        │   └── als_mappings.pkl
        ├── run-002/
        │   ├── als_model.pkl
        │   └── als_mappings.pkl
        └── ... (all historical models, unlimited)
```

**Storage Benefits**:
- **PVC**: Small (~60MB) - only active models + metadata
- **Azure Blob**: Unlimited - all historical models + ratings part files
- **No cleanup needed** - Azure Blob scales automatically
- **Training Data**: Ratings live in Blob (`ratings/incoming/dt=YYYY-MM-DD/part-*.csv`), not on PVC

## Data Pipeline: Kafka Consumer Implementation

### Overview

The Kafka consumer buffers parsed ratings in memory and uploads each unsaved batch to Azure Blob. When the trainer CronJob runs, it concatenates blob parts in the lookback window and trains on that snapshot.

### Implementation Pattern

**Kafka Consumer Code Pattern**:

```python
from kafka import KafkaConsumer
from ratings_blob import upload_ratings_part

SAVE_BATCH_SIZE = 1000
unsaved_batch = []

def flush_new_ratings(batch):
    """Upload only the unsaved rows as a new blob part."""
    if not batch:
        return
    upload_ratings_part(batch)

consumer = KafkaConsumer('ratings-topic', ...)
for message in consumer:
    rating = parse_log_line(message.value)
    if rating:
        unsaved_batch.append(rating)
        if len(unsaved_batch) >= SAVE_BATCH_SIZE:
            flush_new_ratings(unsaved_batch)
            unsaved_batch.clear()
```

### Key Design Decisions

1. **Blob-only ratings**: Consumer never writes `ratings_current.csv` to PVC
   - Azure Blob is the source of truth for training data
   - Missing `AZURE_STORAGE_CONNECTION_STRING` fails at startup

2. **In-memory batch only**: Buffer is bounded by `SAVE_BATCH_SIZE`
   - Upload the unsaved batch, then clear it
   - SIGTERM/SIGINT flushes remaining rows to Blob

3. **Immutable part files**: Each flush writes `incoming/dt=YYYY-MM-DD/part-<uuid>.csv`
   - Trainer concatenates parts in the UTC lookback window
   - No coordination needed between consumer and trainer

4. **PVC is for models, not ratings**: Trainer still mounts `model-pvc` for pickles
   - Kafka consumer does not mount PVC

### Kubernetes Deployment

The Kafka consumer is deployed as a separate Deployment (no PVC):

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-consumer
spec:
  replicas: 1
  template:
    spec:
      containers:
      - name: kafka-consumer
        image: your-kafka-consumer:latest
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "kafka-service:9092"
        - name: KAFKA_TOPIC
          value: "ratings-topic"
        - name: SAVE_BATCH_SIZE
          value: "1000"
        - name: AZURE_STORAGE_CONNECTION_STRING
          valueFrom:
            secretKeyRef:
              name: azure-blob-secret
              key: connectionstring
        - name: RATINGS_BLOB_CONTAINER
          value: "ratings"
        - name: RATINGS_BLOB_PREFIX
          value: "incoming"
```

### Benefits of This Approach

- ✅ **Simple**: Blob is the only ratings store
- ✅ **Safe**: Upload failure is fatal; SIGTERM flushes the unsaved batch
- ✅ **Effective**: Trainer concatenates immutable parts in the lookback window
- ✅ **Efficient**: RAM bounded by SAVE_BATCH_SIZE; no PVC CSV cache
- ✅ **No Coordination**: Consumer and trainer work independently

## Canary Release Strategy

### Inference-1 (Stable)
- **Model Path**: `/model_files/inference_1/`
- **Update Policy**: Never updated by trainer
- **Purpose**: Stable baseline for A/B testing
- **Deployment**: Existing `inference-1-deployment.yaml`

### Inference-2 (Canary)
- **Model Path**: `/model_files/inference_2/`
- **Update Policy**: Updated by trainer on schedule
- **Purpose**: Test new models before promoting
- **Deployment**: Existing `inference-2-deployment.yaml`
- **Rollback**: Download any version from MLflow UI (stored in Azure Blob)

## MLflow Tracking

### What Gets Tracked

1. **Parameters**:
   - `factors`: 50
   - `iterations`: 15
   - `regularization`: 0.1
   - `data_size`: Number of ratings
   - `num_users`: Number of unique users
   - `num_items`: Number of unique movies

2. **Metrics**:
   - `training_time_seconds`: Total training time
   - `model_size_mb`: Size of saved model file
   - `matrix_sparsity`: Sparsity of rating matrix
   - `num_users`: Number of users in training data
   - `num_items`: Number of items in training data

3. **Tags**:
   - `model_type`: "ALS"
   - `inference_instance`: "inference_2"
   - `deployment_status`: "canary"
   - `training_trigger`: "scheduled"
   - `training_timestamp`: ISO format timestamp (e.g., "2024-01-15T02:00:00.123456")

4. **Artifacts**:
   - `als_model.pkl`: Trained model
   - `als_mappings.pkl`: User/item mappings

5. **Metadata** (Automatic):
   - Run ID, timestamp, duration, status

### Accessing MLflow UI

1. **Port-forward to local machine**:
   ```bash
   kubectl port-forward svc/mlflow-service 5000:5000
   ```

2. **Open in browser**:
   ```
   http://localhost:5000
   ```

## Setup Steps

### 1. Create Training Files

- [ ] Create `retraining/train_als.py` (with MLflow integration)
- [ ] Create `retraining/Dockerfile.trainer`
- [ ] Create `retraining/requirements.txt`

### 2. Build and Push Training Image

```bash
docker build -f retraining/Dockerfile.trainer -t sihaoz826/movie_trainer:v1.0.0 .
docker push sihaoz826/movie_trainer:v1.0.0
```

### 3. Set Up Azure Blob Storage

#### Prerequisites
- Azure CLI installed and configured
- Access to Azure subscription
- Kubernetes cluster with access to Azure

#### Steps

1. **Create Azure Storage Account** (if not exists):
   ```bash
   az storage account create \
     --name <storage-account-name> \
     --resource-group <resource-group> \
     --location <location> \
     --sku Standard_LRS
   ```
   - Choose a unique storage account name (globally unique)
   - Use existing resource group or create new one
   - Select appropriate location (e.g., `eastus`, `westus2`)

2. **Create Blob Containers**:
   ```bash
   az storage container create \
     --name mlflow-artifacts \
     --account-name <storage-account-name> \
     --auth-mode login
   az storage container create \
     --name ratings \
     --account-name <storage-account-name> \
     --auth-mode login
   ```
   - `mlflow-artifacts`: MLflow model artifacts only
   - `ratings`: Kafka rating part files (`incoming/dt=YYYY-MM-DD/part-*.csv`); the consumer can also create this container once at startup

3. **Get Connection String**:
   ```bash
   az storage account show-connection-string \
     --name <storage-account-name> \
     --resource-group <resource-group> \
     --query connectionString -o tsv
   ```
   - Save this connection string for next step
   - Format: `DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net`

4. **Create Kubernetes Secret**:
   ```bash
   kubectl create secret generic azure-blob-secret \
     --from-literal=connection-string="<connection-string-from-step-3>"
   ```
   - Secret name: `azure-blob-secret`
   - Key: `connection-string`
   - This secret will be mounted in MLflow server and trainer pods

5. **Verify Secret**:
   ```bash
   kubectl get secret azure-blob-secret
   kubectl describe secret azure-blob-secret
   ```

#### Azure Blob URI Format for MLflow

The artifact root URI format for MLflow:
```
wasbs://mlflow-artifacts@<storage-account-name>.blob.core.windows.net/
```

Example:
```
wasbs://mlflow-artifacts@movierecommendations.blob.core.windows.net/
```

This URI will be set in the MLflow server deployment as `MLFLOW_DEFAULT_ARTIFACT_ROOT`.

### 4. Create Kubernetes Manifests

- [ ] Create `kubernetes/mlflow-server-deployment.yaml` (with Azure Blob config)
- [ ] Create `kubernetes/trainer-cronjob.yaml` (with Azure Blob secret)

### 5. Wait for Ratings in Azure Blob

Do not copy a CSV onto the PVC. Training data comes from the Kafka consumer uploading parts to the `ratings` container (`incoming/dt=YYYY-MM-DD/part-*.csv`). Before the first training run, confirm the consumer is running and at least one part exists in the lookback window (`RATINGS_LOOKBACK_DAYS=1`).

The trainer fails with a clear error if the connection string is missing or no blobs match the lookback window.

### 6. Deploy MLflow Server

```bash
kubectl apply -f kubernetes/mlflow-server-deployment.yaml
kubectl get pods -l app=mlflow-server
kubectl port-forward svc/mlflow-service 5000:5000
```

### 7. Deploy Trainer CronJob

```bash
kubectl apply -f kubernetes/trainer-cronjob.yaml
kubectl get cronjobs
```

### 8. Verify Setup

- [ ] Check MLflow server is running: `kubectl get pods -l app=mlflow-server`
- [ ] Check trainer CronJob exists: `kubectl get cronjobs`
- [ ] Access MLflow UI: `http://localhost:5000` (after port-forward)
- [ ] Manually trigger training job (optional):
  ```bash
  kubectl create job --from=cronjob/trainer-cronjob manual-train-$(date +%s)
  ```

## Environment Variables

### Training Script Environment Variables

- `SHARED_VOLUME_PATH`: PVC mount point (default: `/shared-volume`)
- `INFERENCE_INSTANCE`: Target inference instance (default: `inference_2`)
- `MLFLOW_TRACKING_URI`: MLflow server URL (default: `http://mlflow-service:5000`)
- `AZURE_STORAGE_CONNECTION_STRING`: Azure Blob connection string (from Kubernetes Secret; required)
- `RATINGS_BLOB_CONTAINER`: Ratings container (default: `ratings`; not `mlflow-artifacts`)
- `RATINGS_BLOB_PREFIX`: Blob prefix (default: `incoming`)
- `RATINGS_LOOKBACK_DAYS`: Inclusive UTC lookback (default: `1` → today and yesterday)

**Note**: Training data source of truth is Azure Blob (`ratings/incoming/dt=YYYY-MM-DD/part-*.csv`). There is no PVC CSV cache or local-file fallback.

**Data Pipeline**: A Kafka consumer continuously:
1. Consumes ratings from the Kafka stream
2. Buffers unsaved rows in memory (`SAVE_BATCH_SIZE`)
3. Uploads each unsaved batch as a new blob part (including a SIGTERM flush)

The trainer concatenates blob parts in the lookback window, then fits ALS. The Docker image does not include ratings files.

### MLflow Server Environment Variables

- `MLFLOW_BACKEND_STORE_URI`: SQLite path (default: `/shared-volume/mlflow-db/mlflow.db`)
- `MLFLOW_DEFAULT_ARTIFACT_ROOT`: Azure Blob Storage URI (format: `wasbs://<container>@<account>.blob.core.windows.net/`)
- `AZURE_STORAGE_CONNECTION_STRING`: Azure Blob connection string (from Kubernetes Secret)

**Azure Blob URI Format**:
```
wasbs://mlflow-artifacts@<storage-account-name>.blob.core.windows.net/
```

## Model Versioning Strategy

1. **During Training**:
   - Train new model
   - Save active model to `inference_2/` (overwrites current)
   - Log to MLflow (automatically stores in Azure Blob)

2. **After Training**:
   - MLflow stores complete model version in Azure Blob
   - Inference-2 automatically uses new model (reads from `inference_2/`)
   - All historical models remain in Azure Blob (unlimited storage)

3. **Rollback** (if needed):
   - Open MLflow UI: `http://localhost:5000`
   - Find desired model version
   - Download artifacts from Azure Blob
   - Copy to `inference_2/` directory on PVC
   - Restart inference-2 pod (optional, will auto-reload)

**Benefits**:
- ✅ All models stored in Azure Blob (unlimited)
- ✅ No manual cleanup needed
- ✅ Easy rollback via MLflow UI
- ✅ PVC stays small (only active models)

## Resource Requirements

### MLflow Server
- **CPU**: 100m request, 500m limit
- **Memory**: 256Mi request, 512Mi limit
- **Storage**: 
  - PVC: ~10-50MB for SQLite metadata
  - Azure Blob: Unlimited for artifacts (external)

### Trainer CronJob
- **CPU**: 1000m request, 2000m limit (training is CPU-intensive)
- **Memory**: 1Gi request, 2Gi limit (model training needs memory)
- **Storage**: 
  - PVC: ~50MB for active models only
  - Azure Blob: Unlimited for historical models (external)

## Monitoring and Debugging

### Check Training Jobs

```bash
# List all training jobs
kubectl get jobs -l app=model-trainer

# View logs of latest training job
kubectl logs -l app=model-trainer --tail=100

# Check CronJob status
kubectl describe cronjob trainer-cronjob
```

### Check MLflow Server

```bash
# Check MLflow pod status
kubectl get pods -l app=mlflow-server

# View MLflow logs
kubectl logs -l app=mlflow-server --tail=100
```

### Access MLflow UI

```bash
# Port-forward MLflow service
kubectl port-forward svc/mlflow-service 5000:5000

# Open browser
# http://localhost:5000
```

## Future Enhancements

1. **Model Validation**: Add validation metrics before deploying
2. **A/B Testing Metrics**: Track inference-1 vs inference-2 performance
3. **Data Versioning**: Track which data version was used for training
4. **Automated Promotion**: Auto-promote models from canary to stable based on metrics
5. **PostgreSQL Backend**: Upgrade from SQLite to PostgreSQL for production
6. **Azure Blob Lifecycle Policies**: Set up automatic cleanup of old artifacts (if needed)
7. **Model Registry**: Use MLflow Model Registry for staging/production promotion

## Troubleshooting

### Training Job Fails
- Check logs: `kubectl logs -l app=model-trainer`
- Verify PVC is mounted correctly (model pickles)
- Check Azure Blob parts exist in the lookback window
- Verify MLflow server is accessible

### MLflow Server Not Accessible
- Check pod status: `kubectl get pods -l app=mlflow-server`
- Check service: `kubectl get svc mlflow-service`
- Verify PVC is mounted (for SQLite)
- Verify Azure Blob Secret exists: `kubectl get secret azure-blob-secret`
- Check Azure Blob connection string is correct
- Check logs: `kubectl logs -l app=mlflow-server`

### Azure Blob Connection Issues
- Verify connection string in secret: `kubectl get secret azure-blob-secret -o jsonpath='{.data.connection-string}' | base64 -d`
- Test Azure Blob access from pod
- Verify storage account and container exist
- Check network connectivity from cluster to Azure

### Models Not Updating
- Verify trainer CronJob is running: `kubectl get cronjobs`
- Check if jobs are being created: `kubectl get jobs`
- Verify PVC has write permissions
- Check training script logs

## Summary

This retraining system provides:
- ✅ Automated scheduled model retraining
- ✅ MLflow integration for tracking and versioning
- ✅ Canary release strategy (inference-1 stable, inference-2 updated)
- ✅ Azure Blob Storage for unlimited model versioning
- ✅ Small PVC footprint (only active models + metadata)
- ✅ Easy rollback capability via MLflow UI
- ✅ Comprehensive tracking (params, metrics, artifacts)
- ✅ Production-ready scalable architecture

**Storage Architecture**:
- **PVC (1Gi)**: Active models (~50MB) + MLflow metadata (~50MB)
- **Azure Blob**: All historical models (unlimited) + ratings part files

All components work together to provide a robust, production-ready model retraining pipeline with unlimited model versioning.

