# Evaluation Monitor

Online evaluation monitor for movie recommendation models using Prometheus and Grafana.

## Overview

This service monitors model performance by:
1. Consuming rating events from Kafka stream (ground truth)
2. Computing predictions using the loaded model
3. Calculating MAE (Mean Absolute Error) and MSE (Mean Squared Error)
4. Exposing Prometheus metrics for scraping

## Features

- **Dual Model Monitoring**: Separate instances for inference_1 and inference_2
- **Automatic Model Reloading**: inference_2 monitor automatically reloads when trainer updates the model
- **Prometheus Integration**: Exposes `/metrics` endpoint with labeled metrics
- **Kafka Consumer**: Consumes rating events from `movielog1` topic
- **Health Checks**: `/health` endpoint for Kubernetes probes

## Metrics Exposed

- `model_mae{inference_instance="inference-1"}` - Mean Absolute Error
- `model_mae{inference_instance="inference-2"}` - Mean Absolute Error
- `model_mse{inference_instance="inference-1"}` - Mean Squared Error
- `model_mse{inference_instance="inference-2"}` - Mean Squared Error
- `model_total_predictions{inference_instance="..."}` - Total predictions evaluated
- `model_prediction_error{inference_instance="..."}` - Error distribution histogram
- `model_reloads_total{inference_instance="..."}` - Number of model reloads
- `model_load_timestamp{inference_instance="..."}` - Last model load timestamp

## Building the Docker Image

```bash
cd traning_and_inference/evaluation_monitor
docker build -f Dockerfile.evaluationmonitor -t sihaoz826/evaluation-monitor:latest .
docker push sihaoz826/evaluation-monitor:latest
```

## Deployment

Deploy both evaluation monitors:

```bash
kubectl apply -f ../kubernetes/evaluation-monitor-1-deployment.yaml
kubectl apply -f ../kubernetes/evaluation-monitor-2-deployment.yaml
```

## Configuration

### Environment Variables

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker address (default: `localhost:9092`)
- `KAFKA_TOPIC`: Kafka topic to consume (default: `movielog1`)
- `INFERENCE_INSTANCE`: Model instance to monitor (`inference_1` or `inference_2`)
- `MODEL_RELOAD_CHECK_INTERVAL`: Seconds between model reload checks (default: `30`)
- `HOST`: Flask host (default: `0.0.0.0`)
- `PORT`: Flask port (default: `8084`)

### Model Paths

Models are loaded from:
- `/model_files/inference_1/als_model.pkl`
- `/model_files/inference_2/als_mappings.pkl`

These paths are mounted from the `model-pvc` PersistentVolumeClaim.

## Testing

### Check Health

```bash
kubectl port-forward deployment/evaluation-monitor-1 8084:8084
curl http://localhost:8084/health
```

### Check Metrics

```bash
kubectl port-forward deployment/evaluation-monitor-1 8084:8084
curl http://localhost:8084/metrics
```

## Architecture

```
Kafka Stream (movielog1)
    ↓
Evaluation Monitor Pods
    ├── evaluation-monitor-1 (monitors inference_1)
    └── evaluation-monitor-2 (monitors inference_2)
    ↓
Prometheus (scrapes /metrics endpoints)
    ↓
Grafana (visualizes metrics)
```

## Notes

- **inference_1**: Stable model, no automatic reloading
- **inference_2**: Canary model, automatically reloads when trainer cronjob updates it
- Each monitor uses a separate Kafka consumer group to avoid conflicts
- Metrics are labeled with `inference_instance` for easy comparison in Grafana

