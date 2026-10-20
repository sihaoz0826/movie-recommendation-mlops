# How to Access MLflow UI

This guide explains how to access the MLflow UI running in your Kubernetes cluster.

## Prerequisites

- Kubernetes cluster running with MLflow server deployed
- `kubectl` configured to access your cluster
- MLflow server pod should be running

## Steps to Access MLflow UI

### 1. Check MLflow Server Status

First, verify that the MLflow server is running:

```powershell
kubectl get pods -l app=mlflow-server
```

You should see a pod with status `1/1 Running`. If not, check the logs:

```powershell
kubectl logs -l app=mlflow-server --tail=50
```

### 2. Port-Forward to MLflow Service

Port-forward the MLflow service to your local machine:

```powershell
kubectl port-forward svc/mlflow-service 5000:5000
```

This command:
- Forwards port 5000 from the MLflow service to port 5000 on your local machine
- Keeps running until you stop it (Ctrl+C)

**Note**: Keep this terminal window open while you want to access the UI.

### 3. Access MLflow UI in Browser

Open your web browser and navigate to:

```
http://localhost:5000
```

You should see the MLflow UI with:
- List of experiments
- Training runs
- Metrics, parameters, and artifacts
- Model versions stored in Azure Blob Storage

## Troubleshooting

### Port Already in Use

If port 5000 is already in use, use a different local port:

```powershell
kubectl port-forward svc/mlflow-service 5001:5000
```

Then access at `http://localhost:5001`

### Connection Refused

If you see connection errors:

1. **Check if the pod is running:**
   ```powershell
   kubectl get pods -l app=mlflow-server
   ```

2. **Check MLflow server logs:**
   ```powershell
   kubectl logs -l app=mlflow-server --tail=100
   ```

3. **Verify the service exists:**
   ```powershell
   kubectl get svc mlflow-service
   ```

### Artifacts Not Loading

If artifacts show "Failed to fetch":

1. **Check if Azure SDK is installed** (should be in custom MLflow image):
   ```powershell
   kubectl exec -it <mlflow-pod-name> -- pip list | grep azure
   ```

2. **Verify Azure Blob Storage connection string** is set:
   ```powershell
   kubectl get secret azure-blob-secret
   ```

3. **Check MLflow server logs** for Azure-related errors:
   ```powershell
   kubectl logs -l app=mlflow-server | Select-String -Pattern "azure|blob|error" -Context 3
   ```

## Alternative: Access via Service URL (if using LoadBalancer)

If you've configured the MLflow service as a LoadBalancer type, you can access it directly via the external IP:

```powershell
kubectl get svc mlflow-service
```

Look for the `EXTERNAL-IP` and access it at `http://<EXTERNAL-IP>:5000`

## Quick Reference

```powershell
# Check MLflow status
kubectl get pods -l app=mlflow-server

# Port-forward to local machine
kubectl port-forward svc/mlflow-service 5000:5000

# View logs
kubectl logs -l app=mlflow-server --tail=100

# Access UI
# Open browser: http://localhost:5000
```

## What You Can Do in MLflow UI

Once connected, you can:

- **View Experiments**: See all your training experiments
- **Browse Runs**: Click on runs to see details
- **View Metrics**: Training time, model size, sparsity, etc.
- **View Parameters**: Model hyperparameters (factors, iterations, regularization)
- **Download Artifacts**: Download model files (`als_model.pkl`, `als_mappings.pkl`)
- **Compare Runs**: Compare different training runs side-by-side
- **Search Runs**: Filter runs by metrics, parameters, or tags

## Stopping Port-Forward

To stop the port-forward, press `Ctrl+C` in the terminal where it's running.

