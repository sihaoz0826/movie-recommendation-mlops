# Movie Recommendation MLOps Platform

An end-to-end, production-style movie recommendation system that trains, serves, monitors, and
automatically retrains a collaborative-filtering model under realistic streaming load. Built to
explore what it takes to run a recommender as a real, always-on service rather than a notebook.

> **Deep-dive highlight —** the **autoscaled, zero-downtime serving path**: stateless inference
> replicas sit behind a load balancer doing **weighted canary routing**, autoscale on Kubernetes
> (HPA), and pick up newly retrained models via a **rolling restart** — so new versions ship with no
> downtime and traffic can be shifted gradually. This orchestration + rollout flow is the part most
> relevant to running live services.

## Highlights

- **ALS collaborative-filtering model** (`implicit`, alternating least squares) with time-of-day
  user factors and cold-start fallback tiers for new users.
- **Streaming data pipeline** — a Kafka consumer ingests live rating events, validates them, and
  accumulates them for retraining.
- **Distributed inference serving** — multiple stateless inference replicas behind a load balancer
  with **weighted canary routing**, autoscaled on Kubernetes (HPA).
- **Automated retraining & zero-downtime deploys** — scheduled retraining writes versioned models,
  logs them to **MLflow**, and triggers a Kubernetes rolling restart to pick up the new model.
- **Observability** — Prometheus metrics and Grafana dashboards for prediction quality, data drift,
  and inference health.
- **React dashboard** for visualizing A/B traffic split and routing.

## Architecture

```
        Kafka (rating events)
                │
                ▼
      kafka_consumer  ──►  accumulated ratings ──►  retraining (ALS + MLflow)
                                                          │ rolling restart
                                                          ▼
  client ──► load_balancer ──►  inference-1 / inference-2 (model replicas, HPA)
                │                         │
                ▼                         ▼
            dashboard              evaluation_monitor ──► Prometheus / Grafana
```

## Components

| Directory | What it does |
|---|---|
| `inference/` | Model server: loads the ALS model, serves recommendations, exposes health/metrics |
| `load_balancer/` | Weighted/canary router across inference replicas; load-test scripts |
| `kafka_consumer/` | Consumes and cleans streaming rating events |
| `retraining/` | ALS training with MLflow tracking + Kubernetes rolling restart |
| `evaluation_monitor/` | Online evaluation + data-drift monitoring, Prometheus metrics |
| `mlflow/` | MLflow tracking server deployment |
| `kubernetes/` | Deployments, HPA, CronJob, PVC, and service manifests |
| `frontend/` | React dashboard for A/B routing visualization |
| `dashboard_api/` | Backend API for the dashboard |
| `database/` | Routing/telemetry logging |

## Key code to look at

The autoscaled, zero-downtime serving path:

| File | Lines | What it is |
|---|---|---|
| `load_balancer/load_balancer.py` | `240-252` | `select_backend()` — weighted random routing (the canary split) |
| `load_balancer/load_balancer.py` | `81-104` | Routing weights from env vars (default 90/10) — shift traffic without code changes |
| `load_balancer/load_balancer.py` | `346-370` | Health endpoint — returns 503 when a backend is down |
| `kubernetes/inference-1-hpa.yaml` | `1-42` | HPA autoscaling on CPU 70% / mem 80%, with scale-up/down behavior tuning |
| `retraining/train_als.py` | `14-58` | `restart_inference_deployment()` — patches `restartedAt` to trigger a zero-downtime rolling restart |
| `retraining/train_als.py` | `234-240` | Where retraining fires the rolling restart after logging the new model |

## Tech Stack

Python · implicit (ALS) · Kafka · Flask · Docker · Kubernetes (Deployments, HPA, CronJob, PVC) ·
MLflow · Prometheus · Grafana · React

## Notes

Model files and bundled datasets are intentionally excluded from the repo; the pipeline reads data
and writes model artifacts through configurable paths / persistent volumes at runtime.
