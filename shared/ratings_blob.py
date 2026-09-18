"""Azure Blob helpers for streaming ratings part files.

Container: RATINGS_BLOB_CONTAINER (default ``ratings``), not mlflow-artifacts.
Blob name: {prefix}/dt=YYYY-MM-DD/part-{uuid}.csv
CSV columns: timestamp, userid, movieid, rating

Lookback (UTC calendar dates, inclusive):
    include blobs where dt= date >= utcnow().date() - timedelta(days=lookback_days)
    RATINGS_LOOKBACK_DAYS=1 therefore includes today AND yesterday, so a CronJob
    shortly after midnight UTC still sees the previous day's parts.
"""

import io
import os
import re
import uuid
from datetime import datetime, timedelta, timezone

import pandas as pd
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient

RATINGS_COLUMNS = ["timestamp", "userid", "movieid", "rating"]
DT_IN_BLOB_NAME = re.compile(r"dt=(\d{4}-\d{2}-\d{2})")


def blob_configured():
    """True when AZURE_STORAGE_CONNECTION_STRING is set (cluster); false for local/dev."""
    return bool(os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "").strip())


def ratings_container_name():
    return os.environ.get("RATINGS_BLOB_CONTAINER", "ratings")


def ratings_blob_prefix():
    return os.environ.get("RATINGS_BLOB_PREFIX", "incoming").strip("/")


def ratings_lookback_days():
    return int(os.environ.get("RATINGS_LOOKBACK_DAYS", "1"))


def _container_client():
    connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    service = BlobServiceClient.from_connection_string(connection_string)
    return service.get_container_client(ratings_container_name())


def ensure_ratings_container():
    """Create the ratings container once at startup if it does not exist."""
    client = _container_client()
    try:
        client.create_container()
        print(f"[BLOB] Created container '{ratings_container_name()}'")
    except ResourceExistsError:
        pass
    return client


def upload_ratings_part(rows):
    """Upload only the new rows as a new blob. Raises on failure.

    ``rows`` is a list of rating dicts (or a DataFrame) with the standard columns.
    """
    if rows is None:
        return None
    if isinstance(rows, pd.DataFrame):
        df = rows
    else:
        if len(rows) == 0:
            return None
        df = pd.DataFrame(rows)

    if df.empty:
        return None

    df = df[RATINGS_COLUMNS]
    dt = datetime.now(timezone.utc).date().isoformat()
    blob_name = f"{ratings_blob_prefix()}/dt={dt}/part-{uuid.uuid4()}.csv"

    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)

    client = _container_client()
    client.upload_blob(name=blob_name, data=buf, overwrite=False)
    print(f"[BLOB] Uploaded {len(df)} new ratings to {ratings_container_name()}/{blob_name}")
    return blob_name


def _blob_dt_date(blob_name):
    match = DT_IN_BLOB_NAME.search(blob_name)
    if not match:
        return None
    return datetime.strptime(match.group(1), "%Y-%m-%d").date()


def download_ratings_lookback():
    """Download and concatenate part files whose dt= is in the lookback window.

    Returns a DataFrame (possibly empty if no matching blobs). Azure errors raise.
    """
    lookback_days = ratings_lookback_days()
    # Inclusive UTC window: today and the previous lookback_days calendar dates.
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=lookback_days)
    prefix = ratings_blob_prefix() + "/"
    client = _container_client()

    frames = []
    matched = 0
    listed = 0
    for blob in client.list_blobs(name_starts_with=prefix):
        listed += 1
        dt = _blob_dt_date(blob.name)
        if dt is None or dt < cutoff:
            continue
        matched += 1
        payload = client.download_blob(blob.name).readall()
        part = pd.read_csv(io.BytesIO(payload))
        frames.append(part)

    print(
        f"[BLOB] Listed {listed} blobs under {ratings_container_name()}/{prefix}; "
        f"{matched} within lookback_days={lookback_days} (dt >= {cutoff.isoformat()})"
    )
    if not frames:
        return pd.DataFrame(columns=RATINGS_COLUMNS)
    return pd.concat(frames, ignore_index=True)
