#!/usr/bin/env python3
"""
Kafka consumer that only logs rating events from movielog1 topic
"""

from kafka import KafkaConsumer
import sys
import re
import os
import gc
import signal

# ratings_blob.py is copied next to this script in Docker; locally it lives in ../shared
_here = os.path.dirname(os.path.abspath(__file__))
for _p in (_here, os.path.normpath(os.path.join(_here, "..", "shared"))):
    if _p not in sys.path:
        sys.path.insert(0, _p)
from ratings_blob import blob_configured, ensure_ratings_container, upload_ratings_part

# Configuration: Kafka connection (from environment or default)
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'movielog1')

# Configuration: Batch size for saving
SAVE_BATCH_SIZE = int(os.getenv('SAVE_BATCH_SIZE', '1000'))

def parse_log_line(line):
    """Parse a log line and extract rating data"""
    try:
        parts = line.split(',', 2)
        if len(parts) < 3:
            return None
        
        timestamp, userid, action = parts
        
        # Only parse rating events
        if 'GET /rate/' in action:
            return parse_rating(timestamp, userid, action)
        
        return None
    except Exception as e:
        print(f"Error parsing line: {line[:100]}... Error: {e}")
        return None

def parse_rating(timestamp, userid, action):
    """Parse rating event: GET /rate/<movieid>=<rating>"""
    try:
        match = re.search(r'/rate/([^=]+)=(\d+)', action)
        if match:
            movieid, rating = match.groups()
            
            return {
                'timestamp': timestamp,
                'userid': userid,
                'movieid': movieid,
                'rating': int(rating)
            }
    except Exception as e:
        print(f"Error parsing rating: {e}")
    return None

def flush_new_ratings(unsaved_batch):
    """Flush unsaved rows to Azure Blob. Uploads ONLY the new rows as a new blob part.

    Upload failure is fatal so we do not silently drop data.
    """
    if not unsaved_batch:
        return
    upload_ratings_part(unsaved_batch)

def main():
    # New rows since last successful flush (uploaded as their own blob part)
    unsaved_batch = []
    flushed = {"done": False}
    stopping = {"requested": False}
    consumer = None

    if not blob_configured():
        print("AZURE_STORAGE_CONNECTION_STRING is required; ratings are uploaded to Azure Blob only.")
        sys.exit(1)

    ensure_ratings_container()
    print("Azure Blob ratings upload enabled "
          f"(container={os.getenv('RATINGS_BLOB_CONTAINER', 'ratings')}, "
          f"prefix={os.getenv('RATINGS_BLOB_PREFIX', 'incoming')})")

    def flush_and_commit():
        """Upload pending rows, then commit Kafka offsets. No-op if the batch is empty.

        At-least-once: commit only after a successful Blob PUT. Upload failure
        raises and leaves offsets uncommitted so Kafka redelivers.
        """
        if not unsaved_batch:
            return
        flush_new_ratings(unsaved_batch)
        consumer.commit()
        unsaved_batch.clear()

    def flush_pending():
        if flushed["done"]:
            return
        flush_and_commit()
        flushed["done"] = True

    def handle_stop(signum, frame):
        # Kubernetes sends SIGTERM. Upload leftover rows and commit while the
        # consumer is still open, then close so the poll loop unblocks.
        # close(autocommit=False): never commit unless the PUT already succeeded.
        print(f"\nReceived signal {signum}, flushing pending ratings...")
        stopping["requested"] = True
        try:
            flush_pending()
        except Exception as e:
            print(f"Flush/commit on signal failed: {e}")
        if consumer is not None:
            try:
                consumer.close(autocommit=False)
            except Exception:
                pass

    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)
    
    # Kafka configuration from environment variables
    bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS.split(',')  # Support multiple servers
    topic = KAFKA_TOPIC
    
    print(f"Connecting to Kafka at {bootstrap_servers[0]}...")
    print(f"Consuming from topic: {topic}")
    print("Only logging rating events...")
    print(f"Batch save size: {SAVE_BATCH_SIZE} messages")
    print("Press Ctrl+C to stop\n")

    failed = False
    try:
        # Create consumer
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='latest',  # Start from latest
            enable_auto_commit=False,  # commit only after a successful Blob PUT
            group_id='my-consumer-group',
            value_deserializer=lambda x: x.decode('utf-8') if x else None
        )
        
        # Consume messages
        for message in consumer:
            if stopping["requested"]:
                break
            # Parse the log line - only processes rating events
            parsed_data = parse_log_line(message.value)
            if parsed_data:
                print(f"Rating Event:")
                print(f"  Topic: {message.topic}")
                print(f"  Partition: {message.partition}")
                print(f"  Offset: {message.offset}")
                for key, value in parsed_data.items():
                    print(f"  {key}: {value}")
                
                unsaved_batch.append(parsed_data)
                
                # Batch save: only upload every SAVE_BATCH_SIZE messages
                if len(unsaved_batch) >= SAVE_BATCH_SIZE:
                    flush_and_commit()
                    # Suggest garbage collection after batch save to free memory
                    # (Python's GC will handle this automatically, but explicit call can help)
                    gc.collect()
                
                print("-" * 50)
            
    except KeyboardInterrupt:
        print("\nStopping consumer...")
        stopping["requested"] = True
    except Exception as e:
        if stopping["requested"]:
            print("\nStopping consumer...")
        else:
            print(f"Error: {e}")
            failed = True
    finally:
        try:
            flush_pending()
        except Exception as e:
            print(f"Flush/commit on shutdown failed: {e}")
            failed = True
        if consumer:
            try:
                consumer.close(autocommit=False)
            except Exception:
                pass
    if failed:
        sys.exit(1)

if __name__ == "__main__":
    main()
