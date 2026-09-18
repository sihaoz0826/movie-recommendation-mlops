#!/usr/bin/env python3
"""
Load testing script to trigger HPA (Horizontal Pod Autoscaler) scaling for inference-1 service.

This script sends concurrent API requests to generate load and trigger horizontal pod autoscaling.
It monitors pod count and HPA status in real-time to demonstrate automatic scaling behavior.

Usage:
    # Get node IP first
    kubectl get nodes -o wide
    
    # Run load test (replace <NODE_IP> with actual node IP)
    python test_horizontal_scaling.py --url http://<NODE_IP>:30080
    
    # Or use port-forward
    kubectl port-forward svc/load-balancer-service 8080:8080
    python test_horizontal_scaling.py --url http://localhost:8080
"""

import argparse
import requests
import subprocess
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional
import sys
from datetime import datetime
import signal


class LoadTester:
    def __init__(self, base_url: str, num_threads: int = 10, duration: int = 300):
        self.base_url = base_url.rstrip('/')
        self.num_threads = num_threads
        self.duration = duration
        self.request_count = 0
        self.error_count = 0
        self.success_count = 0
        self.lock = threading.Lock()
        self.running = True
        self.start_time = None
        self.interrupted = False  # Add this flag
        
    def send_request(self, user_id: int) -> bool:
        """Send a single recommendation request."""
        if not self.running or self.interrupted:
            return False
        try:
            url = f"{self.base_url}/recommend/{user_id}"
            # Use shorter timeout - requests will timeout quickly, generating load but not blocking
            response = requests.get(url, timeout=2)  # Short timeout to avoid blocking
            with self.lock:
                if not self.running or self.interrupted:
                    return False
                self.request_count += 1
                if response.status_code == 200:
                    self.success_count += 1
                else:
                    self.error_count += 1
            return response.status_code == 200
        except requests.exceptions.Timeout:
            # Timeout is expected - this still generates load on the server
            with self.lock:
                if not self.running or self.interrupted:
                    return False
                self.request_count += 1
                # Don't count timeouts as errors - they're expected for load generation
            return False
        except Exception as e:
            with self.lock:
                if not self.running or self.interrupted:
                    return False
                self.request_count += 1
                self.error_count += 1
            return False
    
    def worker(self, user_id_start: int):
        """Worker thread that continuously sends requests."""
        user_id = user_id_start
        while self.running and not self.interrupted:
            self.send_request(user_id)
            user_id += 1
            if not self.running or self.interrupted:
                break
            time.sleep(0.1)
    
    def get_pod_count(self) -> int:
        """Get current number of running inference-1 pods."""
        try:
            result = subprocess.run(
                ['kubectl', 'get', 'pods', '-l', 'app=inference-1', '--no-headers'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                # Count running pods
                lines = [l for l in result.stdout.strip().split('\n') if l and 'Running' in l]
                return len(lines)
            return 0
        except Exception:
            return -1
    
    def get_hpa_status(self) -> str:
        """Get HPA current replicas and max replicas."""
        try:
            result = subprocess.run(
                ['kubectl', 'get', 'hpa', 'inference-1-hpa', '-o', 'jsonpath={.status.currentReplicas}/{.spec.maxReplicas}'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                return result.stdout.strip()
            return "unknown"
        except Exception:
            return "unknown"
    
    def get_hpa_metrics(self) -> str:
        """Get HPA CPU and memory metrics."""
        try:
            # Get CPU utilization
            cpu_result = subprocess.run(
                ['kubectl', 'get', 'hpa', 'inference-1-hpa', '-o', 'jsonpath={.status.currentMetrics[0].resource.current.averageUtilization}'],
                capture_output=True,
                text=True,
                timeout=5
            )
            cpu = cpu_result.stdout.strip() if cpu_result.returncode == 0 else "?"
            
            # Get Memory utilization
            mem_result = subprocess.run(
                ['kubectl', 'get', 'hpa', 'inference-1-hpa', '-o', 'jsonpath={.status.currentMetrics[1].resource.current.averageUtilization}'],
                capture_output=True,
                text=True,
                timeout=5
            )
            mem = mem_result.stdout.strip() if mem_result.returncode == 0 else "?"
            
            return f"CPU:{cpu}% MEM:{mem}%"
        except Exception:
            return "unknown"
    
    def monitor_status(self):
        """Monitor pod count and HPA status in a separate thread."""
        last_pod_count = 0
        while self.running:
            pod_count = self.get_pod_count()
            hpa_status = self.get_hpa_status()
            metrics = self.get_hpa_metrics()
            
            with self.lock:
                req_count = self.request_count
                err_count = self.error_count
                succ_count = self.success_count
                elapsed = time.time() - self.start_time if self.start_time else 0
            
            # Print status update
            status_line = (
                f"[{int(elapsed)}s] Pods: {pod_count} | HPA: {hpa_status} | "
                f"Metrics: {metrics} | Requests: {req_count} (✓{succ_count} ✗{err_count})"
            )
            
            # Highlight when pod count changes
            if pod_count != last_pod_count:
                print(f"\n🔄 SCALING EVENT: Pod count changed from {last_pod_count} to {pod_count}")
                last_pod_count = pod_count
            else:
                print(f"\r{status_line}", end='', flush=True)
            
            time.sleep(5)  # Update every 5 seconds
    
    def run(self):
        """Run the load test."""
        print("=" * 80)
        print("HPA Horizontal Scaling Load Test")
        print("=" * 80)
        print(f"Load Balancer URL: {self.base_url}")
        print(f"Concurrent Threads: {self.num_threads}")
        print(f"Test Duration: {self.duration} seconds")
        print(f"Endpoint: /recommend/<userid>")
        print()
        print("Monitoring HPA scaling in real-time...")
        print("Press Ctrl+C to stop early")
        print("=" * 80)
        print()
        
        self.start_time = time.time()
        
        # Set up signal handler for Ctrl+C
        def signal_handler(sig, frame):
            print("\n\n⚠️  Interrupt received, stopping load test...")
            self.running = False
            self.interrupted = True
            sys.exit(0)  # Force exit
        
        signal.signal(signal.SIGINT, signal_handler)
        
        # Start monitoring thread
        monitor_thread = threading.Thread(target=self.monitor_status, daemon=True)
        monitor_thread.start()
        
        # Start load generation
        try:
            with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
                futures = []
                for i in range(self.num_threads):
                    future = executor.submit(self.worker, user_id_start=1000 + i * 1000)
                    futures.append(future)
                
                # Run for specified duration
                end_time = self.start_time + self.duration
                while time.time() < end_time and self.running:
                    time.sleep(0.5)
                    if self.interrupted:
                        break
                
                # Time's up - stop everything
                print(f"\n⏰ Duration ({self.duration}s) reached, stopping...")
                self.running = False
                self.interrupted = True
                
                # Shutdown executor immediately
                executor.shutdown(wait=False, cancel_futures=True)
                
        except KeyboardInterrupt:
            print("\n\n⚠️  Load test interrupted by user")
            self.running = False
            self.interrupted = True
        except Exception as e:
            print(f"\n❌ Error: {e}")
            self.running = False
            self.interrupted = True
        
        # Final status
        elapsed = time.time() - self.start_time
        print("\n\n" + "=" * 80)
        print("Load Test Summary")
        print("=" * 80)
        print(f"Duration: {elapsed:.1f} seconds")
        print(f"Total Requests: {self.request_count}")
        print(f"Successful: {self.success_count}")
        print(f"Errors: {self.error_count}")
        if self.request_count > 0:
            success_rate = (self.success_count / self.request_count) * 100
            print(f"Success Rate: {success_rate:.2f}%")
        
        # Final pod count and HPA status
        final_pods = self.get_pod_count()
        final_hpa = self.get_hpa_status()
        print(f"\nFinal Status:")
        print(f"  Pod Count: {final_pods}")
        print(f"  HPA Status: {final_hpa}")
        print("=" * 80)


def verify_prerequisites():
    """Verify that kubectl and HPA are available."""
    # Verify kubectl is available
    try:
        subprocess.run(['kubectl', 'version', '--client'], 
                     capture_output=True, check=True, timeout=5)
    except Exception:
        print("❌ Error: kubectl not found or not accessible")
        print("   Please ensure kubectl is installed and configured")
        return False
    
    # Verify HPA exists
    try:
        result = subprocess.run(
            ['kubectl', 'get', 'hpa', 'inference-1-hpa'],
            capture_output=True,
            timeout=5
        )
        if result.returncode != 0:
            print("❌ Error: HPA 'inference-1-hpa' not found")
            print("   Please ensure the HPA is deployed:")
            print("   kubectl apply -f kubernetes/inference-1-hpa.yaml")
            return False
    except Exception as e:
        print(f"❌ Error checking HPA: {e}")
        return False
    
    # Verify load balancer service exists
    try:
        result = subprocess.run(
            ['kubectl', 'get', 'svc', 'load-balancer-service'],
            capture_output=True,
            timeout=5
        )
        if result.returncode != 0:
            print("⚠️  Warning: load-balancer-service not found")
            print("   Make sure the load balancer is deployed")
    except Exception:
        pass
    
    return True


def main():
    parser = argparse.ArgumentParser(
        description='Load test to trigger HPA horizontal pod autoscaling',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Using port-forward (recommended)
  kubectl port-forward svc/load-balancer-service 8080:8080
  python test_horizontal_scaling.py --url http://localhost:8080

  # Using NodePort (get node IP first)
  kubectl get nodes -o wide
  python test_horizontal_scaling.py --url http://<NODE_IP>:30080

  # High load test (more threads, longer duration)
  python test_horizontal_scaling.py --threads 30 --duration 600
        """
    )
    parser.add_argument('--url', type=str, default='http://localhost:8080',
                       help='Load balancer URL (default: http://localhost:8080)')
    parser.add_argument('--threads', type=int, default=10,
                       help='Number of concurrent threads (default: 10)')
    parser.add_argument('--duration', type=int, default=300,
                       help='Test duration in seconds (default: 300)')
    
    args = parser.parse_args()
    
    # Verify prerequisites
    if not verify_prerequisites():
        sys.exit(1)
    
    # Test connectivity to load balancer
    try:
        response = requests.get(f"{args.url}/health", timeout=10)
        response.raise_for_status()
        print("✓ Load balancer is accessible")
    except Exception as e:
        print(f"❌ Error: Cannot connect to load balancer at {args.url}")
        print(f"   Error: {e}")
        print("\n   Make sure:")
        print("   1. Load balancer service is running")
        print("   2. Port forwarding is set up: kubectl port-forward svc/load-balancer-service 8080:8080")
        print("   3. Or use NodePort with correct node IP")
        sys.exit(1)
    
    tester = LoadTester(args.url, args.threads, args.duration)
    
    try:
        tester.run()
    except KeyboardInterrupt:
        print("\n\n⚠️  Load test interrupted by user")
        tester.running = False
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()

