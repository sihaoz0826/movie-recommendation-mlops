#!/usr/bin/env python3
"""
Test script to verify weighted load balancer distribution.

This script:
1. Checks the configured weights from the health endpoint
2. Makes multiple requests to the load balancer
3. Parses load balancer logs to determine which backend each request was routed to
4. Calculates actual distribution and compares to expected weights
5. Provides statistical analysis of the distribution

Usage:
    # For Kubernetes deployment (uses kubectl to get logs)
    python test_weighted_load_balancer.py --load-balancer-url http://localhost:8080 --use-kubectl
    
    # For local deployment (reads logs from file)
    python test_weighted_load_balancer.py --load-balancer-url http://localhost:8080 --log-file /path/to/logs
    
    # Quick test with default settings
    python test_weighted_load_balancer.py
"""

import argparse
import requests
import subprocess
import re
import time
import sys
from collections import Counter
from typing import Dict, Tuple, Optional
import json


def get_configured_weights(load_balancer_url: str) -> Tuple[float, float]:
    """
    Get configured weights from the load balancer health endpoint.
    
    Args:
        load_balancer_url: Base URL of the load balancer
        
    Returns:
        Tuple of (inference_1_weight, inference_2_weight)
    """
    try:
        response = requests.get(f"{load_balancer_url}/health", timeout=10)
        response.raise_for_status()
        data = response.json()
        
        weights = data.get('routing_weights', {})
        weight_1 = weights.get('inference_1', 90)
        weight_2 = weights.get('inference_2', 10)
        
        print(f"✓ Load balancer health check successful")
        print(f"  Configured weights: inference-1={weight_1}%, inference-2={weight_2}%")
        return weight_1, weight_2
    except Exception as e:
        print(f"✗ Failed to get weights from health endpoint: {e}")
        print("  Using default weights: inference-1=90%, inference-2=10%")
        return 90.0, 10.0


def make_test_requests(load_balancer_url: str, num_requests: int, timeout: int = 5) -> None:
    """
    Make test requests to the load balancer.
    
    Args:
        load_balancer_url: Base URL of the load balancer
        num_requests: Number of requests to make
        timeout: Timeout for each request (short timeout since we just want routing, not full response)
    """
    print(f"\n📤 Making {num_requests} test requests to load balancer...")
    
    # Use a thread pool for concurrent requests to speed up testing
    import concurrent.futures
    
    def make_request(user_id: int) -> Optional[int]:
        """Make a single request and return user_id if successful."""
        try:
            # Use a short timeout since we only care about routing, not waiting for full inference
            response = requests.get(
                f"{load_balancer_url}/recommend/{user_id}",
                timeout=timeout
            )
            return user_id
        except requests.exceptions.Timeout:
            # Timeout is expected since inference takes 80-135s, but routing happens immediately
            return user_id
        except Exception as e:
            print(f"  Warning: Request for user {user_id} failed: {e}")
            return None
    
    # Make requests concurrently (but not too many to avoid overwhelming)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(make_request, i) for i in range(1000, 1000 + num_requests)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]
    
    successful = sum(1 for r in results if r is not None)
    print(f"  ✓ Completed {successful}/{num_requests} requests")
    
    # Give load balancer a moment to log all requests
    time.sleep(2)


def get_logs_via_kubectl() -> str:
    """
    Get load balancer logs using kubectl.
    
    Returns:
        Log output as string
    """
    try:
        result = subprocess.run(
            ['kubectl', 'logs', '-l', 'app=load-balancer', '--tail=1000'],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            return result.stdout
        else:
            print(f"✗ kubectl command failed: {result.stderr}")
            return ""
    except FileNotFoundError:
        print("✗ kubectl not found. Please install kubectl or use --log-file option")
        return ""
    except Exception as e:
        print(f"✗ Error running kubectl: {e}")
        return ""


def get_logs_from_file(log_file: str) -> str:
    """
    Read logs from a file.
    
    Args:
        log_file: Path to log file
        
    Returns:
        Log content as string
    """
    try:
        with open(log_file, 'r') as f:
            return f.read()
    except Exception as e:
        print(f"✗ Error reading log file: {e}")
        return ""


def parse_routing_logs(logs: str) -> Dict[str, int]:
    """
    Parse load balancer logs to extract routing decisions.
    
    Args:
        logs: Log output as string
        
    Returns:
        Dictionary with counts for 'inference-1' and 'inference-2'
    """
    # Pattern to match: "Routing user <id> to inference-<1|2> (weighted: ...)"
    pattern = r'Routing user \d+ to (inference-[12]) \(weighted:'
    
    matches = re.findall(pattern, logs)
    counts = Counter(matches)
    
    return {
        'inference-1': counts.get('inference-1', 0),
        'inference-2': counts.get('inference-2', 0)
    }


def calculate_statistics(actual_counts: Dict[str, int], expected_weights: Tuple[float, float]) -> Dict:
    """
    Calculate distribution statistics.
    
    Args:
        actual_counts: Actual routing counts
        expected_weights: Expected weights (weight_1, weight_2)
        
    Returns:
        Dictionary with statistics
    """
    total = sum(actual_counts.values())
    if total == 0:
        return {
            'total': 0,
            'inference_1_count': 0,
            'inference_2_count': 0,
            'inference_1_percent': 0.0,
            'inference_2_percent': 0.0,
            'expected_1_percent': expected_weights[0],
            'expected_2_percent': expected_weights[1],
            'difference_1': 0.0,
            'difference_2': 0.0
        }
    
    actual_1_percent = (actual_counts['inference-1'] / total) * 100
    actual_2_percent = (actual_counts['inference-2'] / total) * 100
    
    return {
        'total': total,
        'inference_1_count': actual_counts['inference-1'],
        'inference_2_count': actual_counts['inference-2'],
        'inference_1_percent': actual_1_percent,
        'inference_2_percent': actual_2_percent,
        'expected_1_percent': expected_weights[0],
        'expected_2_percent': expected_weights[1],
        'difference_1': actual_1_percent - expected_weights[0],
        'difference_2': actual_2_percent - expected_weights[1]
    }


def print_results(stats: Dict, expected_weights: Tuple[float, float]):
    """
    Print test results in a formatted way.
    
    Args:
        stats: Statistics dictionary
        expected_weights: Expected weights tuple
    """
    print("\n" + "="*70)
    print("📊 WEIGHTED LOAD BALANCER TEST RESULTS")
    print("="*70)
    
    if stats['total'] == 0:
        print("\n✗ No routing decisions found in logs!")
        print("  Make sure:")
        print("  1. Load balancer is running and accessible")
        print("  2. Logs are accessible (kubectl or log file)")
        print("  3. Requests were actually made")
        return
    
    print(f"\nTotal requests analyzed: {stats['total']}")
    print(f"\nExpected Distribution:")
    print(f"  inference-1: {stats['expected_1_percent']:.2f}%")
    print(f"  inference-2: {stats['expected_2_percent']:.2f}%")
    
    print(f"\nActual Distribution:")
    print(f"  inference-1: {stats['inference_1_count']} requests ({stats['inference_1_percent']:.2f}%)")
    print(f"  inference-2: {stats['inference_2_count']} requests ({stats['inference_2_percent']:.2f}%)")
    
    print(f"\nDifference from Expected:")
    diff_1 = stats['difference_1']
    diff_2 = stats['difference_2']
    print(f"  inference-1: {diff_1:+.2f}% {'✓' if abs(diff_1) < 5 else '⚠'}")
    print(f"  inference-2: {diff_2:+.2f}% {'✓' if abs(diff_2) < 5 else '⚠'}")
    
    # Statistical significance check
    # For weighted random, we expect some variance
    # With 100 requests, standard deviation is roughly sqrt(p*(1-p)*n) = sqrt(0.9*0.1*100) ≈ 3
    # So ±5% is reasonable for 100 requests, ±3% for 1000 requests
    tolerance = max(5.0, 10.0 / (stats['total'] ** 0.5))  # Decreases with more samples
    
    print(f"\n📈 Analysis:")
    if abs(diff_1) <= tolerance and abs(diff_2) <= tolerance:
        print(f"  ✓ Distribution is within expected range (±{tolerance:.1f}% tolerance)")
        print(f"  ✓ Weighted routing appears to be working correctly!")
    else:
        print(f"  ⚠ Distribution deviates more than expected (±{tolerance:.1f}% tolerance)")
        print(f"  ⚠ This could be normal variance, or there may be an issue.")
        print(f"  💡 Try running with more requests (--num-requests 1000) for better accuracy")
    
    print("\n" + "="*70)


def main():
    parser = argparse.ArgumentParser(
        description='Test weighted load balancer distribution',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test with kubectl (Kubernetes deployment)
  python test_weighted_load_balancer.py --use-kubectl
  
  # Test with log file (local deployment)
  python test_weighted_load_balancer.py --log-file /var/log/load_balancer.log
  
  # Custom number of requests
  python test_weighted_load_balancer.py --num-requests 500
  
  # Specify expected weights manually (80/20 split)
  python test_weighted_load_balancer.py --use-kubectl --expected-weight-1 80 --expected-weight-2 20
  
  # Test with custom weights and more requests
  python test_weighted_load_balancer.py --use-kubectl --expected-weight-1 70 --expected-weight-2 30 --num-requests 500
        """
    )
    
    parser.add_argument(
        '--load-balancer-url',
        type=str,
        default='http://localhost:8080',
        help='Base URL of the load balancer (default: http://localhost:8080)'
    )
    
    parser.add_argument(
        '--num-requests',
        type=int,
        default=100,
        help='Number of test requests to make (default: 100, recommend 500+ for accuracy)'
    )
    
    parser.add_argument(
        '--use-kubectl',
        action='store_true',
        help='Use kubectl to get load balancer logs (for Kubernetes deployments)'
    )
    
    parser.add_argument(
        '--log-file',
        type=str,
        help='Path to load balancer log file (for local deployments)'
    )
    
    parser.add_argument(
        '--timeout',
        type=int,
        default=5,
        help='Request timeout in seconds (default: 5, inference takes 80-135s but routing is immediate)'
    )
    
    parser.add_argument(
        '--expected-weight-1',
        type=float,
        default=None,
        help='Expected weight for inference-1 (as percentage, e.g., 80 for 80%%). If not provided, will fetch from health endpoint.'
    )
    
    parser.add_argument(
        '--expected-weight-2',
        type=float,
        default=None,
        help='Expected weight for inference-2 (as percentage, e.g., 20 for 20%%). If not provided, will fetch from health endpoint.'
    )
    
    args = parser.parse_args()
    
    # Validate that if one weight is provided, both must be provided
    if (args.expected_weight_1 is not None and args.expected_weight_2 is None) or \
       (args.expected_weight_1 is None and args.expected_weight_2 is not None):
        print("✗ Error: Both --expected-weight-1 and --expected-weight-2 must be provided together, or neither.")
        sys.exit(1)
    
    print("🧪 Weighted Load Balancer Test")
    print("="*70)
    
    # Step 1: Get configured weights (either from args or health endpoint)
    if args.expected_weight_1 is not None and args.expected_weight_2 is not None:
        expected_weights = (args.expected_weight_1, args.expected_weight_2)
        print(f"✓ Using provided expected weights: inference-1={args.expected_weight_1}%, inference-2={args.expected_weight_2}%")
    else:
        expected_weights = get_configured_weights(args.load_balancer_url)
    
    # Step 2: Make test requests
    make_test_requests(args.load_balancer_url, args.num_requests, args.timeout)
    
    # Step 3: Get logs
    print(f"\n📋 Retrieving load balancer logs...")
    if args.use_kubectl:
        logs = get_logs_via_kubectl()
        if not logs:
            print("✗ Failed to get logs via kubectl")
            sys.exit(1)
    elif args.log_file:
        logs = get_logs_from_file(args.log_file)
        if not logs:
            print("✗ Failed to read log file")
            sys.exit(1)
    else:
        print("⚠ No log source specified. Using kubectl as default...")
        logs = get_logs_via_kubectl()
        if not logs:
            print("✗ Failed to get logs. Please specify --use-kubectl or --log-file")
            sys.exit(1)
    
    print(f"  ✓ Retrieved {len(logs)} characters of logs")
    
    # Step 4: Parse logs
    print(f"\n🔍 Parsing routing decisions from logs...")
    actual_counts = parse_routing_logs(logs)
    print(f"  ✓ Found {sum(actual_counts.values())} routing decisions")
    print(f"    - inference-1: {actual_counts['inference-1']}")
    print(f"    - inference-2: {actual_counts['inference-2']}")
    
    # Step 5: Calculate statistics
    stats = calculate_statistics(actual_counts, expected_weights)
    
    # Step 6: Print results
    print_results(stats, expected_weights)


if __name__ == '__main__':
    main()

