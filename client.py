#!/usr/bin/env python3
"""
Raft Client - Improved version with leader discovery
"""

import requests
import json
import argparse
import sys

NODES = [
    "http://172.31.24.28:8000",  # Node A
    "http://172.31.19.43:8001",  # Node B
    "http://172.31.22.41:8002",  # Node C
]

def find_leader():
    """Find which node is the leader"""
    for node_url in NODES:
        try:
            response = requests.get(f"{node_url}/status", timeout=1)
            if response.status_code == 200:
                data = response.json()
                if data.get("state") == "LEADER":
                    return node_url, data.get("node_id")
        except:
            pass
    return None, None

def send_request(url, data=None, method='GET'):
    """Send HTTP request and handle errors."""
    try:
        if method == 'POST':
            response = requests.post(url, json=data, timeout=2)
        else:
            response = requests.get(url, timeout=2)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error {response.status_code}: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Connection error: {e}")
        return None

def print_status_all():
    """Print status of all nodes"""
    print("=== Cluster Status ===")
    for node_url in NODES:
        try:
            response = requests.get(f"{node_url}/status", timeout=1)
            if response.status_code == 200:
                data = response.json()
                state = data.get("state", "UNKNOWN")
                term = data.get("current_term", 0)
                node_id = data.get("node_id", "?")
                marker = " <-- LEADER" if state == "LEADER" else ""
                print(f"  Node {node_id}: {state} (term={term}){marker}")
        except:
            node_id = node_url.split(":")[-1]
            print(f"  Node at {node_url}: UNREACHABLE")
    print()

def print_logs_all():
    """Print logs of all nodes"""
    print("=== Cluster Logs ===")
    for node_url in NODES:
        try:
            response = requests.get(f"{node_url}/log", timeout=1)
            if response.status_code == 200:
                data = response.json()
                node_id = data.get("node_id", "?")
                log = data.get("log", [])
                commit_idx = data.get("commit_index", 0)
                print(f"  Node {node_id} (committed: {commit_idx}):")
                if log:
                    for i, entry in enumerate(log, 1):
                        committed = "✓" if i <= commit_idx else "○"
                        print(f"    [{committed}] {i}. term={entry['term']}, cmd={entry['command']}")
                else:
                    print(f"    (empty log)")
                print()
        except:
            print(f"  Node at {node_url}: UNREACHABLE\n")

def main():
    parser = argparse.ArgumentParser(description='Raft Client with Auto-Discovery')
    parser.add_argument('--node', help='Specific node URL (optional, will auto-discover leader)')
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Status commands
    status_parser = subparsers.add_parser('status', help='Get status of all nodes')
    log_parser = subparsers.add_parser('logs', help='Get logs of all nodes')
    leader_parser = subparsers.add_parser('leader', help='Find the leader')
    
    # Command submission
    cmd_parser = subparsers.add_parser('cmd', help='Submit command (auto-finds leader)')
    cmd_parser.add_argument('command_text', help='Command text (e.g., "SET x = 5")')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    if args.command == 'status':
        print_status_all()
    
    elif args.command == 'logs':
        print_logs_all()
    
    elif args.command == 'leader':
        leader_url, leader_id = find_leader()
        if leader_url:
            print(f"Current leader: Node {leader_id} at {leader_url}")
        else:
            print("No leader found (election may be in progress)")
    
    elif args.command == 'cmd':
        # Auto-discover leader if not specified
        if args.node:
            target_url = args.node.rstrip('/')
        else:
            print("Auto-discovering leader...")
            leader_url, leader_id = find_leader()
            if not leader_url:
                print("ERROR: No leader found!")
                sys.exit(1)
            print(f"Found leader: Node {leader_id} at {leader_url}")
            target_url = leader_url
        
        # Submit command
        data = {"command": args.command_text}
        print(f"Submitting: {args.command_text}")
        result = send_request(f"{target_url}/command", data, method='POST')
        
        if result:
            if result.get("success"):
                print(f"✓ Success! Log index: {result.get('log_index')}")
                print("  Waiting for replication...")
                import time
                time.sleep(1)
                print("\nUpdated logs:")
                print_logs_all()
            else:
                print(f"✗ Failed: {result.get('message')}")
                if result.get('leader_hint'):
                    print(f"  Leader might be at: {result.get('leader_hint')}")
    
    else:
        print(f"Unknown command: {args.command}")
        sys.exit(1)

if __name__ == "__main__":
    main()