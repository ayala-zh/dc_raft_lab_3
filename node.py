#!/usr/bin/env python3
"""
Raft Lite - Complete Implementation
Supports: Leader Election, Log Replication, Failure Recovery
"""

import argparse
import json
import random
import time
import threading
import requests
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime

class RaftNode:
    def __init__(self, node_id, port, peers):
        self.node_id = node_id
        self.port = port
        self.peers = peers  # List of (peer_id, "host:port")
        
        # Persistent state
        self.currentTerm = 0
        self.votedFor = None
        self.log = []  # [(term, command), ...]
        
        # Volatile state
        self.commitIndex = 0
        self.lastApplied = 0
        self.state = "FOLLOWER"
        
        # Leader state
        self.nextIndex = {}  # For each peer
        self.matchIndex = {}  # For each peer
        
        # Timing - add initial delay to allow all nodes to start
        self.last_heartbeat = time.time() + 7.0  # 7 second grace period
        self.election_timeout = self._random_timeout()
        
        # Threading
        self.lock = threading.Lock()
        self.running = True
        
        print(f"[{node_id}] Starting on port {port}, peers: {[p[0] for p in peers]}")
        print(f"[{node_id}] Waiting 2 seconds for cluster to initialize...")
    
    def _random_timeout(self):
        """Random election timeout between 150-300ms"""
        return random.uniform(0.15, 0.3)
    
    def _log(self, msg):
        """Thread-safe logging with timestamp"""
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print(f"[{timestamp}] [{self.node_id}] {msg}")
    
    def get_status(self):
        """Return current node status"""
        with self.lock:
            return {
                "node_id": self.node_id,
                "state": self.state,
                "current_term": self.currentTerm,
                "voted_for": self.votedFor,
                "log_length": len(self.log),
                "commit_index": self.commitIndex
            }
    
    def get_log(self):
        """Return current log"""
        with self.lock:
            return {
                "node_id": self.node_id,
                "log": [{"term": term, "command": cmd} for term, cmd in self.log],
                "commit_index": self.commitIndex
            }
    
    def handle_request_vote(self, term, candidate_id):
        """Handle RequestVote RPC"""
        with self.lock:
            self._log(f"Received RequestVote from {candidate_id}, term={term}")
            
            # Update term if necessary
            if term > self.currentTerm:
                self.currentTerm = term
                self.votedFor = None
                self.state = "FOLLOWER"
            
            vote_granted = False
            
            # Grant vote if we haven't voted or voted for this candidate
            if term >= self.currentTerm and (self.votedFor is None or self.votedFor == candidate_id):
                vote_granted = True
                self.votedFor = candidate_id
                self.last_heartbeat = time.time()
                self._log(f"Voted for {candidate_id} in term {term}")
            
            return {
                "term": self.currentTerm,
                "voteGranted": vote_granted
            }
    
    def handle_append_entries(self, term, leader_id, entries, leader_commit):
        """Handle AppendEntries RPC (heartbeat or log replication)"""
        with self.lock:
            # Update term if necessary
            if term > self.currentTerm:
                self.currentTerm = term
                self.votedFor = None
                self.state = "FOLLOWER"
            
            success = False
            
            if term >= self.currentTerm:
                self.state = "FOLLOWER"
                self.last_heartbeat = time.time()
                success = True
                
                # Append entries to log
                if entries:
                    for entry in entries:
                        self.log.append((entry["term"], entry["command"]))
                    self._log(f"Appended {len(entries)} entries from leader {leader_id}")
                
                # Update commit index
                if leader_commit > self.commitIndex:
                    self.commitIndex = min(leader_commit, len(self.log))
                    self._log(f"Updated commitIndex to {self.commitIndex}")
            
            return {
                "term": self.currentTerm,
                "success": success
            }
    
    def handle_client_command(self, command):
        """Handle client command submission"""
        with self.lock:
            if self.state != "LEADER":
                # Try to help client find the leader
                leader_info = None
                if self.state == "FOLLOWER" and self.votedFor:
                    # Might know who the leader is
                    for peer_id, peer_addr in self.peers:
                        if peer_id == self.votedFor:
                            leader_info = f"http://{peer_addr}"
                            break
                
                return {
                    "success": False,
                    "message": f"Not leader. Current state: {self.state}",
                    "leader_hint": leader_info
                }
            
            # Append to log
            self.log.append((self.currentTerm, command))
            log_index = len(self.log)
            self._log(f"Append log entry (term={self.currentTerm}, cmd={command}, index={log_index})")
            
            return {
                "success": True,
                "message": "Command received",
                "log_index": log_index
            }
    
    def start_election(self):
        """Start leader election"""
        with self.lock:
            self.state = "CANDIDATE"
            self.currentTerm += 1
            self.votedFor = self.node_id
            self.election_timeout = self._random_timeout()
            term = self.currentTerm
            self._log(f"Timeout → Candidate (term {term})")
        
        # Request votes from all peers
        votes = 1  # Vote for self
        votes_needed = len(self.peers) // 2 + 1
        
        def request_vote(peer_id, peer_addr):
            nonlocal votes
            try:
                url = f"http://{peer_addr}/request_vote"
                data = {"term": term, "candidateId": self.node_id}
                response = requests.post(url, json=data, timeout=0.1)
                
                if response.status_code == 200:
                    result = response.json()
                    
                    with self.lock:
                        if result.get("term", 0) > self.currentTerm:
                            self.currentTerm = result["term"]
                            self.state = "FOLLOWER"
                            self.votedFor = None
                        elif result.get("voteGranted") and self.state == "CANDIDATE":
                            votes += 1
                            self._log(f"Received vote from {peer_id} ({votes}/{votes_needed})")
            except:
                pass
        
        # Send vote requests in parallel
        threads = []
        for peer_id, peer_addr in self.peers:
            t = threading.Thread(target=request_vote, args=(peer_id, peer_addr))
            t.start()
            threads.append(t)
        
        # Wait for responses
        for t in threads:
            t.join(timeout=0.15)
        
        # Check if won election
        with self.lock:
            if self.state == "CANDIDATE" and votes >= votes_needed:
                self.state = "LEADER"
                self._log(f"Received votes from {votes} nodes → Leader")
                
                # Initialize leader state
                for peer_id, _ in self.peers:
                    self.nextIndex[peer_id] = len(self.log) + 1
                    self.matchIndex[peer_id] = 0
    
    def send_heartbeats(self):
        """Send heartbeats to all followers"""
        with self.lock:
            if self.state != "LEADER":
                return
            term = self.currentTerm
            commit_index = self.commitIndex
        
        def send_heartbeat(peer_id, peer_addr):
            try:
                url = f"http://{peer_addr}/append_entries"
                data = {
                    "term": term,
                    "leaderId": self.node_id,
                    "entries": [],
                    "leaderCommit": commit_index
                }
                requests.post(url, json=data, timeout=0.1)
            except:
                pass
        
        # Send heartbeats in parallel
        threads = []
        for peer_id, peer_addr in self.peers:
            t = threading.Thread(target=send_heartbeat, args=(peer_id, peer_addr))
            t.start()
            threads.append(t)
        
        for t in threads:
            t.join(timeout=0.15)
    
    def replicate_log(self):
        """Replicate log entries to followers"""
        with self.lock:
            if self.state != "LEADER":
                return
            
            # Check if there are uncommitted entries
            if len(self.log) == 0 or self.commitIndex >= len(self.log):
                return
            
            term = self.currentTerm
            uncommitted_entries = [
                {"term": t, "command": cmd}
                for t, cmd in self.log[self.commitIndex:]
            ]
        
        if not uncommitted_entries:
            return
        
        # Track acknowledgments
        acks = {i: 1 for i in range(self.commitIndex + 1, len(self.log) + 1)}  # Leader acks itself
        ack_lock = threading.Lock()
        
        def replicate_to_peer(peer_id, peer_addr):
            try:
                url = f"http://{peer_addr}/append_entries"
                data = {
                    "term": term,
                    "leaderId": self.node_id,
                    "entries": uncommitted_entries,
                    "leaderCommit": self.commitIndex
                }
                response = requests.post(url, json=data, timeout=0.2)
                
                if response.status_code == 200:
                    result = response.json()
                    if result.get("success"):
                        with ack_lock:
                            for i in range(self.commitIndex + 1, len(self.log) + 1):
                                acks[i] += 1
            except:
                pass
        
        # Replicate in parallel
        threads = []
        for peer_id, peer_addr in self.peers:
            t = threading.Thread(target=replicate_to_peer, args=(peer_id, peer_addr))
            t.start()
            threads.append(t)
        
        for t in threads:
            t.join(timeout=0.25)
        
        # Check for majority and commit
        majority = len(self.peers) // 2 + 1
        with self.lock:
            for i in range(self.commitIndex + 1, len(self.log) + 1):
                if acks.get(i, 0) >= majority:
                    self.commitIndex = i
                    self._log(f"Entry committed (index={i})")
    
    def run_election_timer(self):
        """Background thread for election timeout"""
        while self.running:
            time.sleep(0.01)
            
            with self.lock:
                if self.state != "LEADER":
                    elapsed = time.time() - self.last_heartbeat
                    if elapsed > self.election_timeout:
                        # Reset heartbeat timer
                        self.last_heartbeat = time.time()
                        needs_election = True
                    else:
                        needs_election = False
                else:
                    needs_election = False
            
            if needs_election:
                self.start_election()
    
    def run_heartbeat_timer(self):
        """Background thread for sending heartbeats"""
        while self.running:
            time.sleep(0.05)  # 50ms heartbeat interval
            
            with self.lock:
                is_leader = self.state == "LEADER"
            
            if is_leader:
                self.send_heartbeats()
                self.replicate_log()


class RaftHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/status':
            node = self.server.raft_node
            response = json.dumps(node.get_status()).encode()
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(response)))
            self.end_headers()
            self.wfile.write(response)
        
        elif self.path == '/log':
            node = self.server.raft_node
            response = json.dumps(node.get_log()).encode()
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(response)))
            self.end_headers()
            self.wfile.write(response)
        
        elif self.path == '/health':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'OK')
        
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'Not found')
    
    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length) if content_length else b'{}'
        data = json.loads(body)
        
        node = self.server.raft_node
        
        if self.path == '/request_vote':
            result = node.handle_request_vote(
                data.get("term", 0),
                data.get("candidateId", "")
            )
            response = json.dumps(result).encode()
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(response)))
            self.end_headers()
            self.wfile.write(response)
        
        elif self.path == '/append_entries':
            result = node.handle_append_entries(
                data.get("term", 0),
                data.get("leaderId", ""),
                data.get("entries", []),
                data.get("leaderCommit", 0)
            )
            response = json.dumps(result).encode()
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(response)))
            self.end_headers()
            self.wfile.write(response)
        
        elif self.path == '/command':
            result = node.handle_client_command(data.get("command", ""))
            response = json.dumps(result).encode()
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(response)))
            self.end_headers()
            self.wfile.write(response)
        
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'Not found')
    
    def log_message(self, format, *args):
        pass  # Suppress HTTP logs


class RaftServer(HTTPServer):
    def __init__(self, server_address, raft_node):
        self.raft_node = raft_node
        super().__init__(server_address, RaftHandler)


def parse_peers(peers_str):
    """Parse peer string: 'B:172.31.19.43:8001,C:172.31.22.41:8002'"""
    peers = []
    if peers_str:
        for peer in peers_str.split(','):
            if peer:
                parts = peer.split(':')
                if len(parts) == 3:
                    peer_id, host, port = parts
                    peers.append((peer_id.strip(), f"{host}:{port}"))
    return peers


def main():
    parser = argparse.ArgumentParser(description='Raft Node')
    parser.add_argument('--id', required=True, help='Node ID')
    parser.add_argument('--port', type=int, required=True, help='Port number')
    parser.add_argument('--peers', required=True, help='Peers in format ID:HOST:PORT,ID:HOST:PORT')
    args = parser.parse_args()
    
    peers = parse_peers(args.peers)
    node = RaftNode(args.id, args.port, peers)
    
    # Start HTTP server
    server = RaftServer(('0.0.0.0', args.port), node)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    print(f"[{args.id}] HTTP server ready on port {args.port}")
    
    # Start background threads
    election_thread = threading.Thread(target=node.run_election_timer, daemon=True)
    election_thread.start()
    
    heartbeat_thread = threading.Thread(target=node.run_heartbeat_timer, daemon=True)
    heartbeat_thread.start()
    
    print(f"[{args.id}] Node ready. Press Ctrl+C to stop.")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n[{args.id}] Shutting down")
        node.running = False


if __name__ == "__main__":
    main()