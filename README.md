# Raft Lite - Consensus Implementation

3-node Raft consensus cluster with leader election, log replication, and failure recovery.

## Quick Start

### 1. Setup (on each EC2 node)

```bash
# Install dependencies
pip3 install requests

# Upload node.py
# (use scp from local machine)
```

### 2. Start Cluster

**Terminal 1 - Node A:**
```bash
python3 node.py --id A --port 8000 --peers "B:172.31.19.43:8001,C:172.31.22.41:8002"
```

**Terminal 2 - Node B:**
```bash
python3 node.py --id B --port 8001 --peers "A:172.31.24.28:8000,C:172.31.22.41:8002"
```

**Terminal 3 - Node C:**
```bash
python3 node.py --id C --port 8002 --peers "A:172.31.24.28:8000,B:172.31.19.43:8001"
```


### 3. Test Commands

**Check status:**
```bash
python3 client.py status
```

**Submit commands (auto-finds leader):**
```bash
python3 client.py cmd "SET x = 5"
python3 client.py cmd "SET y = 10"
```

**View logs:**
```bash
python3 client.py logs
```

### 4. Test Failure Recovery

1. Find leader: `python3 client.py leader`
2. Kill leader process (Ctrl+C)
3. Watch new election in remaining node terminals
4. Submit new command to verify recovery

## Files

- `node.py` - Raft node implementation
- `client.py` - Testing client with auto-discovery
