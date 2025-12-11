# PeaceDatabase Docker Demonstration

This guide shows how to deploy and demonstrate PeaceDatabase with sharding and replication using Docker.

## Quick Start (Local PowerShell)

Run the local demonstration script:

```powershell
.\demo-replication.ps1
```

This demonstrates:
- 3 shards with 2 replicas each (9 nodes total)
- Quorum writes (WriteQuorum=2)
- Read load balancing
- MVCC updates
- Tag-based queries

## Docker Deployment

### 1. Build and Start the Cluster

```bash
# Start the full replicated cluster (1 router + 9 data nodes)
docker-compose -f docker-compose.replication.yml up --build -d

# Wait for all services to be healthy
docker-compose -f docker-compose.replication.yml ps
```

### 2. Test the Cluster

```bash
# Check cluster status
curl http://localhost:8080/v1/_stats | jq

# Create a database (broadcasts to all 9 nodes)
curl -X PUT http://localhost:8080/v1/db/mydb

# Insert a document (quorum write to 2+ replicas)
curl -X PUT http://localhost:8080/v1/db/mydb/docs/doc1 \
  -H "Content-Type: application/json" \
  -d '{"id":"doc1","data":{"name":"Test Document","value":42},"tags":["demo"]}'

# Read the document (load balanced across replicas)
curl http://localhost:8080/v1/db/mydb/docs/doc1 | jq

# Get all documents (scatter-gather from all shards)
curl http://localhost:8080/v1/db/mydb/_all_docs | jq

# Get database statistics (aggregated from all nodes)
curl http://localhost:8080/v1/db/mydb/_stats | jq
```

### 3. Demonstrate Failover

```bash
# Check which nodes are running
docker-compose -f docker-compose.replication.yml ps

# Stop a primary shard (simulates node failure)
docker stop peacedb-shard-0-primary

# The system automatically promotes a replica to primary!
# Try reading the document - it should still work
curl http://localhost:8080/v1/db/mydb/docs/doc1 | jq

# Insert a new document - quorum write still works with 2/3 nodes
curl -X PUT http://localhost:8080/v1/db/mydb/docs/doc2 \
  -H "Content-Type: application/json" \
  -d '{"id":"doc2","data":{"name":"Created after failover"}}'

# Verify the document was created
curl http://localhost:8080/v1/db/mydb/_all_docs | jq

# Restart the failed node - it will sync back
docker start peacedb-shard-0-primary
```

### 4. View Logs

```bash
# View router logs
docker logs -f peacedb-router

# View shard logs
docker logs -f peacedb-shard-0-primary
docker logs -f peacedb-shard-0-replica-1

# View all logs
docker-compose -f docker-compose.replication.yml logs -f
```

### 5. Cleanup

```bash
# Stop and remove all containers
docker-compose -f docker-compose.replication.yml down

# Remove volumes (deletes all data)
docker-compose -f docker-compose.replication.yml down -v
```

## PowerShell Commands for Windows

```powershell
# Check cluster status
Invoke-RestMethod -Uri "http://localhost:8080/v1/_stats" | ConvertTo-Json -Depth 5

# Create database
Invoke-RestMethod -Uri "http://localhost:8080/v1/db/mydb" -Method Put

# Insert document
$body = '{"id":"doc1","data":{"name":"Test","value":42},"tags":["demo"]}'
Invoke-RestMethod -Uri "http://localhost:8080/v1/db/mydb/docs/doc1" -Method Put -Body $body -ContentType "application/json"

# Read document
Invoke-RestMethod -Uri "http://localhost:8080/v1/db/mydb/docs/doc1" | ConvertTo-Json -Depth 5

# Get all documents
Invoke-RestMethod -Uri "http://localhost:8080/v1/db/mydb/_all_docs" | ConvertTo-Json -Depth 5

# Get stats
Invoke-RestMethod -Uri "http://localhost:8080/v1/db/mydb/_stats" | ConvertTo-Json -Depth 5
```

## Architecture Overview

```
                    ┌─────────────────┐
                    │     Router      │ ← Single entry point (port 8080)
                    │  (Coordinator)  │
                    └────────┬────────┘
                             │
           ┌─────────────────┼─────────────────┐
           │                 │                 │
    ┌──────▼──────┐   ┌──────▼──────┐   ┌──────▼──────┐
    │   Shard 0   │   │   Shard 1   │   │   Shard 2   │
    │ (hash % 3=0)│   │ (hash % 3=1)│   │ (hash % 3=2)│
    └──────┬──────┘   └──────┬──────┘   └──────┬──────┘
           │                 │                 │
    ┌──────┴──────┐   ┌──────┴──────┐   ┌──────┴──────┐
    │   Primary   │   │   Primary   │   │   Primary   │
    │  Replica 1  │   │  Replica 1  │   │  Replica 1  │
    │  Replica 2  │   │  Replica 2  │   │  Replica 2  │
    └─────────────┘   └─────────────┘   └─────────────┘
```

## Key Features Demonstrated

1. **Hash-based Sharding**: Documents distributed by `hash(id) % shardCount`
2. **Quorum Writes**: Writes succeed when WriteQuorum replicas acknowledge
3. **Read Load Balancing**: Reads distributed across healthy replicas
4. **Automatic Failover**: Replica promoted to primary when primary fails
5. **Scatter-Gather Queries**: All-docs and search queries hit all shards
6. **MVCC**: Optimistic locking via revisions prevents conflicts

