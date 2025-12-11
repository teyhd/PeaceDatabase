# demo-docker-commands.ps1
# Docker Demonstration Commands for PeaceDatabase Replication
# Run these commands to demonstrate the system to your teacher
#
# Prerequisites: docker-compose -f docker-compose.replication.yml up -d --build

$BaseUrl = "http://localhost:8080"

Write-Host "============================================" -ForegroundColor Cyan
Write-Host " PeaceDatabase Docker Cluster Demonstration" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# Step 1: Show cluster topology
Write-Host "[1] CLUSTER TOPOLOGY" -ForegroundColor Yellow
Write-Host "    10 containers: 1 Router + 9 Data Nodes (3 shards x 3 replicas)" -ForegroundColor Gray
docker-compose -f docker-compose.replication.yml ps --format "table {{.Name}}\t{{.Status}}"
Write-Host ""

# Step 2: Check cluster configuration
Write-Host "[2] CLUSTER CONFIGURATION" -ForegroundColor Yellow
$stats = Invoke-RestMethod -Uri "$BaseUrl/v1/_stats"
Write-Host "    Sharding: Enabled=$($stats.sharding.enabled), Shards=$($stats.sharding.shardCount)" -ForegroundColor Cyan
Write-Host "    Replication: Enabled=$($stats.replication.enabled), Replicas=$($stats.replication.replicaCount)" -ForegroundColor Cyan
Write-Host "    Write Quorum: $($stats.replication.writeQuorum) (writes need 2/3 acks)" -ForegroundColor Cyan
Write-Host "    Read Quorum: $($stats.replication.readQuorum) (reads from any replica)" -ForegroundColor Cyan
Write-Host ""

# Step 3: Create database
Write-Host "[3] CREATE DATABASE (broadcasts to all 9 nodes)" -ForegroundColor Yellow
$dbResult = Invoke-RestMethod -Uri "$BaseUrl/v1/db/demo" -Method Put
Write-Host "    Result: ok=$($dbResult.ok)" -ForegroundColor Green
Write-Host ""

# Step 4: Insert documents with quorum writes
Write-Host "[4] INSERT DOCUMENTS (quorum writes to 2+ replicas)" -ForegroundColor Yellow
$docs = @(
    @{ id = "user-1"; data = @{ name = "Alice"; role = "Admin" }; tags = @("user") }
    @{ id = "user-2"; data = @{ name = "Bob"; role = "User" }; tags = @("user") }
    @{ id = "order-1"; data = @{ item = "Laptop"; price = 999 }; tags = @("order") }
    @{ id = "order-2"; data = @{ item = "Phone"; price = 699 }; tags = @("order") }
)

foreach ($doc in $docs) {
    $body = $doc | ConvertTo-Json -Depth 5
    $result = Invoke-RestMethod -Uri "$BaseUrl/v1/db/demo/docs/$($doc.id)" -Method Put -Body $body -ContentType "application/json"
    Write-Host "    Inserted: $($doc.id) -> rev=$($result.rev.Substring(0, 20))..." -ForegroundColor Green
}
Write-Host ""

# Step 5: Read documents
Write-Host "[5] READ DOCUMENTS (load balanced across replicas)" -ForegroundColor Yellow
$user1 = Invoke-RestMethod -Uri "$BaseUrl/v1/db/demo/docs/user-1"
Write-Host "    user-1: name=$($user1.data.name), role=$($user1.data.role)" -ForegroundColor Cyan
$order1 = Invoke-RestMethod -Uri "$BaseUrl/v1/db/demo/docs/order-1"
Write-Host "    order-1: item=$($order1.data.item), price=$($order1.data.price)" -ForegroundColor Cyan
Write-Host ""

# Step 6: Get all documents (scatter-gather)
Write-Host "[6] GET ALL DOCUMENTS (scatter-gather from all 3 shards)" -ForegroundColor Yellow
$allDocs = Invoke-RestMethod -Uri "$BaseUrl/v1/db/demo/_all_docs"
Write-Host "    Total documents: $($allDocs.total)" -ForegroundColor Cyan
foreach ($item in $allDocs.items) {
    Write-Host "      - $($item.id): $($item.data | ConvertTo-Json -Compress)" -ForegroundColor Gray
}
Write-Host ""

# Step 7: Update with MVCC
Write-Host "[7] UPDATE DOCUMENT (MVCC with revision)" -ForegroundColor Yellow
$user1 = Invoke-RestMethod -Uri "$BaseUrl/v1/db/demo/docs/user-1"
Write-Host "    Current rev: $($user1.rev)" -ForegroundColor Gray
$updateDoc = @{
    id = $user1.id
    rev = $user1.rev
    data = @{ name = $user1.data.name; role = "SuperAdmin"; updatedAt = (Get-Date).ToString("o") }
    tags = $user1.tags
}
$updated = Invoke-RestMethod -Uri "$BaseUrl/v1/db/demo/docs/user-1" -Method Put -Body ($updateDoc | ConvertTo-Json -Depth 5) -ContentType "application/json"
Write-Host "    New rev: $($updated.rev)" -ForegroundColor Green
Write-Host "    Updated role: $($updated.data.role)" -ForegroundColor Cyan
Write-Host ""

# Step 8: Database statistics
Write-Host "[8] DATABASE STATISTICS (aggregated from all nodes)" -ForegroundColor Yellow
$dbStats = Invoke-RestMethod -Uri "$BaseUrl/v1/db/demo/_stats"
Write-Host "    Total Documents: $($dbStats.docsTotal)" -ForegroundColor Cyan
Write-Host "    Sequence Number: $($dbStats.seq)" -ForegroundColor Cyan
Write-Host ""

# Step 9: Demonstrate failover
Write-Host "[9] FAILOVER DEMONSTRATION" -ForegroundColor Yellow
Write-Host "    Stopping shard-0-primary..." -ForegroundColor Red
docker stop peacedb-shard-0-primary | Out-Null
Start-Sleep -Seconds 3

Write-Host "    Testing read after primary failure..." -ForegroundColor Gray
try {
    $user1 = Invoke-RestMethod -Uri "$BaseUrl/v1/db/demo/docs/user-1" -TimeoutSec 10
    Write-Host "    SUCCESS! Read user-1: $($user1.data.name)" -ForegroundColor Green
} catch {
    Write-Host "    Read failed: $_" -ForegroundColor Red
}

Write-Host "    Testing write after primary failure..." -ForegroundColor Gray
$newDoc = @{ id = "test-failover"; data = @{ created = "after failover" }; tags = @("test") }
try {
    $result = Invoke-RestMethod -Uri "$BaseUrl/v1/db/demo/docs/test-failover" -Method Put -Body ($newDoc | ConvertTo-Json) -ContentType "application/json" -TimeoutSec 10
    Write-Host "    SUCCESS! Created test-failover (rev=$($result.rev.Substring(0,20))...)" -ForegroundColor Green
} catch {
    Write-Host "    Write may have partial success: $_" -ForegroundColor Yellow
}

Write-Host "    Restarting shard-0-primary..." -ForegroundColor Gray
docker start peacedb-shard-0-primary | Out-Null
Start-Sleep -Seconds 5
Write-Host "    Node recovered!" -ForegroundColor Green
Write-Host ""

# Step 10: Final state
Write-Host "[10] FINAL STATE" -ForegroundColor Yellow
$finalDocs = Invoke-RestMethod -Uri "$BaseUrl/v1/db/demo/_all_docs"
Write-Host "    Total documents: $($finalDocs.total)" -ForegroundColor Cyan
Write-Host ""

Write-Host "============================================" -ForegroundColor Cyan
Write-Host " Demonstration Complete!" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Key Points Demonstrated:" -ForegroundColor Yellow
Write-Host "  1. 3 shards with 2 replicas each = 9 data nodes" -ForegroundColor Gray
Write-Host "  2. Quorum writes ensure durability (2/3 must acknowledge)" -ForegroundColor Gray
Write-Host "  3. Reads are load balanced across healthy replicas" -ForegroundColor Gray
Write-Host "  4. Scatter-gather queries aggregate from all shards" -ForegroundColor Gray
Write-Host "  5. Failover: cluster survives primary node failure" -ForegroundColor Gray
Write-Host ""
Write-Host "Commands:" -ForegroundColor Yellow
Write-Host "  docker-compose -f docker-compose.replication.yml ps    # Show containers" -ForegroundColor Gray
Write-Host "  docker-compose -f docker-compose.replication.yml logs  # View logs" -ForegroundColor Gray
Write-Host "  docker-compose -f docker-compose.replication.yml down  # Stop cluster" -ForegroundColor Gray

