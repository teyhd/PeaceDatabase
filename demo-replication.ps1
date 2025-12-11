# demo-replication.ps1
# Demonstration script for PeaceDatabase Replication
# This script shows quorum writes, sharding, and data consistency
#
# Usage: .\demo-replication.ps1

$ErrorActionPreference = "Stop"
$BaseUrl = "http://localhost:5000"
$DbName = "demo_db"

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  PeaceDatabase Replication Demonstration" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# ===== STEP 1: Start Server =====
Write-Host "[STEP 1] Starting server in Replicated Sharding mode..." -ForegroundColor Yellow
$env:STORAGE_MODE = "Sharded"
$env:SHARDING_ENABLED = "true"
$env:SHARDING_MODE = "Local"
$env:SHARD_COUNT = "3"
$env:REPLICATION_ENABLED = "true"
$env:REPLICA_COUNT = "2"
$env:WRITE_QUORUM = "2"
$env:READ_QUORUM = "1"

$serverJob = Start-Job -ScriptBlock {
    Set-Location "D:\_ITMOStudy\_Master\OPVP\PeaceDatabase\PeaceDatabase"
    $env:STORAGE_MODE = "Sharded"
    $env:SHARDING_ENABLED = "true"
    $env:SHARDING_MODE = "Local"
    $env:SHARD_COUNT = "3"
    $env:REPLICATION_ENABLED = "true"
    $env:REPLICA_COUNT = "2"
    $env:WRITE_QUORUM = "2"
    dotnet run --urls "http://localhost:5000" 2>&1
}

Write-Host "  Waiting for server to start..." -ForegroundColor Gray
Start-Sleep -Seconds 12

# Wait for server ready
$ready = $false
for ($i = 0; $i -lt 15; $i++) {
    try {
        $null = Invoke-RestMethod -Uri "$BaseUrl/healthz" -TimeoutSec 2
        $ready = $true
        break
    } catch {
        Start-Sleep -Seconds 1
    }
}

if (-not $ready) {
    Write-Host "  ERROR: Server failed to start!" -ForegroundColor Red
    Stop-Job $serverJob -ErrorAction SilentlyContinue
    Remove-Job $serverJob -ErrorAction SilentlyContinue
    exit 1
}

Write-Host "  Server is ready!" -ForegroundColor Green
Write-Host ""

# ===== STEP 2: Show Configuration =====
Write-Host "[STEP 2] Checking cluster configuration..." -ForegroundColor Yellow
$stats = Invoke-RestMethod -Uri "$BaseUrl/v1/_stats"
Write-Host "  Sharding:"
Write-Host "    - Enabled: $($stats.sharding.enabled)" -ForegroundColor Cyan
Write-Host "    - Mode: $($stats.sharding.mode)" -ForegroundColor Cyan
Write-Host "    - Shard Count: $($stats.sharding.shardCount)" -ForegroundColor Cyan
Write-Host "  Replication:"
Write-Host "    - Enabled: $($stats.replication.enabled)" -ForegroundColor Cyan
Write-Host "    - Replicas per Shard: $($stats.replication.replicaCount)" -ForegroundColor Cyan
Write-Host "    - Write Quorum: $($stats.replication.writeQuorum)" -ForegroundColor Cyan
Write-Host "    - Read Quorum: $($stats.replication.readQuorum)" -ForegroundColor Cyan
Write-Host "    - Total Nodes: $($stats.sharding.shardCount * ($stats.replication.replicaCount + 1))" -ForegroundColor Magenta
Write-Host ""

# ===== STEP 3: Create Database =====
Write-Host "[STEP 3] Creating database '$DbName' (broadcasts to all 9 nodes)..." -ForegroundColor Yellow
$createResult = Invoke-RestMethod -Uri "$BaseUrl/v1/db/$DbName" -Method Put
Write-Host "  Database created: ok=$($createResult.ok)" -ForegroundColor Green
Write-Host ""

# ===== STEP 4: Insert Documents with Quorum Writes =====
Write-Host "[STEP 4] Inserting documents with quorum writes..." -ForegroundColor Yellow
Write-Host "  (Each write requires $($stats.replication.writeQuorum) replicas to acknowledge)" -ForegroundColor Gray

$documents = @(
    @{ id = "user-001"; data = @{ name = "Alice"; email = "alice@example.com"; age = 28 }; tags = @("user", "active") }
    @{ id = "user-002"; data = @{ name = "Bob"; email = "bob@example.com"; age = 35 }; tags = @("user", "active") }
    @{ id = "user-003"; data = @{ name = "Charlie"; email = "charlie@example.com"; age = 22 }; tags = @("user", "new") }
    @{ id = "order-001"; data = @{ product = "Laptop"; price = 999.99; userId = "user-001" }; tags = @("order", "electronics") }
    @{ id = "order-002"; data = @{ product = "Phone"; price = 699.99; userId = "user-002" }; tags = @("order", "electronics") }
    @{ id = "order-003"; data = @{ product = "Book"; price = 29.99; userId = "user-001" }; tags = @("order", "books") }
)

foreach ($doc in $documents) {
    $body = $doc | ConvertTo-Json -Depth 5
    $result = Invoke-RestMethod -Uri "$BaseUrl/v1/db/$DbName/docs/$($doc.id)" -Method Put -Body $body -ContentType "application/json"
    $shardId = [Math]::Abs($doc.id.GetHashCode()) % 3
    Write-Host "  Inserted: $($doc.id) -> Shard $shardId (rev: $($result.rev.Substring(0, 20))...)" -ForegroundColor Green
}
Write-Host ""

# ===== STEP 5: Query All Documents =====
Write-Host "[STEP 5] Querying all documents (scatter-gather from all shards)..." -ForegroundColor Yellow
$allDocs = Invoke-RestMethod -Uri "$BaseUrl/v1/db/$DbName/_all_docs"
Write-Host "  Total documents: $($allDocs.total)" -ForegroundColor Cyan
foreach ($doc in $allDocs.items) {
    Write-Host "    - $($doc.id): $($doc.data | ConvertTo-Json -Compress)" -ForegroundColor Gray
}
Write-Host ""

# ===== STEP 6: Get Database Statistics =====
Write-Host "[STEP 6] Getting aggregated statistics from all nodes..." -ForegroundColor Yellow
$dbStats = Invoke-RestMethod -Uri "$BaseUrl/v1/db/$DbName/_stats"
Write-Host "  Database: $($dbStats.db)" -ForegroundColor Cyan
Write-Host "  Total Documents: $($dbStats.docsTotal)" -ForegroundColor Cyan
Write-Host "  Sequence Number: $($dbStats.seq)" -ForegroundColor Cyan
Write-Host ""

# ===== STEP 7: Test Read Load Balancing =====
Write-Host "[STEP 7] Testing read load balancing (multiple reads)..." -ForegroundColor Yellow
Write-Host "  Reading user-001 multiple times (may hit different replicas)..." -ForegroundColor Gray
for ($i = 1; $i -le 5; $i++) {
    $doc = Invoke-RestMethod -Uri "$BaseUrl/v1/db/$DbName/docs/user-001"
    Write-Host "    Read #$i : $($doc.data.name) - $($doc.data.email)" -ForegroundColor Green
}
Write-Host ""

# ===== STEP 8: Update Document with MVCC =====
Write-Host "[STEP 8] Updating document with MVCC (requires current revision)..." -ForegroundColor Yellow
$user = Invoke-RestMethod -Uri "$BaseUrl/v1/db/$DbName/docs/user-001"
Write-Host "  Current revision: $($user.rev)" -ForegroundColor Gray
# Create updated data as hashtable
$updatedData = @{
    name = $user.data.name
    email = $user.data.email
    age = 29
    updatedAt = (Get-Date).ToString("o")
}
$updateDoc = @{
    id = $user.id
    rev = $user.rev
    data = $updatedData
    tags = $user.tags
}
$updateBody = $updateDoc | ConvertTo-Json -Depth 5
$updated = Invoke-RestMethod -Uri "$BaseUrl/v1/db/$DbName/docs/user-001" -Method Put -Body $updateBody -ContentType "application/json"
Write-Host "  Updated revision: $($updated.rev)" -ForegroundColor Green
Write-Host "  New data: age=$($updated.data.age)" -ForegroundColor Cyan
Write-Host ""

# ===== STEP 9: Search by Tags =====
Write-Host "[STEP 9] Searching by tags (scatter-gather query)..." -ForegroundColor Yellow
$searchBody = @{ AllOf = @("order", "electronics") } | ConvertTo-Json
$searchResult = Invoke-RestMethod -Uri "$BaseUrl/v1/db/$DbName/_find/tags" -Method Post -Body $searchBody -ContentType "application/json"
Write-Host "  Found $($searchResult.total) orders with 'electronics' tag:" -ForegroundColor Cyan
foreach ($item in $searchResult.items) {
    Write-Host "    - $($item.id): $($item.data.product) = `$$($item.data.price)" -ForegroundColor Gray
}
Write-Host ""

# ===== STEP 10: Delete Document =====
Write-Host "[STEP 10] Deleting document (quorum delete)..." -ForegroundColor Yellow
$toDelete = Invoke-RestMethod -Uri "$BaseUrl/v1/db/$DbName/docs/order-003"
$null = Invoke-RestMethod -Uri "$BaseUrl/v1/db/$DbName/docs/order-003?rev=$($toDelete.rev)" -Method Delete
Write-Host "  Deleted: order-003" -ForegroundColor Green

# Final count
$finalDocs = Invoke-RestMethod -Uri "$BaseUrl/v1/db/$DbName/_all_docs?includeDeleted=false"
Write-Host "  Remaining documents: $($finalDocs.total)" -ForegroundColor Cyan
Write-Host ""

# ===== CLEANUP =====
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Demonstration Complete!" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Summary:" -ForegroundColor Yellow
Write-Host "  - 3 shards with 2 replicas each = 9 total nodes" -ForegroundColor Gray
Write-Host "  - Quorum writes ensure data durability (2/3 acks required)" -ForegroundColor Gray
Write-Host "  - Read load balancing across all healthy replicas" -ForegroundColor Gray
Write-Host "  - MVCC for optimistic concurrency control" -ForegroundColor Gray
Write-Host "  - Automatic failover when nodes go down (in Docker mode)" -ForegroundColor Gray
Write-Host ""

Write-Host "Stopping server..." -ForegroundColor Yellow
Stop-Job $serverJob -ErrorAction SilentlyContinue
Remove-Job $serverJob -ErrorAction SilentlyContinue
Stop-Process -Name "dotnet" -Force -ErrorAction SilentlyContinue
Write-Host "Done!" -ForegroundColor Green

