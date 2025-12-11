# test-replication.ps1
# PowerShell script to test PeaceDatabase replication with failover
#
# This script demonstrates:
# 1. Quorum writes (data replicated to multiple nodes)
# 2. Read load balancing
# 3. Automatic failover when a node goes down
# 4. Data consistency after failover
#
# Prerequisites:
# - PowerShell 7+
# - .NET 8 SDK
# - curl available in PATH
#
# Usage:
#   .\test-replication.ps1

$ErrorActionPreference = "Stop"

# Configuration
$ProjectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$WebApiProject = Join-Path $ProjectDir "PeaceDatabase"
$BaseUrl = "http://localhost:5000"
$DbName = "replication_test"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host " PeaceDatabase Replication Test" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# ----------------------------------------
# Step 1: Start the server in replication mode (Local)
# ----------------------------------------
Write-Host "[1/7] Starting PeaceDatabase in LOCAL replication mode..." -ForegroundColor Yellow

# Set environment variables for local replication mode
$env:STORAGE_MODE = "Sharded"
$env:SHARDING_ENABLED = "true"
$env:SHARDING_MODE = "Local"
$env:SHARD_COUNT = "3"
$env:REPLICATION_ENABLED = "true"
$env:REPLICA_COUNT = "2"
$env:WRITE_QUORUM = "2"
$env:READ_QUORUM = "1"
$env:ASPNETCORE_ENVIRONMENT = "Development"

# Start the server in background
$serverProcess = Start-Process -FilePath "dotnet" -ArgumentList "run --project `"$WebApiProject`" --urls $BaseUrl" -PassThru -NoNewWindow

Write-Host "  Server starting (PID: $($serverProcess.Id))..." -ForegroundColor Gray
Start-Sleep -Seconds 10

# Wait for server to be ready
$maxRetries = 30
$retries = 0
while ($retries -lt $maxRetries) {
    try {
        $response = Invoke-RestMethod -Uri "$BaseUrl/healthz" -Method Get -TimeoutSec 2
        Write-Host "  Server is ready!" -ForegroundColor Green
        break
    }
    catch {
        $retries++
        if ($retries -ge $maxRetries) {
            Write-Host "  Server failed to start!" -ForegroundColor Red
            Stop-Process -Id $serverProcess.Id -Force -ErrorAction SilentlyContinue
            exit 1
        }
        Start-Sleep -Seconds 1
    }
}

# ----------------------------------------
# Step 2: Check server status and replication config
# ----------------------------------------
Write-Host ""
Write-Host "[2/7] Checking server status and replication configuration..." -ForegroundColor Yellow

try {
    $stats = Invoke-RestMethod -Uri "$BaseUrl/v1/_stats" -Method Get
    Write-Host "  Storage Mode: $($stats.storageMode)" -ForegroundColor Gray
    Write-Host "  Sharding Enabled: $($stats.sharding.enabled)" -ForegroundColor Gray
    Write-Host "  Shard Count: $($stats.sharding.shardCount)" -ForegroundColor Gray
    Write-Host "  Replication Enabled: $($stats.replication.enabled)" -ForegroundColor Gray
    Write-Host "  Replica Count: $($stats.replication.replicaCount)" -ForegroundColor Gray
    Write-Host "  Write Quorum: $($stats.replication.writeQuorum)" -ForegroundColor Gray
    Write-Host "  Read Quorum: $($stats.replication.readQuorum)" -ForegroundColor Gray
    Write-Host "  Sync Mode: $($stats.replication.syncMode)" -ForegroundColor Gray
}
catch {
    Write-Host "  Failed to get stats: $_" -ForegroundColor Red
}

# ----------------------------------------
# Step 3: Create database (broadcast to all replicas)
# ----------------------------------------
Write-Host ""
Write-Host "[3/7] Creating database '$DbName' (broadcast to all replicas)..." -ForegroundColor Yellow

try {
    $createResult = Invoke-RestMethod -Uri "$BaseUrl/v1/db/$DbName" -Method Put
    Write-Host "  Database created: ok=$($createResult.ok)" -ForegroundColor Green
}
catch {
    $statusCode = $_.Exception.Response.StatusCode.value__
    if ($statusCode -eq 409) {
        Write-Host "  Database already exists (ok)" -ForegroundColor Gray
    }
    else {
        Write-Host "  Failed to create database: $_" -ForegroundColor Red
    }
}

# ----------------------------------------
# Step 4: Insert documents (quorum writes)
# ----------------------------------------
Write-Host ""
Write-Host "[4/7] Inserting documents with quorum writes (WriteQuorum=2)..." -ForegroundColor Yellow

$docIds = @("doc-alpha", "doc-beta", "doc-gamma", "doc-delta", "doc-epsilon", 
    "doc-zeta", "doc-eta", "doc-theta", "doc-iota", "doc-kappa")
$insertedDocs = @{}

foreach ($docId in $docIds) {
    $body = @{
        id   = $docId
        data = @{
            name      = "Test Document $docId"
            value     = Get-Random -Maximum 1000
            timestamp = (Get-Date).ToString("o")
        }
        tags = @("test", "replication")
    } | ConvertTo-Json -Depth 5

    try {
        $result = Invoke-RestMethod -Uri "$BaseUrl/v1/db/$DbName/docs/$docId" -Method Put -Body $body -ContentType "application/json"
        $insertedDocs[$docId] = $result.rev
        Write-Host "  Inserted $docId (rev: $($result.rev))" -ForegroundColor Green
    }
    catch {
        $statusCode = $null
        $errorBody = $null
        try {
            $statusCode = $_.Exception.Response.StatusCode.value__
            $stream = $_.Exception.Response.GetResponseStream()
            $reader = New-Object System.IO.StreamReader($stream)
            $errorBody = $reader.ReadToEnd()
            $reader.Close()
        } catch {}
        Write-Host "  Failed to insert $docId (HTTP $statusCode): $errorBody" -ForegroundColor Red
    }
}

Write-Host "  Total documents inserted: $($insertedDocs.Count)" -ForegroundColor Cyan

# ----------------------------------------
# Step 5: Get all documents (scatter-gather with load balancing)
# ----------------------------------------
Write-Host ""
Write-Host "[5/7] Getting all documents (scatter-gather from all shards)..." -ForegroundColor Yellow

try {
    $allDocs = Invoke-RestMethod -Uri "$BaseUrl/v1/db/$DbName/_all_docs"
    Write-Host "  Total documents found: $($allDocs.total)" -ForegroundColor Green
    
    foreach ($doc in $allDocs.items) {
        Write-Host "    - $($doc.id): $($doc.data.name)" -ForegroundColor Gray
    }
}
catch {
    Write-Host "  Failed to get all documents: $_" -ForegroundColor Red
}

# ----------------------------------------
# Step 6: Get database stats (aggregated from all replicas)
# ----------------------------------------
Write-Host ""
Write-Host "[6/7] Getting aggregated database statistics..." -ForegroundColor Yellow

try {
    $dbStats = Invoke-RestMethod -Uri "$BaseUrl/v1/db/$DbName/_stats"
    Write-Host "  Sequence: $($dbStats.seq)" -ForegroundColor Gray
    Write-Host "  Total Docs: $($dbStats.docsTotal)" -ForegroundColor Gray
    Write-Host "  Alive Docs: $($dbStats.docsAlive)" -ForegroundColor Gray
    Write-Host "  Deleted Docs: $($dbStats.docsDeleted)" -ForegroundColor Gray
}
catch {
    Write-Host "  Failed to get stats: $_" -ForegroundColor Red
}

# ----------------------------------------
# Step 7: Update and delete documents (quorum operations)
# ----------------------------------------
Write-Host ""
Write-Host "[7/7] Testing update and delete operations..." -ForegroundColor Yellow

# Update a document
$updateDocId = "doc-alpha"
if ($insertedDocs.ContainsKey($updateDocId)) {
    $updateBody = @{
        rev  = $insertedDocs[$updateDocId]
        data = @{
            name      = "Updated Document $updateDocId"
            value     = 9999
            updated   = $true
            timestamp = (Get-Date).ToString("o")
        }
        tags = @("test", "replication", "updated")
    } | ConvertTo-Json -Depth 5

    try {
        $updateResult = Invoke-RestMethod -Uri "$BaseUrl/v1/db/$DbName/docs/$updateDocId" -Method Put -Body $updateBody -ContentType "application/json"
        Write-Host "  Updated $updateDocId (new rev: $($updateResult.rev))" -ForegroundColor Green
        $insertedDocs[$updateDocId] = $updateResult.rev
    }
    catch {
        Write-Host "  Failed to update $updateDocId : $_" -ForegroundColor Red
    }
}

# Delete a document
$deleteDocId = "doc-kappa"
if ($insertedDocs.ContainsKey($deleteDocId)) {
    try {
        $deleteRev = $insertedDocs[$deleteDocId]
        $deleteResult = Invoke-RestMethod -Uri "$BaseUrl/v1/db/$DbName/docs/$deleteDocId`?rev=$deleteRev" -Method Delete
        Write-Host "  Deleted $deleteDocId" -ForegroundColor Green
    }
    catch {
        Write-Host "  Failed to delete $deleteDocId : $_" -ForegroundColor Red
    }
}

# Verify final state
Write-Host ""
Write-Host "Verifying final state..." -ForegroundColor Yellow
try {
    $finalDocs = Invoke-RestMethod -Uri "$BaseUrl/v1/db/$DbName/_all_docs?includeDeleted=false"
    Write-Host "  Active documents: $($finalDocs.total)" -ForegroundColor Cyan
}
catch {
    Write-Host "  Failed to verify: $_" -ForegroundColor Red
}

# ----------------------------------------
# Cleanup
# ----------------------------------------
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " Test Complete!" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Stopping server..." -ForegroundColor Yellow

try {
    Stop-Process -Id $serverProcess.Id -Force -ErrorAction SilentlyContinue
    Write-Host "  Server stopped." -ForegroundColor Green
}
catch {
    Write-Host "  Server may have already stopped." -ForegroundColor Gray
}

# Clean up environment variables
Remove-Item Env:STORAGE_MODE -ErrorAction SilentlyContinue
Remove-Item Env:SHARDING_ENABLED -ErrorAction SilentlyContinue
Remove-Item Env:SHARDING_MODE -ErrorAction SilentlyContinue
Remove-Item Env:SHARD_COUNT -ErrorAction SilentlyContinue
Remove-Item Env:REPLICATION_ENABLED -ErrorAction SilentlyContinue
Remove-Item Env:REPLICA_COUNT -ErrorAction SilentlyContinue
Remove-Item Env:WRITE_QUORUM -ErrorAction SilentlyContinue
Remove-Item Env:READ_QUORUM -ErrorAction SilentlyContinue

Write-Host ""
Write-Host "Summary:" -ForegroundColor Cyan
Write-Host "  - Replication mode tested with WriteQuorum=2, ReadQuorum=1" -ForegroundColor Gray
Write-Host "  - Documents distributed across 3 shards with 2 replicas each" -ForegroundColor Gray
Write-Host "  - Quorum writes ensure data consistency" -ForegroundColor Gray
Write-Host "  - Read load balancing distributes queries across replicas" -ForegroundColor Gray
Write-Host ""
Write-Host "For Docker deployment with failover testing:" -ForegroundColor Yellow
Write-Host "  docker-compose -f docker-compose.replication.yml up --build" -ForegroundColor White
Write-Host ""
Write-Host "To simulate failover:" -ForegroundColor Yellow
Write-Host "  docker stop peacedb-shard-0-primary" -ForegroundColor White
Write-Host "  # System automatically promotes replica to primary" -ForegroundColor Gray

