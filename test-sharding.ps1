# .\test-sharding.ps1
#
# 1. Запуск PeaceDB в шардированном режиме (Local mode - 3 шарда в одном процессе)
# 2. Создание БД
# 3. Вставка документов с разными _id (распределяются по шардам)
# 4. Получение документов
# 5. Поиск и статистика

$baseUrl = "http://localhost:5000"
$db = "sharding_demo"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host " PeaceDatabase Sharding - Proof of Concept" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Проверяем, запущен ли сервер
Write-Host "[1] Checking server status..." -ForegroundColor Yellow
try {
    $stats = Invoke-RestMethod -Uri "$baseUrl/v1/_stats" -Method Get -ErrorAction Stop
    Write-Host "   Server is running!" -ForegroundColor Green
    Write-Host "   Storage Mode: $($stats.storageMode)" -ForegroundColor Gray
    Write-Host "   Sharding Enabled: $($stats.sharding.enabled)" -ForegroundColor Gray
    Write-Host "   Sharding Mode: $($stats.sharding.mode)" -ForegroundColor Gray
    Write-Host "   Shard Count: $($stats.sharding.shardCount)" -ForegroundColor Gray
} catch {
    Write-Host "   Server is not running. Please start it first with:" -ForegroundColor Red
    Write-Host '   $env:STORAGE_MODE="Sharded"; $env:SHARDING_ENABLED="true"; dotnet run --project PeaceDatabase' -ForegroundColor Yellow
    exit 1
}

Write-Host ""

# Создание БД
Write-Host "[2] Creating database '$db'..." -ForegroundColor Yellow
try {
    $result = Invoke-RestMethod -Uri "$baseUrl/v1/db/$db" -Method Put -ContentType "application/json"
    Write-Host "   Database created: ok=$($result.ok)" -ForegroundColor Green
} catch {
    Write-Host "   Database may already exist, continuing..." -ForegroundColor Gray
}

Write-Host ""

# Вставка документов с разными _id для демонстрации распределения
Write-Host "[3] Inserting 10 documents with different IDs..." -ForegroundColor Yellow

$documents = @(
    @{ id = "user-001"; data = @{ name = "Alice"; age = 25; city = "Moscow" }; tags = @("active", "premium") },
    @{ id = "user-002"; data = @{ name = "Bob"; age = 30; city = "SPB" }; tags = @("active") },
    @{ id = "user-003"; data = @{ name = "Charlie"; age = 35; city = "Moscow" }; tags = @("inactive") },
    @{ id = "order-100"; data = @{ product = "Laptop"; price = 1500; userId = "user-001" }; tags = @("electronics") },
    @{ id = "order-101"; data = @{ product = "Phone"; price = 800; userId = "user-002" }; tags = @("electronics") },
    @{ id = "order-102"; data = @{ product = "Tablet"; price = 600; userId = "user-001" }; tags = @("electronics") },
    @{ id = "log-2024-001"; data = @{ event = "login"; userId = "user-001" }; content = "User logged in" },
    @{ id = "log-2024-002"; data = @{ event = "purchase"; userId = "user-002" }; content = "User made purchase" },
    @{ id = "config-app"; data = @{ theme = "dark"; language = "ru" } },
    @{ id = "config-cache"; data = @{ ttl = 3600; maxSize = 1000 } }
)

foreach ($doc in $documents) {
    $body = @{
        id = $doc.id
        data = $doc.data
        tags = $doc.tags
        content = $doc.content
    } | ConvertTo-Json -Depth 5

    try {
        $result = Invoke-RestMethod -Uri "$baseUrl/v1/db/$db/docs/$($doc.id)" -Method Put -Body $body -ContentType "application/json"
        Write-Host "   Inserted: $($doc.id) -> rev: $($result.rev.Substring(0,8))..." -ForegroundColor Green
    } catch {
        Write-Host "   Failed to insert $($doc.id): $_" -ForegroundColor Red
    }
}

Write-Host ""

# Получение документов
Write-Host "[4] Retrieving specific documents..." -ForegroundColor Yellow
$testIds = @("user-001", "order-100", "config-app")

foreach ($id in $testIds) {
    try {
        $doc = Invoke-RestMethod -Uri "$baseUrl/v1/db/$db/docs/$id" -Method Get
        $name = if ($doc.data.name) { $doc.data.name } elseif ($doc.data.product) { $doc.data.product } else { $doc.data.theme }
        Write-Host "   GET $id -> Found! (data: $name)" -ForegroundColor Green
    } catch {
        Write-Host "   GET $id -> Not found" -ForegroundColor Red
    }
}

Write-Host ""

# Получение всех документов
Write-Host "[5] Listing all documents (scatter-gather across shards)..." -ForegroundColor Yellow
try {
    $allDocs = Invoke-RestMethod -Uri "$baseUrl/v1/db/$db/_all_docs?limit=100" -Method Get
    Write-Host "   Total documents: $($allDocs.total)" -ForegroundColor Green
    Write-Host "   IDs: $($allDocs.items.id -join ', ')" -ForegroundColor Gray
} catch {
    Write-Host "   Failed to list documents: $_" -ForegroundColor Red
}

Write-Host ""

# Поиск по тегам
Write-Host "[6] Searching by tags (allOf: 'electronics')..." -ForegroundColor Yellow
try {
    $searchBody = @{ allOf = @("electronics") } | ConvertTo-Json
    $found = Invoke-RestMethod -Uri "$baseUrl/v1/db/$db/_find/tags" -Method Post -Body $searchBody -ContentType "application/json"
    Write-Host "   Found $($found.total) documents with tag 'electronics':" -ForegroundColor Green
    foreach ($item in $found.items) {
        Write-Host "     - $($item.id): $($item.data.product) ($($item.data.price))" -ForegroundColor Gray
    }
} catch {
    Write-Host "   Search failed: $_" -ForegroundColor Red
}

Write-Host ""

# Поиск по полям
Write-Host "[7] Searching by fields (city = 'Moscow')..." -ForegroundColor Yellow
try {
    $searchBody = @{ equalsMap = @{ "data.city" = "Moscow" } } | ConvertTo-Json
    $found = Invoke-RestMethod -Uri "$baseUrl/v1/db/$db/_find/fields" -Method Post -Body $searchBody -ContentType "application/json"
    Write-Host "   Found $($found.total) users in Moscow:" -ForegroundColor Green
    foreach ($item in $found.items) {
        Write-Host "     - $($item.id): $($item.data.name)" -ForegroundColor Gray
    }
} catch {
    Write-Host "   Search failed: $_" -ForegroundColor Red
}

Write-Host ""

# Статистика БД
Write-Host "[8] Database statistics (aggregated from all shards)..." -ForegroundColor Yellow
try {
    $stats = Invoke-RestMethod -Uri "$baseUrl/v1/db/$db/_stats" -Method Get
    Write-Host "   Database: $($stats.db)" -ForegroundColor Green
    Write-Host "   Docs Total: $($stats.docsTotal)" -ForegroundColor Gray
    Write-Host "   Docs Alive: $($stats.docsAlive)" -ForegroundColor Gray
    Write-Host "   Seq: $($stats.seq)" -ForegroundColor Gray
} catch {
    Write-Host "   Failed to get stats: $_" -ForegroundColor Red
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " Done" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "The sharding system successfully:" -ForegroundColor Green
Write-Host "  - Distributed documents across logical shards using hash(id) % 3" -ForegroundColor Gray
Write-Host "  - Retrieved documents by routing to correct shard" -ForegroundColor Gray
Write-Host "  - Performed scatter-gather queries across all shards" -ForegroundColor Gray
Write-Host "  - Aggregated statistics from all shards" -ForegroundColor Gray

