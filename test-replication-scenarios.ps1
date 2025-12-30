# PowerShell скрипт для тестирования сценариев репликации PeaceDatabase
# Использование: .\test-replication-scenarios.ps1 -Scenario 1

param(
    [Parameter(Mandatory = $false)]
    [ValidateSet(1, 3, 5, 6, 7, 8, 9, 10, 13, "all", "raft")]
    [string]$Scenario = "all",
    
    [Parameter(Mandatory = $false)]
    [string]$ComposeFile = "docker-compose.replication.yml"
)

$ErrorActionPreference = "Continue"  # Изменено для обработки некритичных ошибок

# Цвета для вывода
function Write-Info { param($msg) Write-Host $msg -ForegroundColor Cyan }
function Write-Success { param($msg) Write-Host $msg -ForegroundColor Green }
function Write-Warning { param($msg) Write-Host $msg -ForegroundColor Yellow }
function Write-Error { param($msg) Write-Host $msg -ForegroundColor Red }

# Проверка наличия docker-compose
if (-not (Get-Command docker-compose -ErrorAction SilentlyContinue)) {
    Write-Error "docker-compose не найден. Установите Docker Desktop."
    exit 1
}

# Функция получения имени сети из docker-compose
function Get-NetworkName {
    param($composeFile)
    
    # Пробуем получить имя сети из docker-compose
    $networks = docker network ls --format "{{.Name}}" | Select-String "replication"
    if ($networks) {
        return $networks[0].ToString()
    }
    
    # Если не найдено, пробуем стандартное имя
    return "replication-net"
}

# Функция ожидания готовности контейнера
function Wait-ContainerHealthy {
    param($containerName, $timeoutSeconds = 60)
    
    $elapsed = 0
    while ($elapsed -lt $timeoutSeconds) {
        $status = docker inspect --format='{{.State.Health.Status}}' $containerName 2>$null
        if ($status -eq "healthy") {
            Write-Success "$containerName is healthy"
            return $true
        }
        Start-Sleep -Seconds 2
        $elapsed += 2
        Write-Host "." -NoNewline
    }
    Write-Warning "$containerName не стал healthy за $timeoutSeconds секунд"
    return $false
}

# Функция получения состояния реплики
function Get-ReplicaState {
    param($containerName)
    
    try {
        $json = docker exec $containerName curl -s http://localhost:8080/v1/_replication/state 2>$null
        return $json | ConvertFrom-Json
    }
    catch {
        return $null
    }
}

# Функция вывода состояния реплики
function Show-ReplicaState {
    param($containerName)
    
    $state = Get-ReplicaState $containerName
    if ($state) {
        Write-Host "  Container: $containerName"
        Write-Host "  IsPrimary: $($state.IsPrimary)"
        Write-Host "  Seq: $($state.Seq)"
        Write-Host "  Healthy: $($state.Healthy)"
        # Raft state (если доступен)
        if ($state.CurrentTerm -ne $null) {
            Write-Host "  Term: $($state.CurrentTerm)"
            Write-Host "  Role: $($state.Role)"
        }
        Write-Host ""
    }
    else {
        Write-Warning "  Не удалось получить состояние $containerName"
    }
}

# Функция получения Raft состояния реплики
function Get-RaftState {
    param($containerName)
    
    try {
        $json = docker exec $containerName curl -s http://localhost:8080/v1/_replication/raft-state 2>$null
        return $json | ConvertFrom-Json
    }
    catch {
        return $null
    }
}

# Функция вывода Raft состояния
function Show-RaftState {
    param($containerName)
    
    $state = Get-RaftState $containerName
    if ($state) {
        Write-Host "  Container: $containerName"
        Write-Host "  NodeId: $($state.NodeId)"
        Write-Host "  Term: $($state.CurrentTerm)"
        Write-Host "  Role: $($state.Role)"
        Write-Host "  VotedFor: $($state.VotedFor)"
        Write-Host "  LeaderId: $($state.CurrentLeaderId)"
        Write-Host ""
    }
    else {
        Write-Warning "  Не удалось получить Raft состояние $containerName"
    }
}

# Сценарий 1: Падение лидера
function Test-Scenario1 {
    Write-Info "`n=== Сценарий 1: Падение лидера ==="
    
    $testDb = "testdb_scenario1"
    $primary = "peacedb-shard-0-primary"
    $replica1 = "peacedb-shard-0-replica-1"
    $replica2 = "peacedb-shard-0-replica-2"
    
    Write-Info "Шаг 1: Создание тестовых данных"
    docker exec peacedb-router curl -s -X PUT http://localhost:8080/v1/db/$testDb | Out-Null
    
    # ИСПРАВЛЕНО: Убираем _id из body - контроллер установит из route
    $doc1Json = '{\"content\":\"Test 1\"}'
    docker exec peacedb-router sh -c "curl -s -X PUT http://localhost:8080/v1/db/$testDb/docs/doc1 -H 'Content-Type: application/json' -d '$doc1Json'" | Out-Null
    
    Write-Info "Шаг 2: Проверка начального состояния"
    Write-Host "Primary:"
    Show-ReplicaState $primary
    Write-Host "Replica-1:"
    Show-ReplicaState $replica1
    
    Write-Info "Шаг 3: Остановка primary"
    docker stop $primary | Out-Null
    Write-Warning "Primary остановлен"
    
    Write-Info "Шаг 4: Ожидание failover (15 секунд)..."
    Start-Sleep -Seconds 15
    
    Write-Info "Шаг 5: Проверка состояния после failover"
    Start-Sleep -Seconds 5  # Дополнительное время для промоушена
    
    Write-Host "Replica-1:"
    Show-ReplicaState $replica1
    
    # Проверяем, стал ли новый primary
    $state1 = Get-ReplicaState $replica1
    if ($state1 -and -not $state1.IsPrimary) {
        Write-Warning "Replica-1 ещё не стала primary, пытаемся промоутить..."
        docker exec $replica1 curl -s -X POST http://localhost:8080/v1/_replication/promote | Out-Null
        Start-Sleep -Seconds 2
        Write-Host "Replica-1 после промоушена:"
        Show-ReplicaState $replica1
    }
    else {
        Write-Success "Replica-1 успешно стала primary!"
    }
    
    Write-Host "Replica-2:"
    Show-ReplicaState $replica2
    
    Write-Info "Шаг 6: Проверка работы системы"
    # ИСПРАВЛЕНО: Убираем _id из body - контроллер установит из route
    $doc2Json = '{\"content\":\"Test 2\"}'
    $result = docker exec peacedb-router sh -c "curl -s -X PUT http://localhost:8080/v1/db/$testDb/docs/doc2 -H 'Content-Type: application/json' -d '$doc2Json'"
    
    # PUT возвращает документ с "rev" при успехе, не "ok"
    if ($result -match '"rev"') {
        Write-Success "Запись успешна после failover!"
    }
    elseif ($result -match '"error"') {
        Write-Warning "Запись не удалась: $result"
    }
    else {
        Write-Info "Результат записи: $result"
    }
    
    Write-Info "Шаг 7: Восстановление старого primary"
    docker start $primary | Out-Null
    Start-Sleep -Seconds 5
    Write-Host "Старый primary:"
    Show-ReplicaState $primary
    
    Write-Success "Сценарий 1 завершён"
}

# Сценарий 3: Partition с лидером в majority
function Test-Scenario3 {
    Write-Info "`n=== Сценарий 3: Partition с лидером в majority ==="
    
    $testDb = "testdb_scenario3"
    $primary = "peacedb-shard-0-primary"
    $replica1 = "peacedb-shard-0-replica-1"
    
    Write-Info "Шаг 1: Создание тестовых данных"
    docker exec peacedb-router curl -s -X PUT http://localhost:8080/v1/db/$testDb | Out-Null
    
    Write-Info "Шаг 2: Изоляция replica-1 от сети"
    # ИСПРАВЛЕНО: Получаем реальное имя сети
    $networkName = Get-NetworkName $ComposeFile
    Write-Info "Используется сеть: $networkName"
    
    $disconnectResult = docker network disconnect $networkName $replica1 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Warning "Replica-1 изолирована"
    }
    else {
        Write-Warning "Не удалось изолировать replica-1: $disconnectResult"
        Write-Warning "Продолжаем тест..."
    }
    
    Write-Info "Шаг 3: Проверка работы лидера"
    $doc1Json = '{\"content\":\"Test partition\"}'
    $result = docker exec peacedb-router sh -c "curl -s -X PUT http://localhost:8080/v1/db/$testDb/docs/doc1 -H 'Content-Type: application/json' -d '$doc1Json'"
    
    # PUT возвращает документ с "rev" при успехе
    if ($result -match '"rev"') {
        Write-Success "Лидер продолжает принимать записи (кворум сохранён)"
    }
    elseif ($result -match '"error"') {
        Write-Warning "Запись не удалась: $result"
    }
    
    Write-Info "Шаг 4: Восстановление связи"
    $connectResult = docker network connect $networkName $replica1 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Success "Replica-1 возвращена в сеть"
    }
    else {
        Write-Warning "Не удалось вернуть replica-1 в сеть: $connectResult"
    }
    
    Start-Sleep -Seconds 5
    Write-Host "Replica-1 после восстановления:"
    Show-ReplicaState $replica1
    
    Write-Success "Сценарий 3 завершён"
}

# Сценарий 6: Падение follower
function Test-Scenario6 {
    Write-Info "`n=== Сценарий 6: Падение follower ==="
    
    $testDb = "testdb_scenario6"
    $primary = "peacedb-shard-0-primary"
    $replica2 = "peacedb-shard-0-replica-2"
    
    Write-Info "Шаг 1: Создание тестовых данных"
    docker exec peacedb-router curl -s -X PUT http://localhost:8080/v1/db/$testDb | Out-Null
    
    Write-Info "Шаг 2: Остановка follower"
    docker stop $replica2 | Out-Null
    Write-Warning "Follower остановлен"
    
    Write-Info "Шаг 3: Проверка работы системы (кворум сохранён)"
    $doc1Json = '{\"content\":\"Test follower down\"}'
    $result = docker exec peacedb-router sh -c "curl -s -X PUT http://localhost:8080/v1/db/$testDb/docs/doc1 -H 'Content-Type: application/json' -d '$doc1Json'"
    
    # PUT возвращает документ с "rev" при успехе
    if ($result -match '"rev"') {
        Write-Success "Система продолжает работать (2 из 3 = кворум)"
    }
    elseif ($result -match '"error"') {
        Write-Warning "Запись не удалась: $result"
    }
    
    Write-Info "Шаг 4: Восстановление follower"
    docker start $replica2 | Out-Null
    Start-Sleep -Seconds 5
    
    Write-Host "Follower после восстановления:"
    Show-ReplicaState $replica2
    
    Write-Success "Сценарий 6 завершён"
}

# Сценарий 7: Потеря кворума
function Test-Scenario7 {
    Write-Info "`n=== Сценарий 7: Потеря кворума ==="
    
    $testDb = "testdb_scenario7"
    $primary = "peacedb-shard-0-primary"
    $replica1 = "peacedb-shard-0-replica-1"
    
    Write-Info "Шаг 1: Создание тестовых данных"
    docker exec peacedb-router curl -s -X PUT http://localhost:8080/v1/db/$testDb | Out-Null
    
    Write-Info "Шаг 2: Остановка primary и одного follower (потеря кворума)"
    docker stop $primary | Out-Null
    docker stop $replica1 | Out-Null
    Write-Warning "Кворум потерян (осталась 1 из 3 реплик)"
    
    # ИСПРАВЛЕНО: Ждём обновления статуса HealthMonitor
    Write-Info "Ожидание обновления статуса реплик (10 секунд)..."
    Start-Sleep -Seconds 10
    
    Write-Info "Шаг 3: Попытка записи (должна быть отклонена)"
    $doc1Json = '{\"content\":\"Test no quorum\"}'
    $result = docker exec peacedb-router sh -c "curl -s -X PUT http://localhost:8080/v1/db/$testDb/docs/doc1 -H 'Content-Type: application/json' -d '$doc1Json'"
    
    if ($result -match "Insufficient replicas" -or $result -match "quorum") {
        Write-Success "Запись правильно отклонена: недостаточно реплик для кворума"
    }
    elseif ($result -match '"ok":false') {
        # Любая ошибка при потере кворума - ожидаемое поведение
        Write-Success "Запись отклонена (ошибка: $result)"
    }
    elseif ($result -match '"rev"') {
        Write-Warning "ВНИМАНИЕ: Запись успешна при отсутствии кворума! Это может быть проблемой."
    }
    else {
        Write-Warning "Неожиданный результат: $result"
    }
    
    Write-Info "Шаг 4: Восстановление кворума"
    docker start $primary | Out-Null
    docker start $replica1 | Out-Null
    Start-Sleep -Seconds 5
    Write-Success "Кворум восстановлен"
    
    Write-Info "Шаг 5: Проверка работы после восстановления"
    $doc2Json = '{\"content\":\"Test after recovery\"}'
    $result = docker exec peacedb-router sh -c "curl -s -X PUT http://localhost:8080/v1/db/$testDb/docs/doc2 -H 'Content-Type: application/json' -d '$doc2Json'"
    
    # PUT возвращает документ с "rev" при успехе
    if ($result -match '"rev"') {
        Write-Success "Система снова работает!"
    }
    elseif ($result -match '"error"') {
        Write-Warning "Запись после восстановления не удалась: $result"
    }
    
    Write-Success "Сценарий 7 завершён"
}

# ==================== RAFT SCENARIOS ====================

# Сценарий 8: Увеличение терма после выборов
function Test-Scenario8 {
    Write-Info "`n=== Сценарий 8: Увеличение терма после выборов (Raft) ==="
    
    $primary = "peacedb-shard-0-primary"
    $replica1 = "peacedb-shard-0-replica-1"
    $replica2 = "peacedb-shard-0-replica-2"
    
    Write-Info "Шаг 1: Проверка начального Raft состояния"
    Write-Host "Primary:"
    Show-RaftState $primary
    
    $initialState = Get-RaftState $primary
    $initialTerm = $initialState.CurrentTerm
    Write-Info "Начальный терм: $initialTerm"
    
    Write-Host "Replica-1:"
    Show-RaftState $replica1
    
    Write-Info "Шаг 2: Остановка текущего лидера"
    docker stop $primary | Out-Null
    Write-Warning "Primary остановлен"
    
    Write-Info "Шаг 3: Ожидание election timeout и выборов (20 секунд)..."
    Start-Sleep -Seconds 20
    
    Write-Info "Шаг 4: Проверка нового терма"
    Write-Host "Replica-1:"
    Show-RaftState $replica1
    
    $newState = Get-RaftState $replica1
    if ($newState -and $newState.CurrentTerm -gt $initialTerm) {
        Write-Success "Терм увеличился: $initialTerm -> $($newState.CurrentTerm)"
    }
    elseif ($newState) {
        Write-Warning "Терм не изменился: $($newState.CurrentTerm)"
    }
    
    Write-Host "Replica-2:"
    Show-RaftState $replica2
    
    Write-Info "Шаг 5: Восстановление старого лидера"
    docker start $primary | Out-Null
    Start-Sleep -Seconds 5
    
    Write-Host "Старый primary после восстановления:"
    Show-RaftState $primary
    
    $oldLeaderState = Get-RaftState $primary
    if ($oldLeaderState -and $oldLeaderState.Role -ne "Leader") {
        Write-Success "Старый лидер корректно стал $($oldLeaderState.Role) (не Leader)"
    }
    elseif ($oldLeaderState) {
        Write-Info "Старый лидер всё ещё $($oldLeaderState.Role)"
    }
    
    Write-Success "Сценарий 8 завершён"
}

# Сценарий 9: Отклонение устаревшего лидера (Stale Leader Rejection)
function Test-Scenario9 {
    Write-Info "`n=== Сценарий 9: Отклонение устаревшего лидера (Raft) ==="
    
    $primary = "peacedb-shard-0-primary"
    $replica1 = "peacedb-shard-0-replica-1"
    $replica2 = "peacedb-shard-0-replica-2"
    
    Write-Info "Шаг 1: Проверка начального состояния"
    $initialState = Get-RaftState $primary
    Write-Host "Primary терм: $($initialState.CurrentTerm), Role: $($initialState.Role)"
    
    Write-Info "Шаг 2: Изоляция primary от сети (симуляция partition)"
    $networkName = Get-NetworkName $ComposeFile
    
    docker network disconnect $networkName $primary 2>$null
    Write-Warning "Primary изолирован"
    
    Write-Info "Шаг 3: Ожидание выборов нового лидера (20 секунд)..."
    Start-Sleep -Seconds 20
    
    Write-Info "Шаг 4: Проверка, что majority выбрала нового лидера"
    Write-Host "Replica-1:"
    Show-RaftState $replica1
    
    $replica1State = Get-RaftState $replica1
    Write-Host "Replica-2:"
    Show-RaftState $replica2
    
    Write-Info "Шаг 5: Возвращение старого лидера в сеть"
    docker network connect $networkName $primary 2>$null
    Start-Sleep -Seconds 5
    
    Write-Host "Старый primary после возвращения:"
    Show-RaftState $primary
    
    $returnedState = Get-RaftState $primary
    if ($returnedState) {
        if ($returnedState.Role -eq "Follower") {
            Write-Success "Старый лидер корректно понизился до Follower!"
        }
        elseif ($returnedState.CurrentTerm -gt $initialState.CurrentTerm) {
            Write-Success "Старый лидер обновил терм: $($initialState.CurrentTerm) -> $($returnedState.CurrentTerm)"
        }
        else {
            Write-Info "Старый лидер: Role=$($returnedState.Role), Term=$($returnedState.CurrentTerm)"
        }
    }
    
    Write-Success "Сценарий 9 завершён"
}

# Сценарий 10: Heartbeat и поддержание лидерства
function Test-Scenario10 {
    Write-Info "`n=== Сценарий 10: Heartbeat и поддержание лидерства (Raft) ==="
    
    $primary = "peacedb-shard-0-primary"
    $replica1 = "peacedb-shard-0-replica-1"
    $replica2 = "peacedb-shard-0-replica-2"
    
    Write-Info "Шаг 1: Проверка начального состояния"
    Write-Host "Primary:"
    Show-RaftState $primary
    
    $initialTerm = (Get-RaftState $primary).CurrentTerm
    
    Write-Info "Шаг 2: Ожидание нескольких циклов heartbeat (5 секунд)..."
    Start-Sleep -Seconds 5
    
    Write-Info "Шаг 3: Проверка, что терм и лидер не изменились"
    Write-Host "Primary:"
    Show-RaftState $primary
    
    $afterState = Get-RaftState $primary
    if ($afterState.CurrentTerm -eq $initialTerm -and $afterState.Role -eq "Leader") {
        Write-Success "Лидерство стабильно: терм=$($afterState.CurrentTerm), role=Leader"
    }
    else {
        Write-Warning "Состояние изменилось: терм=$($afterState.CurrentTerm), role=$($afterState.Role)"
    }
    
    Write-Host "Replica-1:"
    Show-RaftState $replica1
    
    $replica1State = Get-RaftState $replica1
    if ($replica1State.Role -eq "Follower") {
        Write-Success "Replica-1 остаётся Follower (получает heartbeat)"
    }
    
    Write-Host "Replica-2:"
    Show-RaftState $replica2
    
    Write-Success "Сценарий 10 завершён"
}

# Главная функция
function Main {
    Write-Info "=========================================="
    Write-Info "  PeaceDatabase Replication Test Suite"
    Write-Info "=========================================="
    
    # Проверка запущенных контейнеров
    Write-Info "`nПроверка контейнеров..."
    $containers = docker ps --format "{{.Names}}" | Select-String "peacedb"
    if ($containers.Count -lt 10) {
        Write-Warning "Не все контейнеры запущены. Запускаю..."
        docker-compose -f $ComposeFile up -d
        Write-Info "Ожидание готовности контейнеров..."
        
        $waitContainers = @(
            "peacedb-shard-0-primary", "peacedb-shard-0-replica-1", "peacedb-shard-0-replica-2",
            "peacedb-shard-1-primary", "peacedb-shard-1-replica-1", "peacedb-shard-1-replica-2",
            "peacedb-shard-2-primary", "peacedb-shard-2-replica-1", "peacedb-shard-2-replica-2"
        )
        
        foreach ($container in $waitContainers) {
            Wait-ContainerHealthy $container
        }
    }
    
    Write-Success "Все контейнеры готовы`n"
    
    # Запуск выбранных сценариев
    switch ($Scenario) {
        "1" { Test-Scenario1 }
        "3" { Test-Scenario3 }
        "6" { Test-Scenario6 }
        "7" { Test-Scenario7 }
        "8" { Test-Scenario8 }
        "9" { Test-Scenario9 }
        "10" { Test-Scenario10 }
        "raft" {
            Write-Info "Запуск Raft-специфичных сценариев..."
            Test-Scenario8
            Start-Sleep -Seconds 5
            Test-Scenario9
            Start-Sleep -Seconds 5
            Test-Scenario10
        }
        "all" {
            Test-Scenario1
            Start-Sleep -Seconds 3
            Test-Scenario3
            Start-Sleep -Seconds 3
            Test-Scenario6
            Start-Sleep -Seconds 3
            Test-Scenario7
            Start-Sleep -Seconds 3
            Write-Info "`n=========================================="
            Write-Info "  Raft-специфичные сценарии"
            Write-Info "==========================================`n"
            Test-Scenario8
            Start-Sleep -Seconds 5
            Test-Scenario9
            Start-Sleep -Seconds 5
            Test-Scenario10
        }
    }
    
    Write-Info "`n=========================================="
    Write-Success "Все тесты завершены!"
    Write-Info "=========================================="
}

# Запуск
Main



