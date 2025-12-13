Write-Host "`n[1] Inserting 'doc-failover' before simulated failure..." -ForegroundColor Cyan
$body = '{"id":"doc-failover","data":{"stage":"before-failover"}}'
$result1 = Invoke-RestMethod -Uri "http://localhost:5000/v1/db/demo2/docs/doc-failover" -Method Put -Body $body -ContentType "application/json"
Write-Host "Insert 1: $($result1 | ConvertTo-Json)"

Write-Host "`n[2] Reading document to confirm insert before failure..." -ForegroundColor Cyan
Get-Content "curl http://localhost:5000/v1/db/demo2/docs/doc-failover" | iex

Write-Host "`n[3] Simulating PRIMARY FAILURE! Stopping service for 8 seconds..." -ForegroundColor Red
Stop-Process -Name dotnet -Force
Start-Sleep -Seconds 8

Write-Host "`n[4] Restarting service (simulates failover and primary election)..." -ForegroundColor Yellow
Start-Process -NoNewWindow -FilePath "dotnet" -ArgumentList "run --urls http://localhost:5000"
Start-Sleep -Seconds 10

Write-Host "`n[5] Inserting document/reading after simulated failover..." -ForegroundColor Cyan
$body2 = '{"id":"doc-failover","data":{"stage":"after-failover"}}'
$result2 = Invoke-RestMethod -Uri "http://localhost:5000/v1/db/demo2/docs/doc-failover" -Method Put -Body $body2 -ContentType "application/json"
Write-Host "Insert 2: $($result2 | ConvertTo-Json)"

Write-Host "`n[6] Confirming document was updated and system is resilient!" -ForegroundColor Green
$final = Invoke-RestMethod -Uri "http://localhost:5000/v1/db/demo2/docs/doc-failover"
Write-Host "READ: $($final | ConvertTo-Json -Depth 5)"