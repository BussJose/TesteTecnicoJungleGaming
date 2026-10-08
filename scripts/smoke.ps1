# Teste rápido de ponta a ponta (PowerShell). Requer: docker compose up --build
# Uso:  powershell -ExecutionPolicy Bypass -File scripts/smoke.ps1
$ErrorActionPreference = "Stop"
$api = "http://localhost:8080"
$kc  = "http://localhost:8081/realms/wager/protocol/openid-connect/token"

function Get-Token($client, $secret) {
    (Invoke-RestMethod -Method Post -Uri $kc -Body @{
        grant_type = "client_credentials"; client_id = $client; client_secret = $secret }).access_token
}

# Chama a API e mostra o status HTTP e o corpo (inclusive em erros 4xx).
function Call($method, $path, $token, $body = $null, $key = $null) {
    $headers = @{ Authorization = "Bearer $token" }
    if ($key) { $headers["Idempotency-Key"] = $key }
    try {
        $params = @{ Method = $method; Uri = "$api$path"; Headers = $headers; ContentType = "application/json"; UseBasicParsing = $true }
        if ($body) { $params["Body"] = $body }
        $r = Invoke-WebRequest @params
        $status = [int]$r.StatusCode; $text = $r.Content
    } catch {
        $status = [int]$_.Exception.Response.StatusCode
        $text = $_.ErrorDetails.Message
    }
    Write-Host ("{0} {1} -> {2}" -f $method, $path, $status) -ForegroundColor Cyan
    Write-Host $text
    if ($text) { return ($text | ConvertFrom-Json) }
}

$internal = Get-Token "wager-internal" "internal-secret"
$provA    = Get-Token "provider-a" "provider-a-secret"
$player   = [guid]::NewGuid().ToString()

Write-Host "`n== 1. Abrir carteira com 100.00 ==" -ForegroundColor Yellow
$w = Call "POST" "/wallets" $internal ('{"playerId":"' + $player + '","initialBalance":{"amount":"100.00","currency":"BRL"}}')

function Bet($ext, $kind, $amount, $key, $ref = $null) {
    $b = @{ externalTransactionId = $ext; playerId = $player; walletId = $w.id; roundId = "r1"; gameId = "g1"
            kind = $kind; money = @{ amount = $amount; currency = "BRL" } }
    if ($ref) { $b["referenceExternalTransactionId"] = $ref }
    Call "POST" "/wagering/transactions" $provA ($b | ConvertTo-Json -Compress) $key
}

Write-Host "`n== 2. Aposta de 30.00 ==" -ForegroundColor Yellow
Bet "bet-1" "BET" "30.00" "key-bet-1" | Out-Null
Write-Host "`n== 3. Mesma requisição de novo (replay idempotente) ==" -ForegroundColor Yellow
Bet "bet-1" "BET" "30.00" "key-bet-1" | Out-Null
Write-Host "`n== 4. Mesma chave com valor diferente (409) ==" -ForegroundColor Yellow
Bet "bet-1" "BET" "31.00" "key-bet-1" | Out-Null
Write-Host "`n== 5. Aposta maior que o saldo (422 INSUFFICIENT_FUNDS) ==" -ForegroundColor Yellow
Bet "bet-2" "BET" "500.00" "key-bet-2" | Out-Null
Write-Host "`n== 6. Estorno antes da aposta existir (202 PENDING_REFERENCE) ==" -ForegroundColor Yellow
Bet "refund-late" "REFUND" "10.00" "key-refund-late" "bet-3" | Out-Null
Write-Host "`n== 7. A aposta chega (o worker conclui o estorno em ~1s) ==" -ForegroundColor Yellow
Bet "bet-3" "BET" "10.00" "key-bet-3" | Out-Null
Start-Sleep -Seconds 3
Call "GET" "/providers/provider-a/wagering/transactions/refund-late" $provA | Out-Null
Write-Host "`n== 8. Saldo, ledger e conciliação ==" -ForegroundColor Yellow
Call "GET" "/wallets/$($w.id)" $internal | Out-Null
Call "GET" "/wallets/$($w.id)/ledger" $internal | Out-Null
Call "POST" "/wallets/$($w.id)/reconciliation" $internal | Out-Null
Write-Host "`n== 9. Sem token (401) e provedor tentando ler carteira (403) ==" -ForegroundColor Yellow
try { Invoke-WebRequest -UseBasicParsing "$api/wallets/$($w.id)" | Out-Null } catch { Write-Host "sem token -> $([int]$_.Exception.Response.StatusCode)" }
Call "GET" "/wallets/$($w.id)" $provA | Out-Null
