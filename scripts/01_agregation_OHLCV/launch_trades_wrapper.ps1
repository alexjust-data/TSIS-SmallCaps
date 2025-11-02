# launch_trades_wrapper.ps1 - Launcher para descarga trades tick-level
# Descarga trades (premarket + market) con configuración conservadora

# Cargar .env
$envPath = "D:\TSIS_SmallCaps\.env"
if (Test-Path $envPath) {
    Get-Content $envPath | ForEach-Object {
        if ($_ -match '^\s*([^#][^=]+?)\s*=\s*(.+)\s*$') {
            $key = $matches[1].Trim()
            $val = $matches[2].Trim()
            [System.Environment]::SetEnvironmentVariable($key, $val, [System.EnvironmentVariableTarget]::Process)
        }
    }
}

# Verificar API KEY
$apiKey = [System.Environment]::GetEnvironmentVariable("POLYGON_API_KEY")
if (-not $apiKey -or $apiKey -eq "") {
    Write-Host "ERROR: POLYGON_API_KEY no configurada en .env" -ForegroundColor Red
    exit 1
}

Write-Host "OK - POLYGON_API_KEY configurada" -ForegroundColor Green
Write-Host ""

# Configuración del WRAPPER TRADES (conservadora - tickers líquidos generan MUCHO volumen)
$tickersCsv = "processed/universe/smallcaps_universe_2025-11-01.parquet"
$outdir = "raw/polygon/trades_ticks"
$dateFrom = "2019-01-01"
$dateTo = "2025-11-01"

# CONFIGURACIÓN OPTIMIZADA (trades tick-level = MUCHO más datos):
# - Micro-batches (15 tickers) - tickers líquidos generan millones de trades
# - Concurrencia balanceada (10 batches) - aprovecha throughput sin saturar
# - Rate limit agresivo (adaptativo ajustará automáticamente si hay 429s)
$batchSize = 15          # Micro-batches para no saturar memoria
$maxConcurrent = 10      # Balanceado (trades pesados pero queremos velocidad)
$rateLimit = 0.15        # Agresivo inicial (adaptativo: 0.12-0.40s)
$ingestScript = "scripts/01_agregation_OHLCV/ingest_trades_ticks.py"

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  WRAPPER TRADES TICK-LEVEL - TSIS SMALLCAPS (6,405 tickers)" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Configuración:"
Write-Host "  Universo:        $tickersCsv (6,405 tickers - Small Caps)"
Write-Host "  Periodo:         $dateFrom -> $dateTo (~7 años)"
Write-Host "  Batch size:      $batchSize tickers/batch"
Write-Host "  Concurrencia:    $maxConcurrent batches simultaneos (OPTIMIZADO)"
Write-Host "  Rate limit:      $rateLimit s/pag (adaptativo 0.12-0.40s)"
Write-Host "  PAGE_LIMIT:      50,000 rows/request"
Write-Host "  Compresion:      ZSTD level 3 (máxima para trades)"
Write-Host "  Descarga:        Por DIAS (evita JSONs gigantes en tickers líquidos)"
Write-Host "  Resume:          Activado (excluye tickers con datos)"
Write-Host ""
Write-Host "Optimizaciones activas:"
Write-Host "  - Descarga DIARIA: ~2,555 días (7 años)"
Write-Host "  - Rate-limit adaptativo: acelera/frena automaticamente"
Write-Host "  - PAGE_LIMIT 50K: reduce requests"
Write-Host "  - TLS heredado a subprocesos"
Write-Host "  - Compresion ZSTD level 3: máxima compresión"
Write-Host "  - Separación premarket/market: filtrado por timestamp"
Write-Host ""
Write-Host "Estimaciones (OPTIMIZADAS):"
Write-Host "  - Batches totales: ~427 batches (6,405 / 15)"
Write-Host "  - Tiempo estimado: ~9-12 horas (config agresiva + adaptativo)"
Write-Host "  - Velocidad esperada: ~450-600 t/h"
Write-Host ""
Write-Host "WARNING: Trades tick-level generan MUCHO más volumen que OHLCV"
Write-Host "         Tickers líquidos pueden tener millones de trades por día"
Write-Host ""
Write-Host "Iniciando descarga..."
Write-Host ""

# Configurar SSL/TLS certs (heredado a subprocesos via env=env)
$cert = & python -c "import certifi; print(certifi.where())" 2>$null
if ($LASTEXITCODE -eq 0 -and $cert) {
    $env:SSL_CERT_FILE = $cert
    $env:REQUESTS_CA_BUNDLE = $cert
    Write-Host "SSL_CERT_FILE: $cert" -ForegroundColor Green
} else {
    Write-Host "Warning: certifi no encontrado, usando certs del sistema" -ForegroundColor Yellow
}
Write-Host ""

# Ejecutar wrapper Python
python scripts/01_agregation_OHLCV/batch_trades_wrapper.py `
  --tickers-csv $tickersCsv `
  --outdir $outdir `
  --from $dateFrom `
  --to $dateTo `
  --batch-size $batchSize `
  --max-concurrent $maxConcurrent `
  --rate-limit $rateLimit `
  --ingest-script $ingestScript `
  --resume

$exitCode = $LASTEXITCODE

Write-Host ""
Write-Host "============================================================" -ForegroundColor $(if ($exitCode -eq 0) {"Green"} else {"Red"})
if ($exitCode -eq 0) {
    Write-Host "  DESCARGA COMPLETA (exit code: $exitCode)" -ForegroundColor Green
} else {
    Write-Host "  DESCARGA TERMINO CON ERRORES (exit code: $exitCode)" -ForegroundColor Red
}
Write-Host "============================================================" -ForegroundColor $(if ($exitCode -eq 0) {"Green"} else {"Red"})
Write-Host ""
Write-Host "Logs de batches: $outdir\_batch_temp\"

exit $exitCode
