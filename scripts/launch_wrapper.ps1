# launch_wrapper.ps1 - Launcher PRINCIPAL (TSIS SmallCaps)
# Descarga universo completo Small Caps con configuración balanceada
# APROVECHA: Descarga mensual + ZSTD + Rate-limit adaptativo + TLS heredado

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

# Configuración del WRAPPER PRINCIPAL (optimizada para TSIS SmallCaps)
$tickersCsv = "processed/universe/smallcaps_universe_2025-11-01.parquet"
$outdir = "raw/polygon/ohlcv_intraday_1m"
$dateFrom = "2019-01-01"
$dateTo = "2025-11-01"

# CONFIGURACIÓN AGRESIVA (optimizada para máxima velocidad):
# - Batches pequeños (15 tickers) - menos bloqueo por tickers pesados
# - Concurrencia alta (12 batches) - más throughput
# - Rate limit agresivo (adaptativo ajustará según necesidad)
$batchSize = 15          # Batches más pequeños, menos espera
$maxConcurrent = 12      # Más batches simultáneos
$rateLimit = 0.15        # Agresivo inicial (adaptativo: 0.12-0.35s)
$ingestScript = "scripts/ingest_ohlcv_intraday_minute.py"

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  WRAPPER PRINCIPAL - TSIS SMALLCAPS (6,405 tickers)" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Configuración:"
Write-Host "  Universo:        $tickersCsv (6,405 tickers - Small Caps)"
Write-Host "  Periodo:         $dateFrom -> $dateTo (~7 años)"
Write-Host "  Batch size:      $batchSize tickers/batch"
Write-Host "  Concurrencia:    $maxConcurrent batches simultaneos"
Write-Host "  Rate limit:      $rateLimit s/pag (adaptativo 0.12-0.35s)"
Write-Host "  PAGE_LIMIT:      50,000 rows/request (5x mejora)"
Write-Host "  Compresion:      ZSTD level 2"
Write-Host "  Descarga:        Por MESES (evita JSONs gigantes)"
Write-Host "  Resume:          Activado (excluye tickers con datos)"
Write-Host ""
Write-Host "Optimizaciones activas:"
Write-Host "  - Descarga mensual: JSON pequeños, menos RAM"
Write-Host "  - Rate-limit adaptativo: acelera/frena automaticamente"
Write-Host "  - PAGE_LIMIT 50K: reduce requests ~80%"
Write-Host "  - TLS heredado a subprocesos"
Write-Host "  - Compresion ZSTD: archivos 40-60% mas pequeños"
Write-Host ""
Write-Host "Estimaciones:"
Write-Host "  - Batches totales: ~427 batches (6,405 / 15)"
Write-Host "  - Tiempo estimado: ~2-3 horas (config agresiva)"
Write-Host "  - Velocidad esperada: ~450-550 t/h"
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
python scripts/batch_intraday_wrapper.py `
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
