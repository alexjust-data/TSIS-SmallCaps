# Comando Optimizado para Descarga de Trades Tick-Level

## Configuración Final Recomendada (SUPER TURBO)

### Comando Principal (PowerShell)

```powershell
cd "D:\TSIS_SmallCaps"

python scripts/01_agregation_OHLCV/batch_trades_wrapper.py `
  --tickers-csv processed/universe/smallcaps_universe_2025-11-01.parquet `
  --outdir "C:\TSIS_Data\trades_ticks_2019_2025" `
  --from 2019-01-01 `
  --to 2025-11-01 `
  --batch-size 60 `
  --max-concurrent 20 `
  --rate-limit 0.08 `
  --ingest-script scripts/01_agregation_OHLCV/ingest_trades_ticks.py `
  --resume
```

### Parámetros Clave

**Wrapper (batch_trades_wrapper.py):**
- `--batch-size 60`: 60 tickers por proceso/batch
- `--max-concurrent 20`: 20 batches ejecutándose en paralelo
- `--rate-limit 0.08`: 0.08s entre páginas (se adapta hasta 0.06s)
- `--resume`: Continúa desde donde quedó (salta tickers ya descargados)

**Throughput Teórico:**
- 20 procesos × (1/0.08) req/s = **250 req/s**
- Con adaptativo puede llegar a **333 req/s** (MIN_RL=0.06s)

### Optimizaciones Aplicadas

#### 1. ingest_trades_ticks.py
- ✅ Pool HTTP ampliado: 32 conexiones, 64 max size
- ✅ Rate limiting adaptativo: MIN_RL = 0.06s, MAX_RL = 0.40s
- ✅ Compresión ZSTD nivel 1 (más rápido, ~10-20% más espacio)
- ✅ Separación automática premarket/market
- ✅ Soporte para flags opcionales:
  - `--skip-weekends`: Salta sábados y domingos
  - `--skip-us-holidays`: Salta festivos USA (lista embebida)
  - `--no-afterhours`: Solo premarket + regular (ahorra ~30-40% datos)

#### 2. batch_trades_wrapper.py
- ✅ Defaults optimizados para producción
- ✅ Gestión automática de batches con ThreadPoolExecutor
- ✅ Logs detallados por batch en `_batch_temp/`

### Monitoreo de Errores 429

**Monitor en tiempo real:**
```bash
cd "D:\TSIS_SmallCaps" && python monitor_429.py
```

**IMPORTANTE:** Los "429" en los logs son mayormente números de trades (ej: "426,429 trades"), NO errores HTTP.

Para verificar errores REALES:
```bash
python -c "
from pathlib import Path
import re
log_dir = Path('C:/TSIS_Data/trades_ticks_2019_2025/_batch_temp')
for log in log_dir.glob('*.log'):
    content = log.read_text(encoding='utf-8', errors='ignore')
    if 'Rate Limit' in content or 'HTTP 429' in content:
        print(f'{log.name}: ERROR 429 REAL')
"
```

### Escenarios Alternativos

#### Opción CONSERVADORA (sin throttling)
```powershell
--batch-size 30 --max-concurrent 10 --rate-limit 0.15
```
- Throughput: ~67 req/s
- Ideal para: Planes con límites estrictos

#### Opción BALANCED (recomendada para 24/7)
```powershell
--batch-size 40 --max-concurrent 12 --rate-limit 0.10
```
- Throughput: ~120 req/s
- Ideal para: Descargas largas sin supervisión

#### Opción SUPER TURBO (actual - máximo rendimiento)
```powershell
--batch-size 60 --max-concurrent 20 --rate-limit 0.08
```
- Throughput: ~250 req/s
- Ideal para: Descargas rápidas con monitoreo

### Flags Opcionales para Optimizar

Para activar en el ingestor, modifica `batch_trades_wrapper.py` línea 91-100:

```python
cmd = [
    sys.executable, str(script_path),
    "--tickers-csv", str(csv_path),
    "--outdir", args.outdir,
    "--from", args.date_from,
    "--to", args.date_to,
    "--rate-limit", str(args.rate_limit),
    "--max-tickers-per-process", str(len(tickers)),
    "--max-workers", "1",
    "--skip-weekends",           # ← AÑADIR
    "--skip-us-holidays",        # ← AÑADIR
    # "--no-afterhours",         # ← OPCIONAL: solo si no necesitas after-hours
]
```

**Ahorro estimado con flags:**
- `--skip-weekends` + `--skip-us-holidays`: ~30% menos llamadas API
- `--no-afterhours`: ~35% menos datos escritos

### Estructura de Salida

```
C:\TSIS_Data\trades_ticks_2019_2025\
├── _batch_temp\              # Logs de batches (temporal)
│   ├── batch_0000.log
│   ├── batch_0001.log
│   └── ...
├── {TICKER}\                 # Un directorio por ticker
│   └── year={YYYY}\
│       └── month={MM}\
│           └── day={YYYY-MM-DD}\
│               ├── premarket.parquet  # 04:00-09:30 ET
│               └── market.parquet     # 09:30-20:00 ET (o 16:00 si --no-afterhours)
└── trades_download.log       # Log consolidado final
```

### Progreso Actual

- **Inicio:** 09:06 am, 2025-11-04
- **Tickers completados:** 501 de 6,405 (7.8%)
- **Configuración:** SUPER TURBO (60/20/0.08)
- **Estado:** ✅ Corriendo sin errores 429 reales

### Consejos del Sistema

1. **Disco:** Excluir `C:\TSIS_Data\` del antivirus
2. **Red:** Cable ethernet (no WiFi)
3. **Energía:** Desactivar suspensión
4. **Disco:** Idealmente NVMe para I/O rápido

### Estimación de Tiempo

Con SUPER TURBO (250 req/s):
- ~2,500 días × 6,405 tickers = ~16M días-ticker
- Promedio ~5-10 páginas/día-ticker = ~80-160M páginas
- A 250 req/s = **3.7-7.4 días** de descarga continua

Con BALANCED (120 req/s):
- **7.7-15.4 días** de descarga continua

---

**Última actualización:** 2025-11-04 09:14 am
**Configuración activa:** SUPER TURBO (60/20/0.08)
**Errores 429 reales:** 0
