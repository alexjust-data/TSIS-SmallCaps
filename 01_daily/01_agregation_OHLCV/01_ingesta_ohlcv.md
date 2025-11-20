# Pipeline de Ingesta Trades Tick-Level

**Fecha de última actualización**: 2025-11-12  
**Estado**: Descarga 2019-2025 completada | Auditoría completa realizada  

1. [Descarga Trades 20019-2025 (6,405 tickers)](#descarga-trades-2007-2025)
2. [Auditoría de Descarga](#auditoria-de-descarga)
3. [Scripts de Auditoría](#scripts-de-auditoria)
4. [Resultados Actuales](#resultados-actuales)

---

## Estado Actual del Pipeline

```bash
================================================================================
                    PIPELINE DE INGESTA TRADES TICK-LEVEL
================================================================================

PASO 0: UNIVERSO SMALL CAPS
--------------------------------------------------------------------------------
Input: processed/universe/smallcaps_universe_2025-11-01.parquet

        TARGET: 6,405 tickers Small Caps
        ├─ Activos (< $2B): 3,105 (48.5%)
        └─ Inactivos preservados: 3,300 (51.5%)
           → ANTI-SURVIVORSHIP BIAS APLICADO


                          ↓  DESCARGA HISTÓRICA


PASO 1: DESCARGA 2019-2025 (COMPLETADA)
--------------------------------------------------------------------------------
Script: batch_trades_wrapper.py + ingest_trades_ticks.py
Fecha: 2025-11-08 a 2025-11-09
Período: 2019-01-01 → 2025-11-01 (7 años)
Output: C:\TSIS_Data\trades_ticks_2019_2025\

        STATUS: COMPLETADA
        ├─ Tiempo total: ~36 horas continuas
        ├─ Velocidad: 300-350 tickers/hora
        ├─ Tamaño: ~100 GB (comprimido ZSTD level 1)
        ├─ Errores HTTP 429: 0
        └─ Rate limit perfecto

        Configuración:
        ├─ Batch size: 60 tickers
        ├─ Max concurrent: 50 batches
        ├─ Rate limit: 0.05s (600 req/s teórico)
        └─ Con --resume (idempotente)


                          ↓  AUDITORÍA REALIZADA


PASO 2: AUDITORÍA COMPLETA (2025-11-12)
--------------------------------------------------------------------------------
Scripts: audit_quick.py + audit_optimized.py
Tiempo: ~50 minutos (procesamiento paralelo)
Output: audit_optimized_*.csv

        RESULTADOS (2019-2025) :
        ├─ Total archivos: 
        ├─ Tamaño total: 
        ├─ Tickers completos: 
        ├─ Tickers incompletos: 
        ├─ Tickers faltantes:
        └─ Ticker-meses totales: 

        RESULTADOS (2004-2018) :
        ├─ Total archivos: 
        ├─ Tamaño total: 
        ├─ Tickers completos:  
        ├─ Tickers incompletos: 
        ├─ Tickers faltantes:
        └─ Ticker-meses totales: 


================================================================================
RESULTADO FINAL ACTUAL
================================================================================

Output: C:\TSIS_Data\trades_ticks_2019_2025\

        COBERTURA REAL: 
        ├─ Años 2019-2025: 
        ├─ Tamaño: 
        └─ Archivos: 

        Distribución de datos:
        ├─ Período principal: 
        ├─ Período histórico: 
        ├─ Estructura: ticker/year=YYYY/month=MM/day=DD/[session].parquet
        └─ Sesiones: premarket.parquet + market.parquet


Output: C:\TSIS_Data\trades_ticks_2004_2018_v2\

        COBERTURA REAL: 
        ├─ Años 2004-2018: 
        ├─ Años 2019-2025: 
        ├─ Tamaño: 
        └─ Archivos: 

        Distribución de datos:
        ├─ Período principal: 
        ├─ Período histórico: Datos esporádicos 2004-2018
        ├─ Estructura: ticker/year=YYYY/month=MM/day=DD/[session].parquet
        └─ Sesiones: premarket.parquet + market.parquet

================================================================================
```

---

## Descarga Trades 2019-2025

### Objetivo

Descargar trades tick-level históricos para 6,405 tickers Small Caps con separación premarket/market para análisis de microestructura y liquidez.

### Scripts de Descarga

#### 1. `ingest_trades_ticks.py` - Core

**Ubicación**: `scripts/01_agregation_OHLCV/ingest_trades_ticks.py`

**Función**: Descarga trades tick-level diarios con separación de sesiones.

**Características**:
- Descarga por día individual
- Separación automática premarket (04:00-09:30 ET) / market (09:30-16:00 ET)
- Merge inteligente: Si archivo existe, concatena y elimina duplicados por timestamp
- Compresión ZSTD level 1
- Retry exponencial con backoff 1.6x
- Rate limiting: 0.05s entre requests

**Configuración**:
```python
BASE_URL = "https://api.polygon.io"
PAGE_LIMIT = 50000                    # Trades por página
TIMEOUT = 45                          # Segundos
RETRY_MAX = 8                         # Reintentos
BACKOFF = 1.6                         # Factor exponencial
COMPRESSION = "zstd"                  # Compresión
COMPRESSION_LEVEL = 1                 # Nivel (1-22)
```

**Horarios de mercado (ET)**:
```
PREMARKET:  04:00 - 09:30 ET  → premarket.parquet
MARKET:     09:30 - 16:00 ET  → market.parquet
```

## Descarga Trades 2019-2025


```sh
python scripts\01_agregation_OHLCV\batch_trades_wrapper.py `
    --tickers-csv audit_2004_2018_to_download.csv `
    --outdir C:/TSIS_Data/trades_ticks_2004_2018_v2 `
    --from 2004-01-01 `
    --to 2018-12-31 `
    --batch-size 50 `
    --max-concurrent 25 `
    --rate-limit 0.08 `
    --ingest-script scripts\01_agregation_OHLCV\ingest_trades_ticks.py
```
```

**Output por ticker**:

```sh
C:\TSIS_Data\trades_ticks_2019_2025\{TICKER}\
└── year={YYYY}\
    └── month={MM}\
        └── day={YYYY-MM-DD}\
            ├── premarket.parquet    # Trades 04:00-09:30 ET
            └── market.parquet       # Trades 09:30-16:00 ET
```

## Descarga Trades 2004-2018

```sh
python scripts\01_agregation_OHLCV\batch_trades_wrapper.py `
    --tickers-csv audit_2004_2018_to_download.csv `
    --outdir C:/TSIS_Data/trades_ticks_2004_2018_v2 `
    --from 2004-01-01 `
    --to 2018-12-31 `
    --batch-size 50 `
    --max-concurrent 25 `
    --rate-limit 0.08 `
    --ingest-script scripts\01_agregation_OHLCV\ingest_trades_ticks.py
```

```
C:\TSIS_Data\trades_ticks_2004_2018_v2\{TICKER}\
└── year={YYYY}\
    └── month={MM}\
        └── day={YYYY-MM-DD}\
            ├── premarket.parquet    # Trades 04:00-09:30 ET
            └── market.parquet       # Trades 09:30-16:00 ET
```

**Schema de parquet**:
```
┌────────┬───────────┬─────────────────────┬───────┬──────┬──────────┬─────────────┐
│ ticker ┆ date      ┆ timestamp           ┆ price ┆ size ┆ exchange ┆ conditions  │
├────────┼───────────┼─────────────────────┼───────┼──────┼──────────┼─────────────┤
│ str    ┆ date      ┆ i64 (nanoseconds)   ┆ f64   ┆ i64  ┆ str      ┆ list[str]   │
└────────┴───────────┴─────────────────────┴───────┴──────┴──────────┴─────────────┘
```

#### 2. `batch_trades_wrapper.py` - Wrapper

**Ubicación**: `scripts/01_agregation_OHLCV/batch_trades_wrapper.py`

**Función**: Paralelización y gestión de micro-batches para procesar múltiples tickers simultáneamente.

**Características**:
- Divide universo en batches de 60 tickers
- Ejecuta hasta 50 batches concurrentes
- Logging independiente por batch
- Idempotente con flag `--resume`
- Rate limiting global

**Configuración**:
```python
BATCH_SIZE = 60                       # Tickers por batch
MAX_CONCURRENT = 50                   # Batches simultáneos
RATE_LIMIT = 0.05                     # Segundos entre requests
```

**Comando de ejecución**:
```bash
cd "D:\TSIS_SmallCaps" && python scripts/01_agregation_OHLCV/batch_trades_wrapper.py \
    --tickers-csv processed/universe/smallcaps_universe_2025-11-01.parquet \
    --outdir "C:\TSIS_Data\trades_ticks_2019_2025" \
    --from 2019-01-01 \
    --to 2025-11-01 \
    --batch-size 60 \
    --max-concurrent 50 \
    --rate-limit 0.05 \
    --ingest-script scripts/01_agregation_OHLCV/ingest_trades_ticks.py \
    --resume
```

**Output de logs**:
```
C:\TSIS_Data\trades_ticks_2019_2025\_batch_temp\
├── batch_0000.csv      # Lista de tickers del batch
├── batch_0000.log      # Log detallado de progreso
├── batch_0001.csv
├── batch_0001.log
└── ...                 # 107 batches totales
```

---





## Estructura de Archivos Final

```
D:\TSIS_SmallCaps\
├── scripts/
│   └── 01_agregation_OHLCV/
│       ├── ingest_trades_ticks.py          # Core de descarga
│       └── batch_trades_wrapper.py         # Wrapper paralelo
│
├── audit_quick.py                          # Auditoría rápida (no recomendado)
├── audit_optimized.py                      # Auditoría optimizada (USAR ESTE)
├── audit_detailed.py                       # Auditoría con timestamps (muy lento)
│
├── audit_optimized_tickers.csv             # Detalle por ticker
├── audit_optimized_months.csv              # Cobertura mensual
├── audit_optimized_pending.csv             # Tickers pendientes
│
├── processed/universe/
│   └── smallcaps_universe_2025-11-01.parquet   # 6,405 tickers
│
└── C:\TSIS_Data\
    └── trades_ticks_2019_2025/             # OUTPUT
        ├── _batch_temp/                    # Logs de descarga
        │   └── batch_*.log
        │
        └── {TICKER}/                       # 6,405 tickers
            └── year={YYYY}/                # Años disponibles
                └── month={MM}/             # 12 meses
                    └── day={YYYY-MM-DD}/   # ~250 días/año
                        ├── premarket.parquet    # 04:00-09:30 ET
                        └── market.parquet       # 09:30-16:00 ET

    │
    └── trades_ticks_2004_2018_v2/          # OUTPUT
        ├── _batch_temp/                    # Logs de descarga
        │   └── batch_*.log
        │
        └── {TICKER}/                       # 6,405 tickers
            └── year={YYYY}/                # Años disponibles
                └── month={MM}/             # 12 meses
                    └── day={YYYY-MM-DD}/   # ~250 días/año
                        ├── premarket.parquet    # 04:00-09:30 ET
                        └── market.parquet       # 09:30-16:00 ET
```

---

## Descarga 2004-2018 y Sistema de Checkpointing (2025-11-17)

### Objetivo

Completar descarga histórica de trades tick-level para 2,249 tickers con datos confirmados en período 2004-2018, implementando sistema robusto de checkpointing con marcadores `_SUCCESS`.

### Bug Crítico Detectado y Corregido

**Fecha**: 2025-11-17

**Problema identificado**:
- Tickers con años incompletos (ej: PSEC solo 2004-2010, faltaban 2011-2018)
- Código original usaba `and` para verificar archivos: `if premarket_fp.exists() and market_fp.exists()`
- **89% de días solo tienen `market.parquet`** (sin premarket trades)
- Resultado: `_SUCCESS` nunca se creaba, días truncados por crashes se perdían

**Evidencia**:
```
HCKT:
├─ 3,099 archivos parquet
├─ 0 archivos _SUCCESS
├─ 330 premarket.parquet (11%)
└─ 2,769 market.parquet (89%)
```

**Causa root**:
- Small caps tienen volumen muy bajo en premarket (04:00-09:30 ET)
- Mayoría de días solo tienen trades en market hours (09:30-16:00 ET)
- Condición `and` requería ambos archivos → marcador nunca se creaba

### Solución Implementada: Checkpointing Robusto de 3 Casos

**Ubicación**: [ingest_trades_ticks.py:446-492](../../scripts/01_agregation_OHLCV/ingest_trades_ticks.py#L446-L492)

**Lógica corregida**:

```python
# CASO 1: Día ya completo (con _SUCCESS)
if success_marker.exists():
    # Contar trades y skip
    # NO re-descargar
    continue

# CASO 2: Día con parquet(s) pero sin _SUCCESS → posible truncamiento
if (premarket_fp.exists() or market_fp.exists()) and (not success_marker.exists()):
    log(f"  {t} {day_str}: parquet(s) found but no _SUCCESS → re-downloading")

    # RE-DESCARGAR para garantizar integridad
    fetch_and_stream_write_trades(...)

    # Marcar completo SOLO si descarga exitosa
    if premarket_fp.exists() or market_fp.exists():
        success_marker.touch()
    continue

# CASO 3: Día nuevo (sin parquet ni _SUCCESS)
# Descarga normal
fetch_and_stream_write_trades(...)
if premarket_fp.exists() or market_fp.exists():
    success_marker.touch()
```

**Cambios clave**:
1. **Cambio `and` → `or`**: Acepta días con solo market.parquet
2. **Re-descarga automática**: Días con parquet pero sin `_SUCCESS` se reparan automáticamente
3. **Idempotencia**: Ejecutar múltiples veces es seguro
4. **Protección contra truncamiento**: Solo marca `_SUCCESS` tras descarga completa

**Beneficios**:
- ✅ Cura días truncados por: cortes de luz, timeouts, crashes, errores de red
- ✅ Permite resume seguro tras interrupciones
- ✅ Detecta y repara inconsistencias automáticamente
- ✅ Logging claro de días siendo reparados

### Script de Auditoría de Integridad

**Ubicación**: [auditar_dias.py](../../scripts/utils/auditar_dias.py)

**Función**: Detecta días incompletos, corruptos o faltantes en dataset descargado.

**Detecciones**:
```python
# CASO A: Día incompleto
if (premarket_fp.exists() or market_fp.exists()) and not success_marker.exists():
    issue = "PARTIAL_DAY_NO_SUCCESS"

# CASO B: Día corrupto
if success_marker.exists() and not (premarket_fp.exists() or market_fp.exists()):
    issue = "SUCCESS_NO_PARQUET"

# CASO C: Ticker sin directorio
if not ticker_dir.exists():
    issue = "TICKER_DIR_MISSING"
```

**Comando de ejecución**:
```bash
python scripts/utils/auditar_dias.py \
    --outdir C:\TSIS_Data\trades_ticks_2004_2018_v2 \
    --ping processed/universe/ping_binary_2004_2018.parquet \
    --period-start 2004-01-01 \
    --period-end 2018-12-31 \
    --out audit_2004_2018.csv
```

**Output**: `audit_2004_2018.csv`
```csv
ticker,date,issue
ABCD,2009-03-04,PARTIAL_DAY_NO_SUCCESS
XYZZ,2007-11-14,SUCCESS_NO_PARQUET
```

### Workflow de Reparación

```bash
# PASO 1: Auditar
python scripts/utils/auditar_dias.py \
    --outdir C:\TSIS_Data\trades_ticks_2004_2018_v2 \
    --ping processed/universe/ping_binary_2004_2018.parquet \
    --period-start 2004-01-01 \
    --period-end 2018-12-31 \
    --out audit_2004_2018.csv

# PASO 2: Extraer tickers afectados (PowerShell)
Import-Csv audit_2004_2018.csv |
    Select-Object -ExpandProperty ticker |
    Sort-Object -Unique |
    Out-File repair_tickers.csv

# PASO 3: Re-descargar solo tickers afectados
python scripts/01_agregation_OHLCV/batch_trades_wrapper.py \
    --tickers-csv repair_tickers.csv \
    --outdir C:\TSIS_Data\trades_ticks_2004_2018_v2 \
    --from 2004-01-01 \
    --to 2018-12-31 \
    --batch-size 100 \
    --max-concurrent 50 \
    --rate-limit 0.05 \
    --ingest-script scripts/01_agregation_OHLCV/ingest_trades_ticks.py
```

### Estado de Descarga 2004-2018

**Período**: 2004-01-01 → 2018-12-31 (15 años)
**Tickers**: 2,249 (confirmados con datos en Polygon)
**Output**: `C:\TSIS_Data\trades_ticks_2004_2018_v2\`

#### garantía de integridad

