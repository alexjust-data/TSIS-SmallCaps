l# FASE B: Ingesta OHLCV Histórico (Daily + Intraday 1-Minute)

## Contexto - De dónde venimos

```bash
════════════════════════════════════════════════════════════════════════════════
FASE A COMPLETADA - UNIVERSO CONSTRUIDO (6,405 tickers Small Caps)
════════════════════════════════════════════════════════════════════════════════

INPUT PARA FASE B:
├── processed/universe/smallcaps_universe_2025-11-01.parquet
│   └── 6,405 tickers Small Caps enriquecidos
│       ├─ Activos (< $2B): 3,105 tickers (48.5%)
│       │  ├─ Market cap, description, employees, SIC code
│       │  ├─ Splits: 2,009 tickers (31.4%)
│       │  └─ Dividends: 1,768 tickers (27.6%)
│       │
│       └─ Inactivos preservados: 3,300 tickers (51.5%)
│          ├─ Delisted dates (100% completitud)
│          ├─ Splits históricos preservados
│          └─ Dividends históricos preservados
│          → ✅ ANTI-SURVIVORSHIP BIAS APLICADO
│
└── processed/corporate_actions/
    ├─ splits/year=*/splits.parquet (3,420 eventos filtrados)
    └─ dividends/year=*/dividends.parquet (71,291 eventos filtrados)

PERÍODO TARGET: 2019-01-01 → 2025-11-01 (~7 años)
OBJETIVO: Descargar OHLCV histórico (Daily + Intraday 1-minute)
════════════════════════════════════════════════════════════════════════════════
```

## Roadmap - FASE B

```bash
                        ↓ DESDE FASE A
    ════════════════════════════════════════════════════════════════════════════
    INPUT: 6,405 tickers Small Caps (2019-2025)
    ════════════════════════════════════════════════════════════════════════════
                           │
            ┌──────────────┴───────────────────────────┐
            │                       │                  │
            ↓                       ↓                  ↓
    ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
    │  DAILY OHLCV    │     │ INTRADAY 1-MIN  │     │  TRADES TICKS   │
    │  (Paralelo)     │     │ (Micro-batches) │     │ (Pre+Market)    │
    └─────────────────┘     └─────────────────┘     └─────────────────┘
            │                       │                        │
            │                       │                        │
            ↓                       ↓                        ↓
    raw/polygon/           raw/polygon/           raw/polygon/
    ohlcv_daily/           ohlcv_intraday_1m/     trades/
    └── {TICKER}/          └── {TICKER}/          └── {TICKER}/
        └── year={YYYY}/       └── year={YYYY}/       └── year={YYYY}/
            └── daily.parquet      └── month={MM}/         └── month={MM}/
                                       └── minute.parquet      ├── premarket.parquet
                                                               └── market.parquet
            │                       │                        │
            └───────────────────────┴────────────────────────┘
                                    │
                                    ↓
            ════════════════════════════════════════════════════════════
            FASE B COMPLETADA - OHLCV HISTÓRICO + TRADES TICK-LEVEL
            ════════════════════════════════════════════════════════════
```

## Objetivo

Descargar datos OHLCV históricos y trades tick-level para 6,405 tickers Small Caps usando Polygon API:

1. **Daily aggregates** (2019-2025): Datos diarios para análisis de tendencias
2. **Intraday 1-minute** (2019-2025): Datos minuto a minuto para patrones intraday
3. **Trades tick-level** (2019-2025): Trades individuales (premarket + market hours) para microestructura

**Características clave**:
- ✅ Descarga paralela (Daily: ThreadPoolExecutor, Intraday: Micro-batches, Trades: Micro-batches)
- ✅ Rate-limit adaptativo (evita 429 Too Many Requests)
- ✅ Idempotente (merge automático, puede reiniciarse sin duplicados)
- ✅ Particionado por año/mes (optimización storage y queries)
- ✅ Compresión ZSTD (reduce ~50% espacio)
- ✅ Adjusted prices (splits/dividends aplicados)
- ✅ Separación premarket/market (filtrado por timestamp)

---

## Estrategia de Descarga

### A. Daily OHLCV (Descarga Simple)

**Endpoint**: `/v2/aggs/ticker/{ticker}/range/1/day/{from}/{to}`

**Características**:
- Descarga paralela simple (ThreadPoolExecutor con 12 workers)
- Paginación cursor-based (50K registros por página)
- Escritura particionada por año
- Merge automático (idempotente)

**Estimación**:
- **Tiempo**: ~20-25 minutos (250-300 tickers/min)
- **Tamaño**: ~30-40 GB sin compresión
- **Success rate esperado**: >99%

---

### B. Intraday 1-Minute (Descarga Avanzada - Micro-batches)

**Endpoint**: `/v2/aggs/ticker/{ticker}/range/1/minute/{from}/{to}`

**Características**:
- **Descarga MENSUAL** (evita JSONs de 20GB que saturan memoria)
- **Micro-batches de 20 tickers** (evita "Atasco de Elefantes")
- **8 batches concurrentes** (paralelismo controlado)
- **Rate-limit adaptativo** (0.12-0.35s, acelera/frena según 429)
- **Escritura streaming** por página (bajo uso de memoria)
- **Compresión ZSTD level 2** (-50% tamaño)

**Problema resuelto: "Atasco de Elefantes"**:
```
ANTES (descarga completa 2019-2025):
- Ticker pesado (ej: AAPL) → JSON 20GB → Saturación de memoria
- Bloqueaba todo el batch → Timeout → Reinicio manual

AHORA (descarga mensual):
- Ticker pesado → 252 requests pequeños (1 por mes)
- Nunca satura memoria
- Micro-batches de 20 tickers → Tickers pesados NO bloquean sistema
```

**Estimación**:
- **Tiempo**: ~5-6 horas (250-300 tickers/hora promedio)
- **Tamaño**: ~2-2.5 TB comprimido (ZSTD)
- **Success rate esperado**: 100%

---

### C. Trades Tick-Level (Descarga Micro-batches - NUEVO)

**Endpoint**: `/v3/trades/{ticker}?timestamp.gte={from}&timestamp.lte={to}`

**Características**:
- **Descarga DIARIA** (evita JSONs gigantes en tickers líquidos)
- **Micro-batches de 15 tickers** (tickers líquidos generan MUCHO más volumen)
- **6 batches concurrentes** (conservador para no saturar API)
- **Separación premarket/market** (filtrado por timestamp 04:00-09:30 vs 09:30-16:00)
- **Rate-limit adaptativo** (0.15-0.40s, más conservador que intraday)
- **Compresión ZSTD level 3** (datos tick son MUY grandes)

**Horarios de mercado (ET)**:
```
PREMARKET:  04:00 - 09:30 ET  → premarket.parquet
MARKET:     09:30 - 16:00 ET  → market.parquet
AFTERHOURS: 16:00 - 20:00 ET  → (NO descargado, fuera de scope)
```

**Estimación**:
- **Tiempo**: ~8-12 horas (dependiendo de liquidez)
- **Tamaño**: ~3-5 TB comprimido (ZSTD level 3)
- **Success rate esperado**: >95%

**Advertencia**: Small caps tienen mucho MENOS volumen que large caps, pero aun así pueden generar ~100K-1M trades/día en momentos de alta actividad (pump & dumps).

---

## Scripts y Herramientas

### Daily OHLCV

| Script | Descripción |
|--------|-------------|
| [ingest_ohlcv_daily.py](../../scripts/01_agregation_OHLCV/ingest_ohlcv_daily.py) | Descarga paralela de OHLCV diario |

**Parámetros clave**:
- `PAGE_LIMIT`: 50,000
- `ADJUSTED`: True
- `MAX_WORKERS`: 12
- `TIMEOUT`: 35s

```sh
📊 RESUMEN DESCARGA DAILY
==================================================
Universo esperado:    6,405 tickers
Descargados:          6,297 tickers
Cobertura:            98.31%
Faltantes:            108

📁 SAMPLE  TICKERS:
==================================================
CUR      | 1 años | 211 rows | 2019-01-02 → 2019-10-31
PACE     | 2 años | 203 rows | 2020-11-27 → 2021-09-20
CHX      | 6 años | 1,284 rows | 2020-06-04 → 2025-07-15
ESM      | 3 años | 351 rows | 2021-04-30 → 2023-03-09
GT       | 7 años | 1,719 rows | 2019-01-02 → 2025-10-31

❌ FALTANTES (108):
==================================================
Primeros 10: ['AANW', 'ABX', 'ACLL', 'AIRCW', 'AIRTV', 'AIVW', 'ALPX', 'ALVU', 'ARMKW', 'ARNCW']
```
---

### Intraday 1-Minute

| Script | Descripción |
|--------|-------------|
| [ingest_ohlcv_intraday_minute.py](../../scripts/01_agregation_OHLCV/ingest_ohlcv_intraday_minute.py) | Core de descarga intraday (mensual, streaming) |
| [batch_intraday_wrapper.py](../../scripts/01_agregation_OHLCV/batch_intraday_wrapper.py) | Wrapper para micro-batches de 20 tickers |
| [launch_wrapper.ps1](../../scripts/01_agregation_OHLCV/launch_wrapper.ps1) | PowerShell launcher (8 batches concurrentes) |

**Parámetros clave**:
- `PAGE_LIMIT`: 50,000 (5x menos requests vs 10K default)
- `ADJUSTED`: True
- `BATCH_SIZE`: 20 tickers
- `CONCURRENT_BATCHES`: 8
- `RATE_LIMIT_BASE`: 0.12s (adaptativo hasta 0.35s)
- `COMPRESSION`: ZSTD level 2

**Optimizaciones críticas**:
1. Descarga mensual (84 meses para 2019-2025, evita JSON gigantes)
2. PAGE_LIMIT 50K (reduce requests en 80%)
3. Rate-limit adaptativo (acelera si no hay 429)
4. Compresión ZSTD (-50% storage)
5. TLS heredado (fix SSL handshake Windows)
6. Pool mejorado (reduce handshake overhead)

```sh
📊 RESUMEN DESCARGA INTRADÍA 1-MINUTE
============================================================
Universo esperado:    6,405 tickers
Descargados:          6,296 tickers
Cobertura:            98.30%
Faltantes:            109

📁 SAMPLE 5 TICKERS:
============================================================
NGNE     | 3 años | 23 meses | 64,908 rows | 2023-12-19 14:15 → 2025-10-31 23:18
COSO     | 5 años | 47 meses | 4,212 rows | 2021-12-31 19:03 → 2025-10-31 19:59
WLDN     | 7 años | 82 meses | 166,886 rows | 2019-01-02 14:30 → 2025-10-31 21:43
MCGA     | 1 años | 2 meses | 4,516 rows | 2025-09-08 08:01 → 2025-10-31 19:59
AEZS     | 6 años | 68 meses | 215,156 rows | 2019-01-02 13:50 → 2024-08-08 19:39

❌ FALTANTES (109):
============================================================
Primeros 10: ['AANW', 'ABX', 'ACLL', 'AEBIV', 'AIRCW', 'AIRTV', 'AIVW', 'ALPX', 'ALVU', 'ARMKW']

💾 ESTIMACIÓN TAMAÑO:
============================================================
Sample 10 tickers:
  Promedio/ticker: 2.9 MB
  Total estimado:  17.5 GB (6296 tickers)
``` 
---

### Trades Tick-Level

* [ingest_trades_ticks.py](../../scripts/01_agregation_OHLCV/ingest_trades_ticks.py) - **Ingestor principal**
    * Descarga DIARIA (2,555 días para 2019-2025, evita JSONs gigantes)
    * Separación premarket (04:00-09:30) / market (09:30-16:00) - (reduce tamaño por archivo)
    * Streaming writes
    * Rate-limit adaptativo (0.12-0.40s) (ticks generan mucho más tráfico)
    * Compresión ZSTD level 3 (trades tick son 10x más grandes que 1-min bars)

* [batch_trades_wrapper.py](../../scripts/01_agregation_OHLCV/batch_trades_wrapper.py) - **Wrapper de micro-batches**
    * Micro-batches de 15 tickers
    * Paralelismo de 10 batches concurrentes
    * Resume logic robusto (detecta días parciales y los reintenta)

* [launch_trades_wrapper.ps1](../../scripts/01_agregation_OHLCV/launch_trades_wrapper.ps1) - **Launcher PowerShell**
    * Configuración optimizada (balanceada velocidad/estabilidad)
    * Estimación: ~9-12 horas

---

```sh
python scripts/01_agregation_OHLCV/batch_trades_wrapper.py  
    --tickers-csv processed/universe/smallcaps_universe_2025-11-01.parquet 
    --outdir raw/polygon/trades_ticks 
    --from 2019-01-01 
    --to 2025-11-01 
    --batch-size 15 
    --max-concurrent 10 
    --rate-limit 0.15 
    --ingest-script scripts/01_agregation_OHLCV/ingest_trades_ticks.py 
    --resume
```

```sh
🚀 OPCIONES PARA ACELERAR LA DESCARGA:

1. AUMENTAR CONCURRENCIA (Opción más efectiva)
# Actual: 10 batches concurrentes
# Recomendado: 15-20 batches concurrentes

Pros:
✅ Acelera 1.5-2x (de 5 días a 2.5-3 días)
✅ Aprovecha mejor el throughput de Polygon API
✅ No requiere cambios de código

Contras:
⚠️ Mayor uso de RAM (~3-4 GB)
⚠️ Más requests simultáneos (pero dentro de límites)

2. AUMENTAR BATCH SIZE
# Actual: 15 tickers/batch
# Recomendado: 20-25 tickers/batch

Pros:
✅ Menos overhead de inicio/fin de batch
✅ Mejor utilización de recursos

Contras:
⚠️ Batches más lentos individualmente
⚠️ Menos granularidad en el progreso

3. REDUCIR RATE LIMIT (Con cuidado)
# Actual: 0.15s/página (adaptativo 0.12-0.40s)
# Agresivo: 0.10s/página

Pros:
✅ Más requests/segundo

Contras:
❌ Alto riesgo de 429 (rate limit exceeded)
❌ Puede hacer que el adaptativo aumente el delay

4. COMBINAR 1+2 (RECOMENDADO)
--batch-size 20 --max-concurrent 15

Estimación: ~3 días (vs 5 días actual)

📊 ¿Qué te recomiendo?

OPCIÓN CONSERVADORA (recomendada):
--batch-size 20 --max-concurrent 15 --rate-limit 0.15

Velocidad: ~2.5-3 días
Riesgo: Bajo
Ganancia: 40-50% más rápido

OPCIÓN AGRESIVA (si tienes prisa):
--batch-size 25 --max-concurrent 20 --rate-limit 0.12

Velocidad: ~2 días
Riesgo: Medio (puede haber más 429s)
Ganancia: 60% más rápido
```

lanzado a las 20:27
```sh
cd "D:\TSIS_SmallCaps" && python scripts/01_agregation_OHLCV/batch_trades_wrapper.py 
    --tickers-csv processed/universe/smallcaps_universe_2025-11-01.parquet 
    --outdir raw/polygon/trades_ticks 
    --from 2019-01-01 --to 2025-11-01 
    --batch-size 20 
    --max-concurrent 15 
    --rate-limit 0.15 
    --ingest-script scripts/01_agregation_OHLCV/ingest_trades_ticks.py 
    --resume
```

## Estructura de Output

```sh
D:\TSIS_SmallCaps\
├── raw/polygon/
│   │
│   ├── ohlcv_daily/                        # DAILY OHLCV
│   │   └── {TICKER}/
│   │       └── year={YYYY}/
│   │           └── daily.parquet
│   │               ├─ Columnas: date, open, high, low, close, volume,
│   │               │            vwap, transactions, otc, ticker
│   │               └─ Tamaño promedio: ~50-100 KB por ticker
│   │
│   └── ohlcv_intraday_1m/                  # INTRADAY 1-MINUTE
│       └── {TICKER}/
│           └── year={YYYY}/
│               └── month={MM}/
│                   └── minute.parquet (ZSTD compressed)
│                       ├─ Columnas: timestamp, open, high, low, close,
│                       │            volume, vwap, transactions, otc,
│                       │            ticker, datetime
│                       └─ Tamaño promedio: ~200-500 MB por ticker
│                           (comprimido, puede ser 1-2 GB descomprimido)
│
└── processed/
    └── ohlcv_audit/                        # AUDITORÍAS Y LOGS
        ├── daily_download_summary.csv
        ├── intraday_download_summary.csv
        ├── failed_tickers.csv
        └── download_logs/
            ├── daily_YYYYMMDD_HHMMSS.log
            └── intraday_batch_*.log
```

---

