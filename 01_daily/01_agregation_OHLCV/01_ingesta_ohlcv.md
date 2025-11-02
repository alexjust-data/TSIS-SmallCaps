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
| [ingest_ohlcv_daily.py](../../scripts/ingest_ohlcv_daily.py) | Descarga paralela de OHLCV diario |

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
| [ingest_ohlcv_intraday_minute.py](../../scripts/ingest_ohlcv_intraday_minute.py) | Core de descarga intraday (mensual, streaming) |
| [batch_intraday_wrapper.py](../../scripts/batch_intraday_wrapper.py) | Wrapper para micro-batches de 20 tickers |
| [launch_wrapper.ps1](../../scripts/launch_wrapper.ps1) | PowerShell launcher (8 batches concurrentes) |

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

---

### Trades Tick-Level (NUEVO)

| Script | Descripción |
|--------|-------------|
| [ingest_trades_ticks.py](../../scripts/ingest_trades_ticks.py) | Core de descarga trades tick-level (diario, streaming) |
| [batch_trades_wrapper.py](../../scripts/batch_trades_wrapper.py) | Wrapper para micro-batches de 15 tickers |
| [launch_trades_wrapper.ps1](../../scripts/launch_trades_wrapper.ps1) | PowerShell launcher (6 batches concurrentes) |

**Parámetros clave**:
- `PAGE_LIMIT`: 50,000
- `BATCH_SIZE`: 15 tickers (más conservador)
- `CONCURRENT_BATCHES`: 6 (más conservador)
- `RATE_LIMIT_BASE`: 0.15s (adaptativo hasta 0.40s)
- `COMPRESSION`: ZSTD level 3 (máxima compresión)
- `SPLIT_SESSIONS`: True (premarket/market separados)

**Optimizaciones críticas**:
1. Descarga DIARIA (2,555 días para 2019-2025, evita JSON gigantes)
2. Separación premarket/market (reduce tamaño por archivo)
3. Rate-limit adaptativo MÁS conservador (ticks generan mucho más tráfico)
4. Compresión ZSTD level 3 (trades tick son 10x más grandes que 1-min bars)
5. Resume logic robusto (detecta días parciales y los reintenta)

---

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

## Próximos Pasos

**PENDIENTE**: Leer scripts de referencia del proyecto anterior para adaptar:

1. ✅ Crear documentación inicial FASE B
2. ⏳ Leer scripts de referencia:
   - [B.2_audit_final_universo_hibrido_20251025.md](../../../04_TRADING_SMALLCAPS/01_DayBook/fase_01/B_ingesta_Daily_Minut_v2/B.2_audit_final_universo_hibrido_20251025.md)
   - [ingest_ohlcv_daily.py](../../../04_TRADING_SMALLCAPS/scripts/fase_B_ingesta_Daily_minut/ingest_ohlcv_daily.py)
   - [ingest_ohlcv_intraday_minute.py](../../../04_TRADING_SMALLCAPS/scripts/fase_B_ingesta_Daily_minut/ingest_ohlcv_intraday_minute.py)
   - [launch_wrapper.ps1](../../../04_TRADING_SMALLCAPS/scripts/fase_B_ingesta_Daily_minut/tools/launch_wrapper.ps1)
   - [batch_intraday_wrapper.py](../../../04_TRADING_SMALLCAPS/scripts/fase_B_ingesta_Daily_minut/tools/batch_intraday_wrapper.py)
3. ⏳ Adaptar scripts a TSIS_SmallCaps (6,405 tickers)
4. ⏳ Ejecutar descarga Daily OHLCV
5. ⏳ Ejecutar descarga Intraday 1-Minute
6. ⏳ Generar auditorías finales

---

**Status**: 📝 Documentación creada, esperando lectura de scripts de referencia
