# Pipeline de Datos Polygon - Infraestructura Tick-Level


1. [Datos Tick-Level Trades (4 datasets)](#paso-1-datos-tick-level-trades)
2. [Datos OHLCV Intraday (3 datasets)](#paso-2-datos-ohlcv-intraday-1-minute)
3. [Datos Quotes Nivel 1 (5 datasets)](#paso-3-datos-quotes-bidask-level-1)
4. [Datasets de Referencia (3 datasets)](#datasets-de-referencia)
5. [Visualizacion del Pipeline Completo](#route-map---pipeline-completo)
6. [Acceso Programatico](#acceso-programatico)

---

## Route Map - Pipeline Completo

```bash
════════════════════════════════════════════════════════════════════════════════
                        INFRAESTRUCTURA DE DATOS POLYGON
════════════════════════════════════════════════════════════════════════════════

PASO 1: DATOS TICK-LEVEL (TRADES)
────────────────────────────────────────────────────────────────────────────────
Ubicacion: C:\TSIS_Data\trades_ticks_*
Estructura: ticker/year=YYYY/month=MM/day=DD/data.parquet
Columnas: timestamp, price, size, exchange, conditions, tape

        📦 TRADES TICK-LEVEL: 11,813 ticker directories
        ├─ trades_ticks: 961 tickers (8.1%)
           ├─ Periodo: 2025
           ├─ Estructura: Particionado Hive (ticker/year/month/day)
           └─ Formato: Parquet con compresion snappy
        ├─ trades_ticks_2004_2018: 3,478 tickers (29.4%)
           ├─ Periodo: 2004-2005
           ├─ Estructura: Particionado Hive (ticker/year/month/day)
           └─ Formato: Parquet con compresion snappy
        ├─ trades_ticks_2019_2025: 6,311 tickers (53.4%)
           ├─ Periodo: 2017-2019
           ├─ Estructura: Particionado Hive (ticker/year/month/day)
           └─ Formato: Parquet con compresion snappy
        └─ trades_ticks: 1,063 tickers (9.0%)
           ├─ Periodo: N/A
           ├─ Estructura: Particionado Hive (ticker/year/month/day)
           └─ Formato: Parquet con compresion snappy


                              ↓  AGREGACION 1-MINUTE


PASO 2: DATOS OHLCV INTRADAY (1-MINUTE)
────────────────────────────────────────────────────────────────────────────────
Ubicacion: C:\TSIS_Data\ohlcv_* y D:\TSIS_SmallCaps\raw\polygon\ohlcv_*
Estructura: ticker/year=YYYY/month=MM/day=DD/data.parquet
Columnas: timestamp, open, high, low, close, volume, vwap, transactions

        📊 OHLCV INTRADAY 1-MINUTE: 14,473 ticker directories
        ├─ ohlcv_intraday_1m: 2 tickers (0.0%)
           ├─ Periodo: N/A
           ├─ Ubicacion: C:\TSIS_Data\ohlcv_intraday_1m
           └─ Agregado desde: Tick-level trades via PyArrow
        ├─ ohlcv_daily: 8,174 tickers (56.5%)
           ├─ Periodo: 2022-2023
           ├─ Ubicacion: D:\TSIS_SmallCaps\raw\polygon\ohlcv_daily
           └─ Agregado desde: Tick-level trades via PyArrow
        └─ ohlcv_intraday_1m: 6,297 tickers (43.5%)
           ├─ Periodo: N/A
           ├─ Ubicacion: D:\TSIS_SmallCaps\raw\polygon\ohlcv_intraday_1m
           └─ Agregado desde: Tick-level trades via PyArrow


                              ↓  QUOTES NIVEL 1


PASO 3: DATOS QUOTES (BID/ASK LEVEL 1)
────────────────────────────────────────────────────────────────────────────────
Ubicacion: C:\TSIS_Data\quotes_*
Estructura: ticker/year=YYYY/month=MM/day=DD/data.parquet
Columnas: timestamp, bid_price, bid_size, ask_price, ask_size, exchange, conditions

        💱 QUOTES LEVEL 1 (BID/ASK): 6,808 ticker directories
        ├─ quotes_2004_2018: 0 tickers (0.0%)
           ├─ Periodo: N/A
           ├─ Estructura: Particionado Hive (ticker/year/month/day)
           └─ Uso: Spread analysis, liquidity metrics, order flow
        ├─ quotes_2029_2025: 0 tickers (0.0%)
           ├─ Periodo: N/A
           ├─ Estructura: Particionado Hive (ticker/year/month/day)
           └─ Uso: Spread analysis, liquidity metrics, order flow
        ├─ quotes_p95_2004_2018: 2,187 tickers (32.1%) ← Percentile 95 sampling
           ├─ Periodo: 2004-2011
           ├─ Estructura: Particionado Hive (ticker/year/month/day)
           └─ Uso: Spread analysis, liquidity metrics, order flow
        ├─ quotes_p95_2019_2025: 4,620 tickers (67.9%) ← Percentile 95 sampling
           ├─ Periodo: 2019
           ├─ Estructura: Particionado Hive (ticker/year/month/day)
           └─ Uso: Spread analysis, liquidity metrics, order flow
        └─ quotes_test: 1 tickers (0.0%) ← Test dataset
           ├─ Periodo: 2023
           ├─ Estructura: Particionado Hive (ticker/year/month/day)
           └─ Uso: Spread analysis, liquidity metrics, order flow


════════════════════════════════════════════════════════════════════════════════
✅ INFRAESTRUCTURA COMPLETADA - DATOS DISPONIBLES
════════════════════════════════════════════════════════════════════════════════

RESULTADO FINAL - INVENTARIO CONSOLIDADO:
├─ Datasets totales: 18
├─ Ticker directories: 33,111
├─ Periodo global: 2004-2025 (21 años)
└─ Almacenamiento: Dual-location (C: + D:)

CATEGORIAS:
├─ Tick-Level (Trades): 4 datasets, 11,813 ticker dirs
├─ OHLCV Intraday (1m): 3 datasets, 14,473 ticker dirs
├─ Quotes Level 1: 5 datasets, 6,808 ticker dirs
├─ Reference/Fundamentals: 3 datasets
└─ Otros: 3 datasets

READY FOR:
├─ Backtesting tick-level con precision nanosegundos
├─ ML feature engineering (spreads, volume profiles, microstructure)
├─ Analisis cuantitativo multi-timeframe
└─ Research de market microstructure

════════════════════════════════════════════════════════════════════════════════
```

---

## PASO 1: Datos Tick-Level (Trades)

### Descripcion General

Los datasets de tick-level contienen TODAS las transacciones individuales (trades) ejecutadas
en los mercados. Cada registro representa una ejecucion real con precision de nanosegundos.

**Casos de uso:**  
- Backtesting ultra-preciso con ejecucion tick-by-tick  
- Analisis de market microstructure (price impact, order flow)  
- Machine learning features basadas en volumen, trade size distributions  
- VWAP execution quality analysis  

### Datasets Disponibles

**Dataset:** `trades_ticks`  
**Ubicacion:** `C:\TSIS_Data\trades_ticks`  
**Tickers:** 961 ticker directories  
**Periodo:** 2025  
**Estructura:** ticker/year=YYYY/month=MM/day=DD/data.parquet  

**Columnas:**  
- `timestamp` (int64) - Unix nanoseconds UTC  
- `price` (float64) - Trade execution price  
- `size` (int64) - Trade size in shares  
- `exchange` (string) - Exchange code (e.g., 'Q' = NASDAQ)  
- `conditions` (list<int>) - Trade condition codes  
- `tape` (string) - Consolidation tape (A/B/C)  

**Ejemplo de lectura:**  
```python
import pyarrow.dataset as ds

# Leer un ticker especifico con filtros
dataset = ds.dataset(
    r'C:\TSIS_Data\trades_ticks\AAPL',
    format='parquet',
    partitioning='hive'
)

# Filtrar por fecha (pushdown predicates)
table = dataset.to_table(
    filter=(ds.field('year') == 2024) & (ds.field('month') == 1)
)

print(f"Trades: {table.num_rows:,}")
df = table.to_pandas()
```

---

**Dataset:** `trades_ticks_2004_2018`
**Ubicacion:** `C:\TSIS_Data\trades_ticks_2004_2018`
**Tickers:** 3,478 ticker directories
**Periodo:** 2004-2005
**Estructura:** ticker/year=YYYY/month=MM/day=DD/data.parquet

**Columnas:**
- `timestamp` (int64) - Unix nanoseconds UTC
- `price` (float64) - Trade execution price
- `size` (int64) - Trade size in shares
- `exchange` (string) - Exchange code (e.g., 'Q' = NASDAQ)
- `conditions` (list<int>) - Trade condition codes
- `tape` (string) - Consolidation tape (A/B/C)

**Ejemplo de lectura:**
```python
import pyarrow.dataset as ds

# Leer un ticker especifico con filtros
dataset = ds.dataset(
    r'C:\TSIS_Data\trades_ticks_2004_2018\AAPL',
    format='parquet',
    partitioning='hive'
)

# Filtrar por fecha (pushdown predicates)
table = dataset.to_table(
    filter=(ds.field('year') == 2024) & (ds.field('month') == 1)
)

print(f"Trades: {table.num_rows:,}")
df = table.to_pandas()
```

---

**Dataset:** `trades_ticks_2019_2025`
**Ubicacion:** `C:\TSIS_Data\trades_ticks_2019_2025`
**Tickers:** 6,311 ticker directories
**Periodo:** 2017-2019
**Estructura:** ticker/year=YYYY/month=MM/day=DD/data.parquet

**Columnas:**
- `timestamp` (int64) - Unix nanoseconds UTC
- `price` (float64) - Trade execution price
- `size` (int64) - Trade size in shares
- `exchange` (string) - Exchange code (e.g., 'Q' = NASDAQ)
- `conditions` (list<int>) - Trade condition codes
- `tape` (string) - Consolidation tape (A/B/C)

**Ejemplo de lectura:**
```python
import pyarrow.dataset as ds

# Leer un ticker especifico con filtros
dataset = ds.dataset(
    r'C:\TSIS_Data\trades_ticks_2019_2025\AAPL',
    format='parquet',
    partitioning='hive'
)

# Filtrar por fecha (pushdown predicates)
table = dataset.to_table(
    filter=(ds.field('year') == 2024) & (ds.field('month') == 1)
)

print(f"Trades: {table.num_rows:,}")
df = table.to_pandas()
```

---

**Dataset:** `trades_ticks`
**Ubicacion:** `D:\TSIS_SmallCaps\raw\polygon\trades_ticks`
**Tickers:** 1,063 ticker directories
**Periodo:** N/A
**Estructura:** ticker/year=YYYY/month=MM/day=DD/data.parquet

**Columnas:**
- `timestamp` (int64) - Unix nanoseconds UTC
- `price` (float64) - Trade execution price
- `size` (int64) - Trade size in shares
- `exchange` (string) - Exchange code (e.g., 'Q' = NASDAQ)
- `conditions` (list<int>) - Trade condition codes
- `tape` (string) - Consolidation tape (A/B/C)

**Ejemplo de lectura:**
```python
import pyarrow.dataset as ds

# Leer un ticker especifico con filtros
dataset = ds.dataset(
    r'D:\TSIS_SmallCaps\raw\polygon\trades_ticks\AAPL',
    format='parquet',
    partitioning='hive'
)

# Filtrar por fecha (pushdown predicates)
table = dataset.to_table(
    filter=(ds.field('year') == 2024) & (ds.field('month') == 1)
)

print(f"Trades: {table.num_rows:,}")
df = table.to_pandas()
```

---

## PASO 2: Datos OHLCV Intraday (1-Minute)

### Descripcion General

Datos agregados a nivel 1-minuto construidos desde los trades tick-level. Cada barra
representa el resumen estadistico de todos los trades en ese intervalo de 60 segundos.

**Casos de uso:**
- Backtesting tradicional con barras 1-minute
- Feature engineering para ML (volatility, volume patterns)
- Construccion de timeframes superiores (5m, 15m, 1h, daily)
- VWAP computation y volume profile analysis

### Datasets Disponibles

**Dataset:** `ohlcv_intraday_1m`
**Ubicacion:** `C:\TSIS_Data\ohlcv_intraday_1m`
**Tickers:** 2 ticker directories
**Periodo:** N/A
**Estructura:** ticker/year=YYYY/month=MM/day=DD/data.parquet

**Columnas:**
- `timestamp` (int64) - Bar start time in Unix milliseconds UTC
- `open`, `high`, `low`, `close` (float64) - OHLC prices
- `volume` (int64) - Total volume in shares
- `vwap` (float64) - Volume-weighted average price
- `transactions` (int32) - Number of trades in bar

**Ejemplo de lectura:**
```python
import polars as pl

# Lazy scan para lectura eficiente
df = pl.scan_parquet(
    r'C:\TSIS_Data\ohlcv_intraday_1m\TSLA\**\*.parquet'
)

# Query con filtros
result = (
    df
    .filter(pl.col('year') == 2024)
    .filter(pl.col('volume') > 1000)
    .select(['timestamp', 'open', 'high', 'low', 'close', 'volume', 'vwap'])
    .collect()
)

print(f"Bars: {len(result):,}")
```

---

**Dataset:** `ohlcv_daily`
**Ubicacion:** `D:\TSIS_SmallCaps\raw\polygon\ohlcv_daily`
**Tickers:** 8,174 ticker directories
**Periodo:** 2022-2023
**Estructura:** ticker/year=YYYY/month=MM/day=DD/data.parquet

**Columnas:**
- `timestamp` (int64) - Bar start time in Unix milliseconds UTC
- `open`, `high`, `low`, `close` (float64) - OHLC prices
- `volume` (int64) - Total volume in shares
- `vwap` (float64) - Volume-weighted average price
- `transactions` (int32) - Number of trades in bar

**Ejemplo de lectura:**
```python
import polars as pl

# Lazy scan para lectura eficiente
df = pl.scan_parquet(
    r'D:\TSIS_SmallCaps\raw\polygon\ohlcv_daily\TSLA\**\*.parquet'
)

# Query con filtros
result = (
    df
    .filter(pl.col('year') == 2024)
    .filter(pl.col('volume') > 1000)
    .select(['timestamp', 'open', 'high', 'low', 'close', 'volume', 'vwap'])
    .collect()
)

print(f"Bars: {len(result):,}")
```

---

**Dataset:** `ohlcv_intraday_1m`
**Ubicacion:** `D:\TSIS_SmallCaps\raw\polygon\ohlcv_intraday_1m`
**Tickers:** 6,297 ticker directories
**Periodo:** N/A
**Estructura:** ticker/year=YYYY/month=MM/day=DD/data.parquet

**Columnas:**
- `timestamp` (int64) - Bar start time in Unix milliseconds UTC
- `open`, `high`, `low`, `close` (float64) - OHLC prices
- `volume` (int64) - Total volume in shares
- `vwap` (float64) - Volume-weighted average price
- `transactions` (int32) - Number of trades in bar

**Ejemplo de lectura:**
```python
import polars as pl

# Lazy scan para lectura eficiente
df = pl.scan_parquet(
    r'D:\TSIS_SmallCaps\raw\polygon\ohlcv_intraday_1m\TSLA\**\*.parquet'
)

# Query con filtros
result = (
    df
    .filter(pl.col('year') == 2024)
    .filter(pl.col('volume') > 1000)
    .select(['timestamp', 'open', 'high', 'low', 'close', 'volume', 'vwap'])
    .collect()
)

print(f"Bars: {len(result):,}")
```

---

## PASO 3: Datos Quotes (Bid/Ask Level 1)

### Descripcion General

Datos de quotes (NBBO - National Best Bid and Offer) nivel 1. Cada registro captura
el mejor bid/ask disponible en ese momento con precision de nanosegundos.

**Casos de uso:**
- Spread analysis y liquidity metrics
- Market impact modeling
- Order execution simulation (fill probability, slippage)
- Microstructure research (bid-ask bounce, adverse selection)

### Datasets Disponibles

**Dataset:** `quotes_2004_2018`
**Ubicacion:** `C:\TSIS_Data\quotes_2004_2018`
**Tickers:** 0 ticker directories
**Periodo:** N/A
**Estructura:** ticker/year=YYYY/month=MM/day=DD/data.parquet

**Columnas:**
- `timestamp` (int64) - Unix nanoseconds UTC
- `bid_price`, `ask_price` (float64) - Best bid/ask prices
- `bid_size`, `ask_size` (int64) - Size at best bid/ask (shares)
- `exchange` (string) - Exchange code
- `conditions` (list<int>) - Quote condition codes

**Ejemplo - Calcular spread promedio:**
```python
import polars as pl

df = pl.scan_parquet(r'C:\TSIS_Data\quotes_2004_2018\AAPL\year=2024\**\*.parquet')

spread_analysis = (
    df
    .with_columns([
        ((pl.col('ask_price') - pl.col('bid_price')) / pl.col('bid_price') * 10000).alias('spread_bps'),
        (pl.col('bid_size') + pl.col('ask_size')).alias('total_depth')
    ])
    .select([
        pl.col('spread_bps').mean().alias('avg_spread_bps'),
        pl.col('spread_bps').median().alias('median_spread_bps'),
        pl.col('total_depth').mean().alias('avg_depth_shares')
    ])
    .collect()
)

print(spread_analysis)
```

---

**Dataset:** `quotes_2029_2025`
**Ubicacion:** `C:\TSIS_Data\quotes_2029_2025`
**Tickers:** 0 ticker directories
**Periodo:** N/A
**Estructura:** ticker/year=YYYY/month=MM/day=DD/data.parquet

**Columnas:**
- `timestamp` (int64) - Unix nanoseconds UTC
- `bid_price`, `ask_price` (float64) - Best bid/ask prices
- `bid_size`, `ask_size` (int64) - Size at best bid/ask (shares)
- `exchange` (string) - Exchange code
- `conditions` (list<int>) - Quote condition codes

**Ejemplo - Calcular spread promedio:**
```python
import polars as pl

df = pl.scan_parquet(r'C:\TSIS_Data\quotes_2029_2025\AAPL\year=2024\**\*.parquet')

spread_analysis = (
    df
    .with_columns([
        ((pl.col('ask_price') - pl.col('bid_price')) / pl.col('bid_price') * 10000).alias('spread_bps'),
        (pl.col('bid_size') + pl.col('ask_size')).alias('total_depth')
    ])
    .select([
        pl.col('spread_bps').mean().alias('avg_spread_bps'),
        pl.col('spread_bps').median().alias('median_spread_bps'),
        pl.col('total_depth').mean().alias('avg_depth_shares')
    ])
    .collect()
)

print(spread_analysis)
```

---

**Dataset:** `quotes_p95_2004_2018`
**Ubicacion:** `C:\TSIS_Data\quotes_p95_2004_2018`
**Tickers:** 2,187 ticker directories
**Periodo:** 2004-2011
**Estructura:** ticker/year=YYYY/month=MM/day=DD/data.parquet

**Columnas:**
- `timestamp` (int64) - Unix nanoseconds UTC
- `bid_price`, `ask_price` (float64) - Best bid/ask prices
- `bid_size`, `ask_size` (int64) - Size at best bid/ask (shares)
- `exchange` (string) - Exchange code
- `conditions` (list<int>) - Quote condition codes

**Ejemplo - Calcular spread promedio:**
```python
import polars as pl

df = pl.scan_parquet(r'C:\TSIS_Data\quotes_p95_2004_2018\AAPL\year=2024\**\*.parquet')

spread_analysis = (
    df
    .with_columns([
        ((pl.col('ask_price') - pl.col('bid_price')) / pl.col('bid_price') * 10000).alias('spread_bps'),
        (pl.col('bid_size') + pl.col('ask_size')).alias('total_depth')
    ])
    .select([
        pl.col('spread_bps').mean().alias('avg_spread_bps'),
        pl.col('spread_bps').median().alias('median_spread_bps'),
        pl.col('total_depth').mean().alias('avg_depth_shares')
    ])
    .collect()
)

print(spread_analysis)
```

---

**Dataset:** `quotes_p95_2019_2025`
**Ubicacion:** `C:\TSIS_Data\quotes_p95_2019_2025`
**Tickers:** 4,620 ticker directories
**Periodo:** 2019
**Estructura:** ticker/year=YYYY/month=MM/day=DD/data.parquet

**Columnas:**
- `timestamp` (int64) - Unix nanoseconds UTC
- `bid_price`, `ask_price` (float64) - Best bid/ask prices
- `bid_size`, `ask_size` (int64) - Size at best bid/ask (shares)
- `exchange` (string) - Exchange code
- `conditions` (list<int>) - Quote condition codes

**Ejemplo - Calcular spread promedio:**
```python
import polars as pl

df = pl.scan_parquet(r'C:\TSIS_Data\quotes_p95_2019_2025\AAPL\year=2024\**\*.parquet')

spread_analysis = (
    df
    .with_columns([
        ((pl.col('ask_price') - pl.col('bid_price')) / pl.col('bid_price') * 10000).alias('spread_bps'),
        (pl.col('bid_size') + pl.col('ask_size')).alias('total_depth')
    ])
    .select([
        pl.col('spread_bps').mean().alias('avg_spread_bps'),
        pl.col('spread_bps').median().alias('median_spread_bps'),
        pl.col('total_depth').mean().alias('avg_depth_shares')
    ])
    .collect()
)

print(spread_analysis)
```

---

**Dataset:** `quotes_test`
**Ubicacion:** `C:\TSIS_Data\quotes_test`
**Tickers:** 1 ticker directories
**Periodo:** 2023
**Estructura:** ticker/year=YYYY/month=MM/day=DD/data.parquet

**Columnas:**
- `timestamp` (int64) - Unix nanoseconds UTC
- `bid_price`, `ask_price` (float64) - Best bid/ask prices
- `bid_size`, `ask_size` (int64) - Size at best bid/ask (shares)
- `exchange` (string) - Exchange code
- `conditions` (list<int>) - Quote condition codes

**Ejemplo - Calcular spread promedio:**
```python
import polars as pl

df = pl.scan_parquet(r'C:\TSIS_Data\quotes_test\AAPL\year=2024\**\*.parquet')

spread_analysis = (
    df
    .with_columns([
        ((pl.col('ask_price') - pl.col('bid_price')) / pl.col('bid_price') * 10000).alias('spread_bps'),
        (pl.col('bid_size') + pl.col('ask_size')).alias('total_depth')
    ])
    .select([
        pl.col('spread_bps').mean().alias('avg_spread_bps'),
        pl.col('spread_bps').median().alias('median_spread_bps'),
        pl.col('total_depth').mean().alias('avg_depth_shares')
    ])
    .collect()
)

print(spread_analysis)
```

---

## Datasets de Referencia

Datasets adicionales de metadata, fundamentales y reference data:

- **fundamentals**: 5 entries
- **reference**: 0 entries
- **reference**: 4 entries

---

## Acceso Programatico

### 1. PyArrow Dataset API (Recomendado para filtros)

```python
import pyarrow.dataset as ds

# Leer dataset completo de un ticker
dataset_path = r'C:\TSIS_Data\trades_ticks_2019_2025\AAPL'
dataset = ds.dataset(dataset_path, format='parquet', partitioning='hive')

# Filtrar por particion (pushdown predicates - MUY eficiente)
table = dataset.to_table(
    filter=(ds.field('year') == 2024) &
           (ds.field('month') == 6) &
           (ds.field('day') == 15)
)

print(f"Rows: {table.num_rows:,}")
print(f"Size: {table.nbytes / (1024**2):.2f} MB")

# Convertir a pandas
df = table.to_pandas()
```

### 2. Polars (Ultra-Rapido, Multi-threaded)

```python
import polars as pl

# Lazy scan (no carga todo en memoria)
df = pl.scan_parquet(r'C:\TSIS_Data\trades_ticks_2019_2025\AAPL\**\*.parquet')

# Query eficiente con lazy evaluation
result = (
    df
    .filter(pl.col('year') == 2024)
    .filter(pl.col('month') == 6)
    .filter(pl.col('price') > 150)
    .select(['timestamp', 'price', 'size', 'exchange'])
    .collect()  # Solo aqui se ejecuta la query
)

print(f"Filtered trades: {len(result):,}")
```

### 3. Listar Tickers Disponibles

```python
import os

def list_tickers(dataset_path):
    """Lista todos los tickers en un dataset"""
    tickers = []
    for entry in os.scandir(dataset_path):
        if entry.is_dir() and not entry.name.startswith('.') and not entry.name.startswith('year='):
            tickers.append(entry.name)
    return sorted(tickers)

tickers = list_tickers(r'C:\TSIS_Data\trades_ticks_2019_2025')
print(f"Total tickers: {len(tickers):,}")
print(f"Sample: {tickers[:10]}")
```

### 4. Leer Multiple Tickers en Paralelo

```python
import polars as pl
from concurrent.futures import ThreadPoolExecutor

def read_ticker_day(ticker, date_path):
    """Lee un ticker para un dia especifico"""
    path = f"C:\\TSIS_Data\\trades_ticks_2019_2025\\{ticker}\\{date_path}\\*.parquet"
    return pl.scan_parquet(path).collect()

# Leer multiples tickers en paralelo
tickers = ['AAPL', 'TSLA', 'NVDA', 'AMD', 'GOOGL']
date_path = 'year=2024/month=6/day=15'

with ThreadPoolExecutor(max_workers=5) as executor:
    results = list(executor.map(
        lambda t: read_ticker_day(t, date_path),
        tickers
    ))

total_trades = sum(len(df) for df in results)
print(f"Total trades across {len(tickers)} tickers: {total_trades:,}")
```

### 5. Convertir a Diferentes Timeframes

```python
import polars as pl

# Leer 1-minute bars
df_1m = pl.scan_parquet(r'C:\TSIS_Data\ohlcv_intraday_1m\AAPL\**\*.parquet')

# Agregar a 5-minute bars
df_5m = (
    df_1m
    .with_columns([
        (pl.col('timestamp') // (5 * 60 * 1000) * (5 * 60 * 1000)).alias('bar_5m')
    ])
    .group_by('bar_5m')
    .agg([
        pl.col('open').first(),
        pl.col('high').max(),
        pl.col('low').min(),
        pl.col('close').last(),
        pl.col('volume').sum(),
        pl.col('transactions').sum()
    ])
    .sort('bar_5m')
    .collect()
)

print(f"5-minute bars: {len(df_5m):,}")
```

---

**Notas Importantes:**

1. **Particionamiento Hive**: Siempre aprovechar los filtros por year/month/day para lectura eficiente
2. **Lazy Evaluation**: Usar `scan_parquet()` en lugar de `read_parquet()` cuando sea posible
3. **Memoria**: Los tick-level datasets son GRANDES - usar filtros antes de cargar a memoria
4. **Parallel Reading**: PyArrow y Polars leen archivos en paralelo automaticamente
5. **Timestamp Handling**:
   - Trades/Quotes: Unix nanoseconds (int64)
   - OHLCV: Unix milliseconds (int64)
   - Convertir a datetime: `pd.to_datetime(df['timestamp'], unit='ns')`

---

*Generado por audit_polygon_data_v6.py*
*Timestamp: {timestamp}*
*Infraestructura: C:\TSIS_Data + D:\TSIS_SmallCaps\raw\polygon*
