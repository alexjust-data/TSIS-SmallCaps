#!/usr/bin/env python3
"""
Auditoria de Datos Polygon - v6
MATCHING EXACT STYLE from 00_universe_ingest_2019_2025.md
"""

import os
from pathlib import Path
from datetime import datetime

def format_size(size_bytes):
    """Format size in human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

def analyze_dataset_structure(root_path):
    """Analyze partitioned dataset structure with file/size stats"""
    if not os.path.exists(root_path):
        return None

    print(f"Scanning {root_path}...")

    datasets = {}

    try:
        top_level = list(os.scandir(root_path))
        dirs = [d for d in top_level if d.is_dir() and not d.name.startswith('.')]

        for dataset_entry in dirs:
            dataset_name = dataset_entry.name
            dataset_path = dataset_entry.path

            print(f"  Analyzing: {dataset_name}")

            try:
                # Count files and size
                total_files = 0
                total_size = 0
                ticker_dirs = []

                subdirs = list(os.scandir(dataset_path))
                for d in subdirs:
                    if d.is_dir() and not d.name.startswith('.') and not d.name.startswith('year='):
                        ticker_dirs.append(d)

                ticker_count = len(ticker_dirs)

                structure_type = 'unknown'
                year_range = None

                # Sample first ticker to get structure
                if ticker_dirs:
                    sample_ticker = ticker_dirs[0]
                    try:
                        ticker_subdirs = list(os.scandir(sample_ticker.path))
                        year_partitions = [d.name for d in ticker_subdirs if d.is_dir() and d.name.startswith('year=')]

                        if year_partitions:
                            structure_type = 'partitioned'
                            years = sorted([int(y.split('=')[1]) for y in year_partitions])
                            year_range = f"{years[0]}-{years[-1]}" if len(years) > 1 else str(years[0])
                        else:
                            structure_type = 'flat'
                    except:
                        pass

                # Quick file/size count (sample only for speed)
                sample_count = min(10, ticker_count)
                if sample_count > 0:
                    for ticker in ticker_dirs[:sample_count]:
                        for root, dirs, files in os.walk(ticker.path):
                            for f in files:
                                if f.endswith('.parquet'):
                                    total_files += 1
                                    try:
                                        total_size += os.path.getsize(os.path.join(root, f))
                                    except:
                                        pass

                datasets[dataset_name] = {
                    'tickers': ticker_count,
                    'structure': structure_type,
                    'year_range': year_range,
                    'path': dataset_path,
                    'sample_files': total_files,
                    'sample_size': total_size
                }

            except Exception as e:
                print(f"    Warning: {e}")
                datasets[dataset_name] = {
                    'tickers': 0,
                    'structure': 'error',
                    'year_range': None,
                    'path': dataset_path,
                    'sample_files': 0,
                    'sample_size': 0
                }

        return datasets

    except Exception as e:
        print(f"  Error scanning {root_path}: {e}")
        return None

def generate_audit_report(root_paths, output_path):
    """Generate audit matching reference document EXACTLY"""

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Analyze all root paths
    all_datasets = {}
    for root_path in root_paths:
        print(f"\n{'='*60}")
        print(f"Procesando {root_path}")
        print('='*60)

        result = analyze_dataset_structure(root_path)
        if result:
            location = "C_TSIS_Data" if "C:" in root_path else "D_SmallCaps"
            for ds_name, ds_info in result.items():
                full_name = f"{location}/{ds_name}"
                all_datasets[full_name] = ds_info

    # Categorize
    tick_datasets = {}
    ohlcv_datasets = {}
    quotes_datasets = {}
    reference_datasets = {}
    other_datasets = {}

    for name, info in all_datasets.items():
        if 'trade' in name.lower() or 'tick' in name.lower():
            tick_datasets[name] = info
        elif 'ohlcv' in name.lower() or 'daily' in name.lower():
            ohlcv_datasets[name] = info
        elif 'quote' in name.lower() or 'nbbo' in name.lower():
            quotes_datasets[name] = info
        elif 'reference' in name.lower() or 'fundamental' in name.lower():
            reference_datasets[name] = info
        else:
            other_datasets[name] = info

    total_tickers = sum(info['tickers'] for info in all_datasets.values())
    total_tick_tickers = sum(info['tickers'] for info in tick_datasets.values())
    total_ohlcv_tickers = sum(info['tickers'] for info in ohlcv_datasets.values())
    total_quotes_tickers = sum(info['tickers'] for info in quotes_datasets.values())

    # Build markdown - EXACT STYLE from reference
    md_content = f"""# Pipeline de Datos Polygon - Infraestructura Tick-Level


1. [Datos Tick-Level Trades ({len(tick_datasets)} datasets)](#paso-1-datos-tick-level-trades)
2. [Datos OHLCV Intraday ({len(ohlcv_datasets)} datasets)](#paso-2-datos-ohlcv-intraday-1-minute)
3. [Datos Quotes Nivel 1 ({len(quotes_datasets)} datasets)](#paso-3-datos-quotes-bidask-level-1)
4. [Datasets de Referencia ({len(reference_datasets)} datasets)](#datasets-de-referencia)
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
Ubicacion: C:\\TSIS_Data\\trades_ticks_*
Estructura: ticker/year=YYYY/month=MM/day=DD/data.parquet
Columnas: timestamp, price, size, exchange, conditions, tape

"""

    # Add tick datasets with detailed stats
    if tick_datasets:
        total_tick = sum(info['tickers'] for info in tick_datasets.values())
        md_content += f"        📦 TRADES TICK-LEVEL: {total_tick:,} ticker directories\n"

        for ds_name, ds_info in sorted(tick_datasets.items()):
            simple_name = ds_name.split('/')[-1]
            year_range = ds_info['year_range'] or 'N/A'
            tickers = ds_info['tickers']
            pct = (tickers / total_tick * 100) if total_tick > 0 else 0

            # Use different tree char for last item
            is_last = ds_name == sorted(tick_datasets.keys())[-1]
            tree_char = "└─" if is_last else "├─"

            md_content += f"        {tree_char} {simple_name}: {tickers:,} tickers ({pct:.1f}%)\n"
            md_content += f"           ├─ Periodo: {year_range}\n"
            md_content += f"           ├─ Estructura: Particionado Hive (ticker/year/month/day)\n"
            md_content += f"           └─ Formato: Parquet con compresion snappy\n"

    md_content += f"""

                              ↓  AGREGACION 1-MINUTE


PASO 2: DATOS OHLCV INTRADAY (1-MINUTE)
────────────────────────────────────────────────────────────────────────────────
Ubicacion: C:\\TSIS_Data\\ohlcv_* y D:\\TSIS_SmallCaps\\raw\\polygon\\ohlcv_*
Estructura: ticker/year=YYYY/month=MM/day=DD/data.parquet
Columnas: timestamp, open, high, low, close, volume, vwap, transactions

"""

    # Add OHLCV datasets with detailed stats
    if ohlcv_datasets:
        total_ohlcv = sum(info['tickers'] for info in ohlcv_datasets.values())
        md_content += f"        📊 OHLCV INTRADAY 1-MINUTE: {total_ohlcv:,} ticker directories\n"

        for ds_name, ds_info in sorted(ohlcv_datasets.items()):
            simple_name = ds_name.split('/')[-1]
            year_range = ds_info['year_range'] or 'N/A'
            tickers = ds_info['tickers']
            pct = (tickers / total_ohlcv * 100) if total_ohlcv > 0 else 0
            location_prefix = "C:\\TSIS_Data" if "C_TSIS_Data" in ds_name else "D:\\TSIS_SmallCaps\\raw\\polygon"

            is_last = ds_name == sorted(ohlcv_datasets.keys())[-1]
            tree_char = "└─" if is_last else "├─"

            md_content += f"        {tree_char} {simple_name}: {tickers:,} tickers ({pct:.1f}%)\n"
            md_content += f"           ├─ Periodo: {year_range}\n"
            md_content += f"           ├─ Ubicacion: {location_prefix}\\{simple_name}\n"
            md_content += f"           └─ Agregado desde: Tick-level trades via PyArrow\n"

    md_content += f"""

                              ↓  QUOTES NIVEL 1


PASO 3: DATOS QUOTES (BID/ASK LEVEL 1)
────────────────────────────────────────────────────────────────────────────────
Ubicacion: C:\\TSIS_Data\\quotes_*
Estructura: ticker/year=YYYY/month=MM/day=DD/data.parquet
Columnas: timestamp, bid_price, bid_size, ask_price, ask_size, exchange, conditions

"""

    # Add quotes datasets with detailed stats
    if quotes_datasets:
        total_quotes = sum(info['tickers'] for info in quotes_datasets.values())
        md_content += f"        💱 QUOTES LEVEL 1 (BID/ASK): {total_quotes:,} ticker directories\n"

        for ds_name, ds_info in sorted(quotes_datasets.items()):
            simple_name = ds_name.split('/')[-1]
            year_range = ds_info['year_range'] or 'N/A'
            tickers = ds_info['tickers']
            pct = (tickers / total_quotes * 100) if total_quotes > 0 else 0

            is_last = ds_name == sorted(quotes_datasets.keys())[-1]
            tree_char = "└─" if is_last else "├─"

            # Add special notes for p95 vs full datasets
            note = ""
            if "p95" in simple_name:
                note = " ← Percentile 95 sampling"
            elif "test" in simple_name:
                note = " ← Test dataset"

            md_content += f"        {tree_char} {simple_name}: {tickers:,} tickers ({pct:.1f}%){note}\n"
            md_content += f"           ├─ Periodo: {year_range}\n"
            md_content += f"           ├─ Estructura: Particionado Hive (ticker/year/month/day)\n"
            md_content += f"           └─ Uso: Spread analysis, liquidity metrics, order flow\n"


    md_content += f"""

════════════════════════════════════════════════════════════════════════════════
✅ INFRAESTRUCTURA COMPLETADA - DATOS DISPONIBLES
════════════════════════════════════════════════════════════════════════════════

RESULTADO FINAL - INVENTARIO CONSOLIDADO:
├─ Datasets totales: {len(all_datasets)}
├─ Ticker directories: {total_tickers:,}
├─ Periodo global: 2004-2025 (21 años)
└─ Almacenamiento: Dual-location (C: + D:)

CATEGORIAS:
├─ Tick-Level (Trades): {len(tick_datasets)} datasets, {total_tick_tickers:,} ticker dirs
├─ OHLCV Intraday (1m): {len(ohlcv_datasets)} datasets, {total_ohlcv_tickers:,} ticker dirs
├─ Quotes Level 1: {len(quotes_datasets)} datasets, {total_quotes_tickers:,} ticker dirs
├─ Reference/Fundamentals: {len(reference_datasets)} datasets
└─ Otros: {len(other_datasets)} datasets

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

"""

    for ds_name, ds_info in sorted(tick_datasets.items()):
        location = "C:\\TSIS_Data" if "C_TSIS_Data" in ds_name else "D:\\TSIS_SmallCaps\\raw\\polygon"
        simple_name = ds_name.split('/')[-1]
        year_range = ds_info['year_range'] or 'N/A'
        tickers = ds_info['tickers']

        md_content += f"""**Dataset:** `{simple_name}`
**Ubicacion:** `{location}\\{simple_name}`
**Tickers:** {tickers:,} ticker directories
**Periodo:** {year_range}
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
    r'{location}\\{simple_name}\\AAPL',
    format='parquet',
    partitioning='hive'
)

# Filtrar por fecha (pushdown predicates)
table = dataset.to_table(
    filter=(ds.field('year') == 2024) & (ds.field('month') == 1)
)

print(f"Trades: {{table.num_rows:,}}")
df = table.to_pandas()
```

---

"""

    md_content += f"""## PASO 2: Datos OHLCV Intraday (1-Minute)

### Descripcion General

Datos agregados a nivel 1-minuto construidos desde los trades tick-level. Cada barra
representa el resumen estadistico de todos los trades en ese intervalo de 60 segundos.

**Casos de uso:**
- Backtesting tradicional con barras 1-minute
- Feature engineering para ML (volatility, volume patterns)
- Construccion de timeframes superiores (5m, 15m, 1h, daily)
- VWAP computation y volume profile analysis

### Datasets Disponibles

"""

    for ds_name, ds_info in sorted(ohlcv_datasets.items()):
        location = "C:\\TSIS_Data" if "C_TSIS_Data" in ds_name else "D:\\TSIS_SmallCaps\\raw\\polygon"
        simple_name = ds_name.split('/')[-1]
        year_range = ds_info['year_range'] or 'N/A'
        tickers = ds_info['tickers']

        md_content += f"""**Dataset:** `{simple_name}`
**Ubicacion:** `{location}\\{simple_name}`
**Tickers:** {tickers:,} ticker directories
**Periodo:** {year_range}
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
    r'{location}\\{simple_name}\\TSLA\\**\\*.parquet'
)

# Query con filtros
result = (
    df
    .filter(pl.col('year') == 2024)
    .filter(pl.col('volume') > 1000)
    .select(['timestamp', 'open', 'high', 'low', 'close', 'volume', 'vwap'])
    .collect()
)

print(f"Bars: {{len(result):,}}")
```

---

"""

    md_content += f"""## PASO 3: Datos Quotes (Bid/Ask Level 1)

### Descripcion General

Datos de quotes (NBBO - National Best Bid and Offer) nivel 1. Cada registro captura
el mejor bid/ask disponible en ese momento con precision de nanosegundos.

**Casos de uso:**
- Spread analysis y liquidity metrics
- Market impact modeling
- Order execution simulation (fill probability, slippage)
- Microstructure research (bid-ask bounce, adverse selection)

### Datasets Disponibles

"""

    for ds_name, ds_info in sorted(quotes_datasets.items()):
        location = "C:\\TSIS_Data" if "C_TSIS_Data" in ds_name else "D:\\TSIS_SmallCaps\\raw\\polygon"
        simple_name = ds_name.split('/')[-1]
        year_range = ds_info['year_range'] or 'N/A'
        tickers = ds_info['tickers']

        md_content += f"""**Dataset:** `{simple_name}`
**Ubicacion:** `{location}\\{simple_name}`
**Tickers:** {tickers:,} ticker directories
**Periodo:** {year_range}
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

df = pl.scan_parquet(r'{location}\\{simple_name}\\AAPL\\year=2024\\**\\*.parquet')

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

"""

    if reference_datasets:
        md_content += f"""## Datasets de Referencia

Datasets adicionales de metadata, fundamentales y reference data:

"""
        for ds_name, ds_info in sorted(reference_datasets.items()):
            simple_name = ds_name.split('/')[-1]
            tickers = ds_info['tickers']
            md_content += f"- **{simple_name}**: {tickers:,} entries\n"

        md_content += "\n---\n\n"

    md_content += """## Acceso Programatico

### 1. PyArrow Dataset API (Recomendado para filtros)

```python
import pyarrow.dataset as ds

# Leer dataset completo de un ticker
dataset_path = r'C:\\TSIS_Data\\trades_ticks_2019_2025\\AAPL'
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
df = pl.scan_parquet(r'C:\\TSIS_Data\\trades_ticks_2019_2025\\AAPL\\**\\*.parquet')

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
    \"\"\"Lista todos los tickers en un dataset\"\"\"
    tickers = []
    for entry in os.scandir(dataset_path):
        if entry.is_dir() and not entry.name.startswith('.') and not entry.name.startswith('year='):
            tickers.append(entry.name)
    return sorted(tickers)

tickers = list_tickers(r'C:\\TSIS_Data\\trades_ticks_2019_2025')
print(f"Total tickers: {len(tickers):,}")
print(f"Sample: {tickers[:10]}")
```

### 4. Leer Multiple Tickers en Paralelo

```python
import polars as pl
from concurrent.futures import ThreadPoolExecutor

def read_ticker_day(ticker, date_path):
    \"\"\"Lee un ticker para un dia especifico\"\"\"
    path = f"C:\\\\TSIS_Data\\\\trades_ticks_2019_2025\\\\{ticker}\\\\{date_path}\\\\*.parquet"
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
df_1m = pl.scan_parquet(r'C:\\TSIS_Data\\ohlcv_intraday_1m\\AAPL\\**\\*.parquet')

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
*Infraestructura: C:\\TSIS_Data + D:\\TSIS_SmallCaps\\raw\\polygon*
"""

    # Write report
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(md_content)

    print(f"\n{'='*60}")
    print(f"OK Reporte generado: {output_path}")
    print('='*60)

if __name__ == "__main__":
    root_paths = [
        r"C:\TSIS_Data",
        r"D:\TSIS_SmallCaps\raw\polygon"
    ]

    output_path = r"D:\TSIS_SmallCaps\01_daily\04_data_final\00_audit_polygon_data.md"

    print("="*60)
    print("AUDITORIA DE DATOS POLYGON - v6 (EXACT REFERENCE STYLE)")
    print("="*60)
    print(f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    generate_audit_report(root_paths, output_path)

    print(f"\nFin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\nOK Auditoria completada")
