#!/usr/bin/env python3
"""
Auditoría Técnica v3 - Formato Pipeline-Style
Genera documentación profesional con flowcharts y narrativa
"""

import os
from pathlib import Path
from collections import defaultdict
from datetime import datetime

def analyze_dataset_inventory(root_path):
    """Analyze datasets with ticker counts and year ranges"""

    if not os.path.exists(root_path):
        return None

    print(f"Analyzing {root_path}...")

    datasets = {}
    total_ticker_dirs = 0

    try:
        top_level = list(os.scandir(root_path))
        dirs = sorted([d.name for d in top_level if d.is_dir()])

        for dir_name in dirs:
            dir_path = os.path.join(root_path, dir_name)
            print(f"  Scanning dataset: {dir_name}")

            try:
                subdirs = list(os.scandir(dir_path))
                ticker_dirs = [d for d in subdirs if d.is_dir() and not d.name.startswith('.')]

                # Determine structure and year range
                sample_structure = 'flat'
                year_range = 'N/A'

                if ticker_dirs:
                    sample_ticker = ticker_dirs[0]
                    try:
                        year_dirs = list(os.scandir(sample_ticker.path))
                        year_partitions = [d.name for d in year_dirs if d.is_dir() and d.name.startswith('year=')]

                        if year_partitions:
                            sample_structure = 'partitioned'
                            years = sorted([int(y.split('=')[1]) for y in year_partitions])
                            year_range = f"{years[0]}-{years[-1]}" if years else "unknown"
                    except:
                        pass

                datasets[dir_name] = {
                    'tickers': len(ticker_dirs),
                    'structure': sample_structure,
                    'year_range': year_range,
                    'path': dir_path
                }

                total_ticker_dirs += len(ticker_dirs)

            except Exception as e:
                print(f"    Warning: {e}")
                datasets[dir_name] = {
                    'tickers': 0,
                    'structure': 'error',
                    'year_range': 'N/A',
                    'path': dir_path
                }

        return {
            'datasets': datasets,
            'total_ticker_dirs': total_ticker_dirs
        }

    except Exception as e:
        print(f"  Error: {e}")
        return None

def generate_pipeline_audit(root_paths, output_path):
    """Generate pipeline-style technical audit"""

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Analyze both locations
    print("\n" + "="*60)
    print("ANALYZING DATA LOCATIONS")
    print("="*60)

    c_result = analyze_dataset_inventory(root_paths[0])
    d_result = analyze_dataset_inventory(root_paths[1])

    # Start building markdown with pipeline style
    md_content = f"""# Auditoría Técnica - Infraestructura de Datos Polygon

**Fecha:** {timestamp}
**Tipo:** Auditoría de infraestructura de datasets particionados
**Alcance:** Datos completos 2004-2025

---

## Resumen Ejecutivo

Esta auditoría mapea la infraestructura completa de datos de mercado provenientes de Polygon (Massive API), organizada en dos ubicaciones principales con estructura particionada Hive-style.

### Estructura General

```
INFRAESTRUCTURA DE DATOS
════════════════════════════════════════════════════════════════════════════════

📦 UBICACIÓN PRINCIPAL: C:\\TSIS_Data
   └─ {c_result['total_ticker_dirs']:,} ticker directories
   └─ Datasets: {len(c_result['datasets'])}
   └─ Millones de archivos parquet subyacentes

📦 UBICACIÓN SECUNDARIA: D:\\TSIS_SmallCaps\\raw\\polygon
   └─ {d_result['total_ticker_dirs']:,} ticker directories
   └─ Datasets: {len(d_result['datasets'])}
   └─ Datos raw y procesamiento intermedio

════════════════════════════════════════════════════════════════════════════════
```

**Total de ticker directories:** {c_result['total_ticker_dirs'] + d_result['total_ticker_dirs']:,}

---

## Route Map - Arquitectura de Datos

### NIVEL 1: Organización de Datasets

Los datos están organizados en datasets especializados por tipo de información:

```bash
════════════════════════════════════════════════════════════════════════════════
                        ARQUITECTURA DE DATASETS
════════════════════════════════════════════════════════════════════════════════

CATEGORÍA 1: TICK-LEVEL DATA (Transacciones individuales)
────────────────────────────────────────────────────────────────────────────────
Dataset: trades_ticks_2019_2025
Ubicación: C:\\TSIS_Data\\trades_ticks_2019_2025
Tickers: {c_result['datasets'].get('trades_ticks_2019_2025', {}).get('tickers', 0):,}
Período: {c_result['datasets'].get('trades_ticks_2019_2025', {}).get('year_range', 'N/A')}
Estructura: ticker/year=YYYY/month=MM/day=DD/data.parquet

        📊 Cada trade individual con timestamp nanosegundos
        ├─ price, size, exchange, conditions, tape
        └─ Millones de transacciones por ticker/día

Dataset: trades_ticks_2004_2018
Ubicación: C:\\TSIS_Data\\trades_ticks_2004_2018
Tickers: {c_result['datasets'].get('trades_ticks_2004_2018', {}).get('tickers', 0):,}
Período: {c_result['datasets'].get('trades_ticks_2004_2018', {}).get('year_range', 'N/A')}
Estructura: ticker/year=YYYY/month=MM/day=DD/data.parquet

        📊 Datos históricos tick-level
        ├─ Mismo esquema que 2019-2025
        └─ Cobertura completa período pre-2019

                              ↓  AGREGACIÓN

CATEGORÍA 2: OHLCV INTRADAY (Barras de 1 minuto)
────────────────────────────────────────────────────────────────────────────────
Dataset: ohlcv_intraday_1m
Ubicación: C:\\TSIS_Data\\ohlcv_intraday_1m
Tickers: {c_result['datasets'].get('ohlcv_intraday_1m', {}).get('tickers', 0):,}
Estructura: ticker/year=YYYY/month=MM/day=DD/data.parquet

        📊 Barras OHLCV de 1 minuto agregadas desde ticks
        ├─ timestamp, open, high, low, close, volume, vwap
        ├─ transactions (count de trades por barra)
        └─ ~390 barras por día de mercado (6.5 horas trading)

                              ↓  NIVEL 1 QUOTES

CATEGORÍA 3: QUOTES DATA (Bid/Ask Level 1)
────────────────────────────────────────────────────────────────────────────────
Dataset: quotes_p95_2019_2025
Ubicación: C:\\TSIS_Data\\quotes_p95_2019_2025
Tickers: {c_result['datasets'].get('quotes_p95_2019_2025', {}).get('tickers', 0):,}
Período: {c_result['datasets'].get('quotes_p95_2019_2025', {}).get('year_range', 'N/A')}
Estructura: ticker/year=YYYY/month=MM/day=DD/data.parquet

        📊 Quotes de bid/ask con sampling p95 (reducción volumen)
        ├─ timestamp (nanosegundos)
        ├─ bid_price, bid_size, ask_price, ask_size
        ├─ exchange, conditions
        └─ Actualización por cada cambio en NBBO

Dataset: quotes_p95_2004_2018
Ubicación: C:\\TSIS_Data\\quotes_p95_2004_2018
Tickers: {c_result['datasets'].get('quotes_p95_2004_2018', {}).get('tickers', 0):,}
Período: {c_result['datasets'].get('quotes_p95_2004_2018', {}).get('year_range', 'N/A')}
Estructura: ticker/year=YYYY/month=MM/day=DD/data.parquet

        📊 Datos históricos quotes nivel 1
        └─ Mismo esquema que 2019-2025

                              ↓  AGREGACIÓN DIARIA

CATEGORÍA 4: OHLCV DAILY (Barras diarias)
────────────────────────────────────────────────────────────────────────────────
Dataset: ohlcv_daily
Ubicación: D:\\TSIS_SmallCaps\\raw\\polygon\\ohlcv_daily
Tickers: {d_result['datasets'].get('ohlcv_daily', {}).get('tickers', 0):,}
Período: {d_result['datasets'].get('ohlcv_daily', {}).get('year_range', 'N/A')}
Estructura: ticker/year=YYYY/month=MM/data.parquet

        📊 Barras OHLCV diarias agregadas
        ├─ Usado para análisis de largo plazo
        └─ Menor resolución, mayor cobertura temporal

                              ↓  DATOS COMPLEMENTARIOS

CATEGORÍA 5: REFERENCE & FUNDAMENTALS
────────────────────────────────────────────────────────────────────────────────
Dataset: fundamentals
Ubicación: C:\\TSIS_Data\\fundamentals
Tickers: {c_result['datasets'].get('fundamentals', {}).get('tickers', 0):,}

        📊 Balance sheets, income statements, cash flow
        └─ Reportes trimestrales y anuales (10-Q, 10-K)

Dataset: additional
Ubicación: C:\\TSIS_Data\\additional
Tickers: {c_result['datasets'].get('additional', {}).get('tickers', 0):,}

        📊 Splits, dividendos, earnings
        └─ Corporate actions y eventos

Dataset: short_data
Ubicación: C:\\TSIS_Data\\short_data
Tickers: {c_result['datasets'].get('short_data', {}).get('tickers', 0):,}

        📊 Short interest data
        └─ Posiciones cortas y utilización

════════════════════════════════════════════════════════════════════════════════
```

---

## Inventario Detallado por Ubicación

### C:\\TSIS_Data - Producción Principal

"""

    # Add C:\TSIS_Data table
    md_content += "| Dataset | Tickers | Estructura | Período | Uso |\n"
    md_content += "|---------|---------|------------|---------|-----|\n"

    # Sort by ticker count
    for ds_name, ds_info in sorted(c_result['datasets'].items(), key=lambda x: x[1]['tickers'], reverse=True):
        tickers = ds_info['tickers']
        structure = ds_info['structure']
        year_range = ds_info['year_range']

        # Determine usage
        if 'trades_ticks' in ds_name:
            uso = "Tick-level trades"
        elif 'quotes' in ds_name:
            uso = "Bid/Ask L1"
        elif 'ohlcv_intraday' in ds_name:
            uso = "OHLCV 1m bars"
        elif 'fundamentals' in ds_name:
            uso = "Financials"
        elif 'additional' in ds_name:
            uso = "Corp actions"
        elif 'short_data' in ds_name:
            uso = "Short interest"
        elif 'regime' in ds_name:
            uso = "Indicators"
        else:
            uso = "Mixed"

        md_content += f"| `{ds_name}` | {tickers:,} | {structure} | {year_range} | {uso} |\n"

    md_content += f"\n**Total ticker directories:** {c_result['total_ticker_dirs']:,}\n\n"

    # Add D:\TSIS_SmallCaps\raw\polygon table
    md_content += "### D:\\TSIS_SmallCaps\\raw\\polygon - Raw & Procesamiento\n\n"
    md_content += "| Dataset | Tickers | Estructura | Período | Uso |\n"
    md_content += "|---------|---------|------------|---------|-----|\n"

    for ds_name, ds_info in sorted(d_result['datasets'].items(), key=lambda x: x[1]['tickers'], reverse=True):
        tickers = ds_info['tickers']
        structure = ds_info['structure']
        year_range = ds_info['year_range']

        # Determine usage
        if 'ohlcv_daily' in ds_name:
            uso = "Daily bars"
        elif 'ohlcv_intraday' in ds_name:
            uso = "OHLCV 1m"
        elif 'trades' in ds_name:
            uso = "Tick trades"
        elif 'reference' in ds_name:
            uso = "Metadata"
        else:
            uso = "Mixed"

        md_content += f"| `{ds_name}` | {tickers:,} | {structure} | {year_range} | {uso} |\n"

    md_content += f"\n**Total ticker directories:** {d_result['total_ticker_dirs']:,}\n\n"

    # Add access patterns section
    md_content += """---

## Patrones de Acceso - Guía Técnica

### Lectura de Datos Particionados (PyArrow)

Para datasets particionados con estructura Hive (ticker/year=YYYY/month=MM/day=DD):

```python
import pyarrow.dataset as ds
import pyarrow.parquet as pq

# CASO 1: Leer un ticker completo (todos los años)
# ────────────────────────────────────────────────────────────────────────────
dataset_path = r'C:\\TSIS_Data\\trades_ticks_2019_2025\\AAPL'

# Crear dataset con particionado Hive
dataset = ds.dataset(dataset_path, format='parquet', partitioning='hive')

# Ver schema sin cargar datos
print(dataset.schema)

# Cargar todo el dataset (CUIDADO: puede ser HUGE)
table = dataset.to_table()
print(f"Total rows: {table.num_rows:,}")

# CASO 2: Filtrar por particiones (MÁS EFICIENTE)
# ────────────────────────────────────────────────────────────────────────────
# Solo diciembre 2024
filtered_table = dataset.to_table(
    filter=(ds.field('year') == 2024) & (ds.field('month') == 12)
)

# Convertir a pandas
df = filtered_table.to_pandas()
print(df.head())

# CASO 3: Query selectivo con columnas específicas
# ────────────────────────────────────────────────────────────────────────────
# Solo timestamp y price para reducir memoria
result = dataset.to_table(
    filter=(ds.field('year') == 2024) & (ds.field('month') == 12),
    columns=['timestamp', 'price', 'size']
)

df = result.to_pandas()
print(f"Shape: {df.shape}")
```

### Lectura Ultra-Rápida con Polars

Polars es 10-100x más rápido que pandas para grandes volúmenes:

```python
import polars as pl

# CASO 1: Lazy loading (query optimization)
# ────────────────────────────────────────────────────────────────────────────
df = pl.scan_parquet(r'C:\\TSIS_Data\\trades_ticks_2019_2025\\AAPL\\**\\*.parquet')

# Query optimizado - Polars empuja filtros a nivel de archivo
result = (
    df
    .filter(pl.col('year') == 2024)
    .filter(pl.col('month') == 12)
    .filter(pl.col('price') > 100)
    .select(['timestamp', 'price', 'size', 'exchange'])
    .collect()  # Solo aquí se ejecuta la query
)

print(result)

# CASO 2: Agregaciones sobre tick data
# ────────────────────────────────────────────────────────────────────────────
# Calcular VWAP por hora desde ticks
hourly_vwap = (
    df
    .filter(pl.col('year') == 2024)
    .with_columns([
        (pl.col('timestamp') / 3_600_000_000_000).cast(pl.Int64).alias('hour')
    ])
    .group_by('hour')
    .agg([
        (pl.col('price') * pl.col('size')).sum() / pl.col('size').sum().alias('vwap'),
        pl.col('size').sum().alias('volume'),
        pl.col('price').count().alias('num_trades')
    ])
    .collect()
)

print(hourly_vwap)

# CASO 3: Multi-ticker parallel load
# ────────────────────────────────────────────────────────────────────────────
tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN']

dfs = []
for ticker in tickers:
    ticker_df = (
        pl.scan_parquet(f'C:\\\\TSIS_Data\\\\trades_ticks_2019_2025\\\\{ticker}\\\\**\\\\*.parquet')
        .filter(pl.col('year') == 2024)
        .filter(pl.col('month') == 12)
        .with_columns(pl.lit(ticker).alias('ticker'))
        .collect()
    )
    dfs.append(ticker_df)

# Concatenar todos
combined = pl.concat(dfs)
print(f"Total rows: {len(combined):,}")
```

### Acceso a OHLCV Intraday (1-minute bars)

```python
import pandas as pd
import pyarrow.parquet as pq

# Leer un día específico de barras 1m
# ────────────────────────────────────────────────────────────────────────────
file_path = r'C:\\TSIS_Data\\ohlcv_intraday_1m\\AAPL\\year=2024\\month=12\\day=15\\data.parquet'

# Con pandas (simple)
df = pd.read_parquet(file_path)
print(f"Bars: {len(df):,}")
print(df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].head(10))

# Convertir timestamp a datetime
df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
df = df.set_index('datetime')

# Plot intraday
df[['open', 'high', 'low', 'close']].plot(figsize=(12, 6))
```

### Quotes (Bid/Ask) - Level 1 Data

```python
import polars as pl

# Leer quotes con sampling p95
# ────────────────────────────────────────────────────────────────────────────
quotes_path = r'C:\\TSIS_Data\\quotes_p95_2019_2025\\AAPL\\year=2024\\month=12\\day=15\\data.parquet'

df = pl.read_parquet(quotes_path)

# Calcular spread
df = df.with_columns([
    (pl.col('ask_price') - pl.col('bid_price')).alias('spread'),
    ((pl.col('ask_price') - pl.col('bid_price')) / pl.col('bid_price') * 10000).alias('spread_bps')
])

# Estadísticas de spread
print(df.select([
    pl.col('spread').mean().alias('avg_spread'),
    pl.col('spread').median().alias('median_spread'),
    pl.col('spread_bps').mean().alias('avg_spread_bps')
]))
```

---

## Estructura de Datos - Schemas

### Trades Ticks (Tick-Level)

**Particionado:** `ticker/year=YYYY/month=MM/day=DD/data.parquet`

```python
timestamp: int64           # Unix nanoseconds (UTC)
price: float64             # Trade price
size: int64                # Trade size (shares)
exchange: string           # Exchange code (P=NYSE, Q=NASDAQ, etc)
conditions: list<int32>    # Trade conditions (sale type, etc)
tape: string               # Tape (A=NYSE, B=NYSE Arca, C=NASDAQ)
```

**Ejemplo de uso:**
- Microstructure analysis
- High-frequency trading research
- Execution quality analysis
- Order flow imbalance

### OHLCV Intraday 1-minute

**Particionado:** `ticker/year=YYYY/month=MM/day=DD/data.parquet`

```python
timestamp: int64           # Bar start time (Unix milliseconds UTC)
open: float64              # Opening price
high: float64              # High price
low: float64               # Low price
close: float64             # Closing price
volume: int64              # Volume (shares)
vwap: float64              # Volume-weighted average price
transactions: int32        # Number of trades in bar
```

**Ejemplo de uso:**
- Intraday strategies
- Pattern recognition
- Support/resistance levels
- Volume profile analysis

### Quotes (Bid/Ask Level 1)

**Particionado:** `ticker/year=YYYY/month=MM/day=DD/data.parquet`

```python
timestamp: int64           # Unix nanoseconds (UTC)
bid_price: float64         # Best bid price
bid_size: int64            # Best bid size (shares)
ask_price: float64         # Best ask price
ask_size: int64            # Best ask size (shares)
exchange: string           # Exchange code
conditions: list<int32>    # Quote conditions
```

**Ejemplo de uso:**
- Spread analysis
- Liquidity measurement
- Market making research
- NBBO tracking

---

## Notas Técnicas Críticas

### Performance & Optimización

```bash
════════════════════════════════════════════════════════════════════════════════
                        BEST PRACTICES PARA GRANDES VOLÚMENES
════════════════════════════════════════════════════════════════════════════════

1. FILTRADO DE PARTICIONES
────────────────────────────────────────────────────────────────────────────────
   ✓ SIEMPRE usar filtros en year/month/day
   ✓ PyArrow hace "partition pruning" automático
   ✓ Reduce I/O en 90-99% vs cargar todo

2. SELECCIÓN DE COLUMNAS
────────────────────────────────────────────────────────────────────────────────
   ✓ Especificar columnas necesarias
   ✓ Parquet solo lee columnas solicitadas (columnar format)
   ✓ Reduce memoria y tiempo de lectura

3. LAZY EVALUATION
────────────────────────────────────────────────────────────────────────────────
   ✓ Usar pl.scan_parquet() en vez de pl.read_parquet()
   ✓ Permite optimización de query plan
   ✓ Solo carga datos necesarios al llamar .collect()

4. EVITAR GLOB DIRECTO
────────────────────────────────────────────────────────────────────────────────
   ✗ NO usar glob('**/*.parquet') para millones de archivos
   ✓ Usar PyArrow Dataset API o Polars scan
   ✓ Mucho más eficiente para datasets particionados

════════════════════════════════════════════════════════════════════════════════
```

### Almacenamiento & Compresión

- **Formato:** Parquet con compresión SNAPPY (default)
- **Particionado:** Hive-style (key=value/)
- **Tamaño de partición:** ~100KB - 50MB óptimo
- **Encoding:** Dictionary + RLE para strings
- **Metadata:** Schema embebido en cada archivo

### Timestamps

- **Trades/Quotes:** Unix nanoseconds (int64)
- **OHLCV 1m:** Unix milliseconds (int64)
- **OHLCV Daily:** Unix seconds o ISO date
- **Timezone:** UTC siempre
- **Market hours:** US Eastern Time (ET)

### Consideraciones de Uso

1. **Survivorship bias:** Los tickers delistados están preservados
2. **Corporate actions:** NO ajustados en tick data (usar 'additional' dataset)
3. **Trade conditions:** Consultar Polygon docs para decodificar
4. **Exchange codes:** Ver Polygon reference para mapping
5. **Missing data:** Gaps pueden ocurrir (halts, holidays, delisting)

---

## Comandos Rápidos - Exploración

```bash
# Listar tickers disponibles en un dataset
ls C:\\TSIS_Data\\trades_ticks_2019_2025 | wc -l

# Ver estructura de un ticker
tree /F C:\\TSIS_Data\\trades_ticks_2019_2025\\AAPL

# Contar archivos parquet (aproximado)
dir /S /B C:\\TSIS_Data\\trades_ticks_2019_2025\\AAPL\\*.parquet | measure

# Tamaño total de un ticker
du -sh C:\\TSIS_Data\\trades_ticks_2019_2025\\AAPL
```

```python
# Python: Listar tickers disponibles
import os

def list_tickers(dataset_path):
    return sorted([d for d in os.listdir(dataset_path)
                   if os.path.isdir(os.path.join(dataset_path, d))
                   and not d.startswith('.')])

tickers = list_tickers(r'C:\\TSIS_Data\\trades_ticks_2019_2025')
print(f"Total tickers: {len(tickers):,}")
print(f"Sample: {tickers[:10]}")
```

---

**Generado:** {timestamp}
**Script:** audit_polygon_data_v3.py
**Versión:** 3.0 - Pipeline-style documentation
"""

    # Write report
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(md_content)

    print(f"\n{'='*60}")
    print(f"OK Reporte generado: {output_path}")
    print(f"  Estilo: Pipeline/flowchart profesional")
    print(f"  Total ticker dirs: {c_result['total_ticker_dirs'] + d_result['total_ticker_dirs']:,}")
    print('='*60)

if __name__ == "__main__":
    root_paths = [
        r"C:\TSIS_Data",
        r"D:\TSIS_SmallCaps\raw\polygon"
    ]

    output_path = r"D:\TSIS_SmallCaps\01_daily\04_data_final\00_audit_polygon_data.md"

    print("="*60)
    print("AUDITORÍA V3 - PIPELINE-STYLE DOCUMENTATION")
    print("="*60)
    print(f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    generate_pipeline_audit(root_paths, output_path)

    print(f"\nFin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\nOK Auditoria completada - estilo profesional pipeline")
