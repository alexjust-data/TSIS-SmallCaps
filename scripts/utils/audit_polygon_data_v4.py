#!/usr/bin/env python3
"""
Auditoria Tecnica de Datos Polygon - v4
REPLICANDO EL ESTILO DEL REFERENCE DOCUMENT
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
    """Analyze partitioned dataset structure efficiently"""

    if not os.path.exists(root_path):
        return None

    print(f"Scanning {root_path}...")

    datasets = {}

    try:
        # Get top-level directories (datasets)
        top_level = list(os.scandir(root_path))
        dirs = [d for d in top_level if d.is_dir() and not d.name.startswith('.')]

        for dataset_entry in dirs:
            dataset_name = dataset_entry.name
            dataset_path = dataset_entry.path

            print(f"  Analyzing: {dataset_name}")

            # Count ticker directories
            try:
                subdirs = list(os.scandir(dataset_path))
                ticker_dirs = [d for d in subdirs if d.is_dir() and not d.name.startswith('.') and not d.name.startswith('year=')]
                ticker_count = len(ticker_dirs)

                # Detect structure type (partitioned vs flat)
                structure_type = 'unknown'
                year_range = None

                if ticker_dirs:
                    # Sample first ticker to detect partitioning
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

                datasets[dataset_name] = {
                    'tickers': ticker_count,
                    'structure': structure_type,
                    'year_range': year_range,
                    'path': dataset_path
                }

            except Exception as e:
                print(f"    Warning: {e}")
                datasets[dataset_name] = {
                    'tickers': 0,
                    'structure': 'error',
                    'year_range': None,
                    'path': dataset_path
                }

        return datasets

    except Exception as e:
        print(f"  Error scanning {root_path}: {e}")
        return None

def generate_pipeline_audit(root_paths, output_path, parquet_stats=None):
    """Generate audit report matching reference document style"""

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Analyze all root paths
    all_datasets = {}
    for root_path in root_paths:
        print(f"\n{'='*60}")
        print(f"Procesando {root_path}")
        print('='*60)

        result = analyze_dataset_structure(root_path)
        if result:
            for ds_name, ds_info in result.items():
                full_name = f"{Path(root_path).name}/{ds_name}"
                all_datasets[full_name] = ds_info

    # Categorize datasets
    tick_datasets = {}
    ohlcv_datasets = {}
    quotes_datasets = {}
    other_datasets = {}

    for name, info in all_datasets.items():
        if 'trade' in name.lower() or 'tick' in name.lower():
            tick_datasets[name] = info
        elif 'ohlcv' in name.lower() or 'agg' in name.lower():
            ohlcv_datasets[name] = info
        elif 'quote' in name.lower() or 'nbbo' in name.lower():
            quotes_datasets[name] = info
        else:
            other_datasets[name] = info

    # Build markdown content
    md_content = f"""# Pipeline de Datos Polygon - Auditoria Tecnica

**Fecha:** {timestamp}

## Tabla de Contenidos

1. [Route Map - Pipeline Completo de Datos](#route-map---pipeline-completo-de-datos)
2. [Datasets Tick-Level (Trades)](#datasets-tick-level-trades)
3. [Datasets OHLCV Intraday](#datasets-ohlcv-intraday)
4. [Datasets Quotes (Bid/Ask)](#datasets-quotes-bidask)
5. [Acceso Programatico](#acceso-programatico)

---

## Route Map - Pipeline Completo de Datos

```bash
════════════════════════════════════════════════════════════════════════════════
                    ARQUITECTURA DE DATOS POLYGON - TICK-LEVEL
════════════════════════════════════════════════════════════════════════════════

PASO 1: DESCARGA TRADES TICK-LEVEL (2004-2018)
────────────────────────────────────────────────────────────────────────────────
Dataset: trades_ticks_2004_2018
Ubicacion: C:\\TSIS_Data\\trades_ticks_2004_2018
Estructura: ticker/year=YYYY/month=MM/day=DD/data.parquet
"""

    # Add tick-level datasets
    if tick_datasets:
        for ds_name, ds_info in sorted(tick_datasets.items()):
            year_range = ds_info['year_range'] or 'N/A'
            tickers = ds_info['tickers']

            if '2004' in ds_name or '2018' in ds_name:
                md_content += f"""
        📊 TRADES TICKS {year_range}: {tickers:,} tickers
        ├─ Estructura: Particionado Hive (ticker/year/month/day)
        ├─ Periodo: {year_range}
        ├─ Columnas: timestamp, price, size, exchange, conditions, tape
        └─ Uso: Reconstruccion tick-by-tick, microstructure analysis


                              ↓  CONTINUACION TEMPORAL


PASO 2: DESCARGA TRADES TICK-LEVEL (2019-2025)
────────────────────────────────────────────────────────────────────────────────
Dataset: trades_ticks_2019_2025
Ubicacion: C:\\TSIS_Data\\trades_ticks_2019_2025
Estructura: ticker/year=YYYY/month=MM/day=DD/data.parquet
"""
            elif '2019' in ds_name or '2025' in ds_name:
                md_content += f"""
        📊 TRADES TICKS {year_range}: {tickers:,} tickers
        ├─ Estructura: Particionado Hive (ticker/year/month/day)
        ├─ Periodo: {year_range}
        ├─ Columnas: timestamp, price, size, exchange, conditions, tape
        └─ Uso: Reconstruccion tick-by-tick, microstructure analysis


                          ↓  AGREGACION A 1-MINUTE BARS


"""

    # Add OHLCV datasets
    md_content += """PASO 3: AGREGACION OHLCV INTRADAY 1-MINUTE
────────────────────────────────────────────────────────────────────────────────
Dataset: ohlcv_intraday_1m
Ubicacion: C:\\TSIS_Data\\ohlcv_intraday_1m
Estructura: ticker/year=YYYY/month=MM/day=DD/data.parquet
"""

    if ohlcv_datasets:
        for ds_name, ds_info in sorted(ohlcv_datasets.items()):
            year_range = ds_info['year_range'] or 'N/A'
            tickers = ds_info['tickers']

            md_content += f"""
        📈 OHLCV 1-MINUTE {year_range}: {tickers:,} tickers
        ├─ Estructura: Particionado Hive (ticker/year/month/day)
        ├─ Periodo: {year_range}
        ├─ Columnas: timestamp, open, high, low, close, volume, vwap, transactions
        ├─ Barras por dia: ~390 (6.5 horas mercado regular)
        └─ Uso: Estrategias intraday, feature engineering ML


                          ↓  QUOTES NIVEL 1 (BID/ASK)


"""

    # Add quotes datasets
    md_content += """PASO 4: DESCARGA QUOTES LEVEL 1 (BID/ASK)
────────────────────────────────────────────────────────────────────────────────
Dataset: quotes_nbbo
Ubicacion: C:\\TSIS_Data\\quotes_nbbo
Estructura: ticker/year=YYYY/month=MM/day=DD/data.parquet
"""

    if quotes_datasets:
        for ds_name, ds_info in sorted(quotes_datasets.items()):
            year_range = ds_info['year_range'] or 'N/A'
            tickers = ds_info['tickers']

            md_content += f"""
        💰 QUOTES NBBO {year_range}: {tickers:,} tickers
        ├─ Estructura: Particionado Hive (ticker/year/month/day)
        ├─ Periodo: {year_range}
        ├─ Columnas: timestamp, bid_price, bid_size, ask_price, ask_size, conditions
        └─ Uso: Spread analysis, order book reconstruction


"""

    # Add statistics summary
    total_tickers = sum(info['tickers'] for info in all_datasets.values())

    md_content += f"""

════════════════════════════════════════════════════════════════════════════════
✅ PIPELINE DE DATOS COMPLETADO
════════════════════════════════════════════════════════════════════════════════

INVENTARIO CONSOLIDADO:
├─ Datasets totales: {len(all_datasets)}
├─ Ticker directories: {total_tickers:,}
"""

    if parquet_stats:
        md_content += f"""├─ Archivos parquet: {parquet_stats['total_files']:,}
├─ Tamano total: {format_size(parquet_stats['total_size'])}
"""

    md_content += f"""└─ Periodo global: 2004-2025 (21 anos)

CATEGORIAS:
├─ Tick-Level (Trades): {len(tick_datasets)} datasets
├─ OHLCV Intraday 1m: {len(ohlcv_datasets)} datasets
├─ Quotes Level 1: {len(quotes_datasets)} datasets
└─ Otros: {len(other_datasets)} datasets

READY FOR: Analisis cuantitativo, ML feature engineering, backtesting
════════════════════════════════════════════════════════════════════════════════
```

---

## Datasets Tick-Level (Trades)

### Estructura de Directorios

```bash
C:\\TSIS_Data\\
"""

    for ds_name, ds_info in sorted(tick_datasets.items()):
        simple_name = ds_name.split('/')[-1]
        tickers = ds_info['tickers']
        year_range = ds_info['year_range'] or 'N/A'

        md_content += f"""├── {simple_name}/                          ({tickers:,} tickers × {year_range})
│   ├── AAPL/
│   │   ├── year=2019/
│   │   │   ├── month=01/
│   │   │   │   ├── day=02/data.parquet
│   │   │   │   ├── day=03/data.parquet
│   │   │   │   └── ...
│   │   │   └── month=12/...
│   │   └── year=2025/...
│   ├── MSFT/...
│   └── [+{tickers-2:,} mas tickers]
"""

    md_content += """```

### Schema de Datos - Trades Ticks

**Columnas tipicas:**
- `timestamp` (int64) - Unix nanoseconds
- `price` (float64) - Trade price
- `size` (int64) - Trade size (shares)
- `exchange` (string) - Exchange code (Q=NASDAQ, N=NYSE, etc.)
- `conditions` (list<int>) - Trade conditions/qualifiers
- `tape` (string) - Tape (A/B/C)

**Ejemplo de lectura:**
```python
import pyarrow.dataset as ds

# Leer todo AAPL en 2019
dataset = ds.dataset('C:\\\\TSIS_Data\\\\trades_ticks_2019_2025\\\\AAPL',
                     format='parquet', partitioning='hive')
table = dataset.to_table(filter=(ds.field('year') == 2019))
print(f"Trades en 2019: {table.num_rows:,}")
```

---

## Datasets OHLCV Intraday

### Estructura de Directorios

```bash
C:\\TSIS_Data\\
"""

    for ds_name, ds_info in sorted(ohlcv_datasets.items()):
        simple_name = ds_name.split('/')[-1]
        tickers = ds_info['tickers']
        year_range = ds_info['year_range'] or 'N/A'

        md_content += f"""├── {simple_name}/                          ({tickers:,} tickers × {year_range})
│   ├── AAPL/
│   │   ├── year=2019/
│   │   │   ├── month=01/
│   │   │   │   ├── day=02/data.parquet      (~390 bars de 1min)
│   │   │   │   └── ...
│   │   │   └── ...
│   │   └── ...
│   └── [+{tickers-1:,} mas tickers]
"""

    md_content += """```

### Schema de Datos - OHLCV 1-Minute

**Columnas tipicas:**
- `timestamp` (int64) - Bar start time (Unix ms)
- `open` (float64)
- `high` (float64)
- `low` (float64)
- `close` (float64)
- `volume` (int64)
- `vwap` (float64) - Volume weighted average price
- `transactions` (int32) - Number of trades in bar

**Ejemplo de lectura:**
```python
import pandas as pd

# Leer un dia especifico
file_path = 'C:\\\\TSIS_Data\\\\ohlcv_intraday_1m\\\\AAPL\\\\year=2019\\\\month=12\\\\day=31\\\\data.parquet'
df = pd.read_parquet(file_path)
print(f"Bars: {len(df)} (deberia ser ~390)")
```

---

## Datasets Quotes (Bid/Ask)

### Estructura de Directorios

```bash
C:\\TSIS_Data\\
"""

    for ds_name, ds_info in sorted(quotes_datasets.items()):
        simple_name = ds_name.split('/')[-1]
        tickers = ds_info['tickers']
        year_range = ds_info['year_range'] or 'N/A'

        md_content += f"""├── {simple_name}/                          ({tickers:,} tickers × {year_range})
│   ├── AAPL/
│   │   ├── year=2019/
│   │   │   ├── month=01/
│   │   │   │   ├── day=02/data.parquet
│   │   │   │   └── ...
│   │   │   └── ...
│   │   └── ...
│   └── [+{tickers-1:,} mas tickers]
"""

    md_content += """```

### Schema de Datos - Quotes NBBO

**Columnas tipicas:**
- `timestamp` (int64) - Unix nanoseconds
- `bid_price` (float64)
- `bid_size` (int64)
- `ask_price` (float64)
- `ask_size` (int64)
- `exchange` (string)
- `conditions` (list<int>)

**Ejemplo de lectura:**
```python
import pyarrow.dataset as ds

# Leer quotes de AAPL en diciembre 2019
dataset = ds.dataset('C:\\\\TSIS_Data\\\\quotes_nbbo\\\\AAPL',
                     format='parquet', partitioning='hive')
table = dataset.to_table(
    filter=(ds.field('year') == 2019) & (ds.field('month') == 12)
)
print(f"Quotes: {table.num_rows:,}")
```

---

## Acceso Programatico

### Lectura con PyArrow Dataset (Recomendado)

```python
import pyarrow.dataset as ds

# Leer dataset completo de un ticker con filtros
dataset_path = r'C:\\TSIS_Data\\trades_ticks_2019_2025\\AAPL'
dataset = ds.dataset(dataset_path, format='parquet', partitioning='hive')

# Filtrar por particion (muy eficiente - pushdown predicates)
table = dataset.to_table(
    filter=(ds.field('year') == 2019) & (ds.field('month') == 12)
)

print(f"Rows: {table.num_rows:,}")
print(f"Columns: {table.column_names}")
print(table.schema)

# Convertir a pandas
df = table.to_pandas()
```

### Lectura con Polars (10-100x mas rapido)

```python
import polars as pl

# Lazy scan (no carga datos hasta .collect())
df = pl.scan_parquet(r'C:\\TSIS_Data\\trades_ticks_2019_2025\\AAPL\\**\\*.parquet')

# Ver schema sin cargar
print(df.schema)

# Query con filtros eficientes
result = (
    df
    .filter(pl.col('year') == 2019)
    .filter(pl.col('price') > 100)
    .select(['timestamp', 'price', 'size'])
    .collect()
)

print(result)
```

### Listar Tickers Disponibles

```python
import os

def list_tickers(dataset_path):
    '''Lista todos los tickers en un dataset'''
    tickers = []
    for entry in os.scandir(dataset_path):
        if entry.is_dir() and not entry.name.startswith('.'):
            tickers.append(entry.name)
    return sorted(tickers)

# Ejemplo
tickers = list_tickers(r'C:\\TSIS_Data\\trades_ticks_2019_2025')
print(f"Tickers disponibles: {len(tickers):,}")
print(tickers[:10])  # Primeros 10
```

### Query Multi-Ticker en Paralelo

```python
import pyarrow.dataset as ds
import pandas as pd
import os

def load_multiple_tickers(base_path, tickers, year=2019, month=12):
    '''Carga datos de multiples tickers en paralelo'''

    dfs = []
    for ticker in tickers:
        ticker_path = os.path.join(base_path, ticker)

        if os.path.exists(ticker_path):
            dataset = ds.dataset(ticker_path, format='parquet', partitioning='hive')

            # Filtrar por particion
            table = dataset.to_table(
                filter=(ds.field('year') == year) & (ds.field('month') == month)
            )

            if table.num_rows > 0:
                df = table.to_pandas()
                df['ticker'] = ticker
                dfs.append(df)

    # Concatenar todos
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()

# Uso
tickers_of_interest = ['AAPL', 'MSFT', 'GOOGL', 'AMZN']
df = load_multiple_tickers(
    r'C:\\TSIS_Data\\trades_ticks_2019_2025',
    tickers_of_interest,
    year=2019,
    month=12
)

print(f"Total trades cargados: {len(df):,}")
```

---

## Notas Tecnicas

### Performance

- **PyArrow Dataset**: Recomendado para datasets particionados masivos
- **Polars**: 10-100x mas rapido que pandas para queries grandes
- **Filtros de particion**: Usar filtros en `year`, `month`, `day` reduce I/O drasticamente
- **Lazy evaluation**: `pl.scan_parquet()` solo carga datos necesarios

### Almacenamiento

- Particionado Hive: `key=value/` permite pushdown de predicados
- Compresion SNAPPY por defecto en Parquet
- Cada particion ~100KB-50MB optimo para balance I/O vs overhead

### Consideraciones

- Millones de archivos: No usar `glob` directo, usar PyArrow Dataset
- Timestamps en UTC (Unix epoch)
- Trade conditions: Consultar Polygon docs para decodificar
- Survivorship bias: Tickers delistados preservados

---

*Generado por audit_polygon_data_v4.py*
*Timestamp: {timestamp}*
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
    print("AUDITORIA TECNICA DE DATOS POLYGON - v4")
    print("="*60)
    print(f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Try to get parquet stats from background task if available
    parquet_stats = None

    generate_pipeline_audit(root_paths, output_path, parquet_stats)

    print(f"\nFin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\nOK Auditoria completada")
