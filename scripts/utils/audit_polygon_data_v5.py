#!/usr/bin/env python3
"""
Auditoria de Datos Polygon - v5
ESTILO EXACTO del reference document
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
    """Analyze partitioned dataset structure"""
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
                subdirs = list(os.scandir(dataset_path))
                ticker_dirs = [d for d in subdirs if d.is_dir() and not d.name.startswith('.') and not d.name.startswith('year=')]
                ticker_count = len(ticker_dirs)

                structure_type = 'unknown'
                year_range = None

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
    other_datasets = {}

    for name, info in all_datasets.items():
        if 'trade' in name.lower() or 'tick' in name.lower():
            tick_datasets[name] = info
        elif 'ohlcv' in name.lower() or 'daily' in name.lower():
            ohlcv_datasets[name] = info
        elif 'quote' in name.lower() or 'nbbo' in name.lower():
            quotes_datasets[name] = info
        else:
            other_datasets[name] = info

    total_tickers = sum(info['tickers'] for info in all_datasets.values())

    # Build markdown
    md_content = f"""# Pipeline de Datos Polygon - Infraestructura Tick-Level

**Fecha:** {timestamp}

## Tabla de Contenidos

1. [Route Map - Pipeline Completo](#route-map---pipeline-completo)
2. [Datasets Disponibles](#datasets-disponibles)
3. [Acceso Programatico](#acceso-programatico)

---

## Route Map - Pipeline Completo

```bash
================================================================================
                        INFRAESTRUCTURA DE DATOS POLYGON
================================================================================

PASO 1: DATOS TICK-LEVEL (TRADES)
--------------------------------------------------------------------------------
Ubicacion: C:\\TSIS_Data\\trades_ticks_*
Estructura: ticker/year=YYYY/month=MM/day=DD/data.parquet

"""

    # Add tick datasets
    for ds_name, ds_info in sorted(tick_datasets.items()):
        simple_name = ds_name.split('/')[-1]
        year_range = ds_info['year_range'] or 'N/A'
        tickers = ds_info['tickers']

        md_content += f"""        TRADES TICKS ({simple_name}): {tickers:,} tickers
        |- Periodo: {year_range}
        |- Estructura: Particionado Hive (ticker/year/month/day)
        +- Columnas: timestamp, price, size, exchange, conditions, tape

"""

    md_content += f"""

                              |  AGREGACION


PASO 2: DATOS OHLCV INTRADAY (1-MINUTE)
--------------------------------------------------------------------------------
Ubicacion: C:\\TSIS_Data\\ohlcv_* y D:\\TSIS_SmallCaps\\raw\\polygon\\ohlcv_*
Estructura: ticker/year=YYYY/month=MM/day=DD/data.parquet

"""

    # Add OHLCV datasets
    for ds_name, ds_info in sorted(ohlcv_datasets.items()):
        simple_name = ds_name.split('/')[-1]
        year_range = ds_info['year_range'] or 'N/A'
        tickers = ds_info['tickers']

        md_content += f"""        OHLCV ({simple_name}): {tickers:,} tickers
        |- Periodo: {year_range}
        |- Estructura: Particionado Hive (ticker/year/month/day)
        +- Columnas: timestamp, open, high, low, close, volume, vwap, transactions

"""

    md_content += f"""

                              |  QUOTES NIVEL 1


PASO 3: DATOS QUOTES (BID/ASK LEVEL 1)
--------------------------------------------------------------------------------
Ubicacion: C:\\TSIS_Data\\quotes_*
Estructura: ticker/year=YYYY/month=MM/day=DD/data.parquet

"""

    # Add quotes datasets
    for ds_name, ds_info in sorted(quotes_datasets.items()):
        simple_name = ds_name.split('/')[-1]
        year_range = ds_info['year_range'] or 'N/A'
        tickers = ds_info['tickers']

        md_content += f"""        QUOTES ({simple_name}): {tickers:,} tickers
        |- Periodo: {year_range}
        |- Estructura: Particionado Hive (ticker/year/month/day)
        +- Columnas: timestamp, bid_price, bid_size, ask_price, ask_size, conditions

"""

    md_content += f"""

================================================================================
   RESULTADO FINAL - INVENTARIO CONSOLIDADO
================================================================================

INFRAESTRUCTURA TOTAL:
   - Datasets totales: {len(all_datasets)}
   - Ticker directories: {total_tickers:,}
   - Periodo global: 2004-2025 (21 anos)

CATEGORIAS:
   |- Tick-Level (Trades): {len(tick_datasets)} datasets
   |- OHLCV Intraday: {len(ohlcv_datasets)} datasets
   |- Quotes Level 1: {len(quotes_datasets)} datasets
   +- Otros: {len(other_datasets)} datasets

READY FOR: Backtesting, ML feature engineering, Analisis cuantitativo

================================================================================
```

---

## Datasets Disponibles

### Trades Tick-Level

"""

    for ds_name, ds_info in sorted(tick_datasets.items()):
        location = "C:\\TSIS_Data" if "C_TSIS_Data" in ds_name else "D:\\TSIS_SmallCaps\\raw\\polygon"
        simple_name = ds_name.split('/')[-1]
        year_range = ds_info['year_range'] or 'N/A'
        tickers = ds_info['tickers']

        md_content += f"""**Dataset:** `{simple_name}`
**Ubicacion:** `{location}\\{simple_name}`
**Tickers:** {tickers:,}
**Periodo:** {year_range}
**Estructura:** ticker/year=YYYY/month=MM/day=DD/data.parquet

**Columnas:**
- timestamp (int64) - Unix nanoseconds
- price (float64) - Trade price
- size (int64) - Trade size (shares)
- exchange (string) - Exchange code
- conditions (list<int>) - Trade conditions
- tape (string) - Tape (A/B/C)

---

"""

    md_content += """### OHLCV Intraday

"""

    for ds_name, ds_info in sorted(ohlcv_datasets.items()):
        location = "C:\\TSIS_Data" if "C_TSIS_Data" in ds_name else "D:\\TSIS_SmallCaps\\raw\\polygon"
        simple_name = ds_name.split('/')[-1]
        year_range = ds_info['year_range'] or 'N/A'
        tickers = ds_info['tickers']

        md_content += f"""**Dataset:** `{simple_name}`
**Ubicacion:** `{location}\\{simple_name}`
**Tickers:** {tickers:,}
**Periodo:** {year_range}
**Estructura:** ticker/year=YYYY/month=MM/day=DD/data.parquet

**Columnas:**
- timestamp (int64) - Bar start time (Unix ms)
- open, high, low, close (float64)
- volume (int64)
- vwap (float64) - Volume weighted average price
- transactions (int32) - Number of trades in bar

---

"""

    md_content += """### Quotes (Bid/Ask Level 1)

"""

    for ds_name, ds_info in sorted(quotes_datasets.items()):
        location = "C:\\TSIS_Data" if "C_TSIS_Data" in ds_name else "D:\\TSIS_SmallCaps\\raw\\polygon"
        simple_name = ds_name.split('/')[-1]
        year_range = ds_info['year_range'] or 'N/A'
        tickers = ds_info['tickers']

        md_content += f"""**Dataset:** `{simple_name}`
**Ubicacion:** `{location}\\{simple_name}`
**Tickers:** {tickers:,}
**Periodo:** {year_range}
**Estructura:** ticker/year=YYYY/month=MM/day=DD/data.parquet

**Columnas:**
- timestamp (int64) - Unix nanoseconds
- bid_price, ask_price (float64)
- bid_size, ask_size (int64)
- exchange (string)
- conditions (list<int>)

---

"""

    md_content += """## Acceso Programatico

### Lectura con PyArrow Dataset (Recomendado)

```python
import pyarrow.dataset as ds

# Leer dataset completo de un ticker
dataset_path = r'C:\\TSIS_Data\\trades_ticks_2019_2025\\AAPL'
dataset = ds.dataset(dataset_path, format='parquet', partitioning='hive')

# Filtrar por particion (pushdown predicates)
table = dataset.to_table(
    filter=(ds.field('year') == 2019) & (ds.field('month') == 12)
)

print(f"Rows: {table.num_rows:,}")
df = table.to_pandas()
```

### Lectura con Polars (Ultra-Rapido)

```python
import polars as pl

# Lazy scan
df = pl.scan_parquet(r'C:\\TSIS_Data\\trades_ticks_2019_2025\\AAPL\\**\\*.parquet')

# Query eficiente
result = (
    df
    .filter(pl.col('year') == 2019)
    .filter(pl.col('price') > 100)
    .select(['timestamp', 'price', 'size'])
    .collect()
)
```

### Listar Tickers Disponibles

```python
import os

def list_tickers(dataset_path):
    tickers = []
    for entry in os.scandir(dataset_path):
        if entry.is_dir() and not entry.name.startswith('.'):
            tickers.append(entry.name)
    return sorted(tickers)

tickers = list_tickers(r'C:\\TSIS_Data\\trades_ticks_2019_2025')
print(f"Tickers: {len(tickers):,}")
```

---

*Generado por audit_polygon_data_v5.py*
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
    print("AUDITORIA DE DATOS POLYGON - v5 (ESTILO REFERENCE)")
    print("="*60)
    print(f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    generate_audit_report(root_paths, output_path)

    print(f"\nFin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\nOK Auditoria completada")
