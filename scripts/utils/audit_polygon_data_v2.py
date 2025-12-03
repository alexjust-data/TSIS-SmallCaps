#!/usr/bin/env python3
"""
Auditoría Técnica de Datos Polygon/Massive
Genera reporte profesional con tree structures y estadísticas precisas
"""

import os
from pathlib import Path
from collections import defaultdict
from datetime import datetime

def format_size(size_bytes):
    """Format size in human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

def get_file_extension(filename):
    """Get file extension"""
    return Path(filename).suffix.lower()

def analyze_directory_tree(root_path, max_depth=3):
    """Analyze directory structure and create technical tree"""

    if not os.path.exists(root_path):
        return None

    # Track datasets and structure
    datasets = {}
    total_dirs = 0

    print(f"Scanning {root_path}...")

    # Get top-level structure
    try:
        top_level = list(os.scandir(root_path))
        dirs = sorted([d.name for d in top_level if d.is_dir()])
        files = [f for f in top_level if f.is_file()]

        # Build dataset inventory
        for dir_name in dirs:
            dir_path = os.path.join(root_path, dir_name)

            print(f"  Analyzing dataset: {dir_name}")

            # Count subdirectories (tickers)
            try:
                subdirs = list(os.scandir(dir_path))
                ticker_dirs = [d for d in subdirs if d.is_dir() and not d.name.startswith('.')]

                # Sample a ticker to understand structure
                sample_structure = None
                if ticker_dirs:
                    sample_ticker = ticker_dirs[0]
                    sample_path = sample_ticker.path

                    # Check if partitioned by year
                    try:
                        year_dirs = list(os.scandir(sample_path))
                        year_partitions = [d.name for d in year_dirs if d.is_dir() and d.name.startswith('year=')]

                        if year_partitions:
                            sample_structure = 'partitioned'
                            # Get year range
                            years = sorted([int(y.split('=')[1]) for y in year_partitions])
                            year_range = f"{years[0]}-{years[-1]}" if years else "unknown"
                        else:
                            sample_structure = 'flat'
                            year_range = None
                    except:
                        sample_structure = 'unknown'
                        year_range = None
                else:
                    year_range = None

                datasets[dir_name] = {
                    'tickers': len(ticker_dirs),
                    'structure': sample_structure,
                    'year_range': year_range,
                    'path': dir_path
                }

                total_dirs += len(ticker_dirs)

            except Exception as e:
                print(f"    Warning: {e}")
                datasets[dir_name] = {
                    'tickers': 0,
                    'structure': 'error',
                    'year_range': None,
                    'path': dir_path
                }

        return {
            'datasets': datasets,
            'top_files': files,
            'total_ticker_dirs': total_dirs
        }

    except Exception as e:
        print(f"  Error: {e}")
        return None

def generate_technical_report(root_paths, output_path):
    """Generate technical markdown audit"""

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    md_content = f"""# Auditoría Técnica - Datos Polygon/Massive

**Fecha:** {timestamp}

## Metodología

Esta auditoría analiza la estructura de datasets particionados de alta frecuencia:
- Identifica datasets y su organización (ticker → year → month → day)
- Cuenta directorios de tickers (millones de archivos parquet subyacentes)
- Proporciona comandos para lectura directa con PyArrow/Polars
- Mapea rutas para acceso programático

---

## Inventario de Datasets

"""

    # Analyze each root path
    for root_path in root_paths:
        print(f"\n{'='*60}")
        print(f"Processing {root_path}")
        print('='*60)

        result = analyze_directory_tree(root_path)

        if result is None:
            md_content += f"### {root_path}\n\n**Status:** No encontrado o inaccesible\n\n"
            continue

        # Generate tree structure
        md_content += f"""### {root_path}

**Estructura:**

```
{os.path.basename(root_path)}/
"""

        # Add datasets to tree
        for ds_name, ds_info in sorted(result['datasets'].items()):
            tickers = ds_info['tickers']
            structure = ds_info['structure']
            year_range = ds_info['year_range']

            md_content += f"├── {ds_name}/"

            if year_range:
                md_content += f"  ({tickers:,} tickers × {year_range})"
            else:
                md_content += f"  ({tickers:,} tickers)"

            md_content += "\n"

        # Add top-level files if any
        if result['top_files']:
            for f in result['top_files'][:5]:
                try:
                    size = f.stat().st_size
                    md_content += f"├── {f.name}  ({format_size(size)})\n"
                except:
                    md_content += f"├── {f.name}\n"

        md_content += "```\n\n"

        # Dataset details table
        md_content += "**Datasets:**\n\n"
        md_content += "| Dataset | Tickers | Estructura | Período |\n"
        md_content += "|---------|---------|------------|----------|\n"

        for ds_name, ds_info in sorted(result['datasets'].items(), key=lambda x: x[1]['tickers'], reverse=True):
            tickers = ds_info['tickers']
            structure = ds_info['structure'] or 'unknown'
            year_range = ds_info['year_range'] or 'N/A'

            md_content += f"| `{ds_name}` | {tickers:,} | {structure} | {year_range} |\n"

        md_content += f"\n**Total ticker directories:** {result['total_ticker_dirs']:,}\n\n"
        md_content += "---\n\n"

    # Add code examples section
    md_content += """## Acceso Programático

### Lectura con PyArrow (Datasets Particionados)

```python
import pyarrow.dataset as ds
import pyarrow.parquet as pq

# Ejemplo: Leer trades_ticks_2004_2018 para un ticker
dataset_path = r'C:\\TSIS_Data\\trades_ticks_2004_2018\\AAPL'

# Opción 1: PyArrow Dataset (recomendado para particionados)
dataset = ds.dataset(dataset_path, format='parquet', partitioning='hive')
table = dataset.to_table()
print(f"Rows: {table.num_rows:,}")
print(f"Columns: {table.column_names}")
print(table.schema)

# Filtrar por partición (más eficiente)
filtered = dataset.to_table(
    filter=(ds.field('year') == 2018) & (ds.field('month') == 12)
)

# Opción 2: Leer directamente con pandas
import pandas as pd
df = pd.read_parquet(dataset_path)
print(df.head())
```

### Lectura con Polars (Ultra-Rápido)

```python
import polars as pl

# Leer dataset completo de un ticker (lazy evaluation)
df = pl.scan_parquet(r'C:\\TSIS_Data\\trades_ticks_2004_2018\\AAPL\\**\\*.parquet')

# Ver schema sin cargar datos
print(df.schema)

# Query con filtros (lazy - solo carga lo necesario)
result = (
    df
    .filter(pl.col('year') == 2018)
    .filter(pl.col('price') > 100)
    .select(['timestamp', 'price', 'size'])
    .collect()
)

print(result)
```

### Lectura de OHLCV Intraday (1-minute)

```python
import pyarrow.parquet as pq
import pandas as pd

# Leer un día específico
file_path = r'C:\\TSIS_Data\\ohlcv_intraday_1m\\2004_2018\\AAPL\\year=2018\\month=12\\day=31\\data.parquet'

# Con pyarrow
table = pq.read_table(file_path)
df = table.to_pandas()

# Directamente con pandas
df = pd.read_parquet(file_path)

print(f"OHLCV bars: {len(df):,}")
print(df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].head())
```

### Listar Tickers Disponibles en un Dataset

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
tickers_2004_2018 = list_tickers(r'C:\\TSIS_Data\\trades_ticks_2004_2018')
print(f"Tickers disponibles: {len(tickers_2004_2018):,}")
print(tickers_2004_2018[:10])  # Primeros 10
```

### Query Masivo Multi-Ticker

```python
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pandas as pd

def load_multiple_tickers(base_path, tickers, year=2018, month=12):
    '''Carga datos de múltiples tickers en paralelo'''

    dfs = []
    for ticker in tickers:
        ticker_path = os.path.join(base_path, ticker)

        if os.path.exists(ticker_path):
            dataset = ds.dataset(ticker_path, format='parquet', partitioning='hive')

            # Filtrar por partición
            table = dataset.to_table(
                filter=(ds.field('year') == year) & (ds.field('month') == month)
            )

            if table.num_rows > 0:
                df = table.to_pandas()
                df['ticker'] = ticker
                dfs.append(df)

    # Concatenar todos
    if dfs:
        result = pd.concat(dfs, ignore_index=True)
        return result
    return pd.DataFrame()

# Uso
tickers_of_interest = ['AAPL', 'MSFT', 'GOOGL', 'AMZN']
df = load_multiple_tickers(
    r'C:\\TSIS_Data\\trades_ticks_2004_2018',
    tickers_of_interest,
    year=2018,
    month=12
)

print(f"Total trades cargados: {len(df):,}")
```

---

## Estructura de Datos

### Trades Ticks (Tick-Level)

**Particionado:** `ticker/year=YYYY/month=MM/day=DD/data.parquet`

**Columnas típicas:**
- `timestamp` (int64) - Unix nanoseconds
- `price` (float64) - Trade price
- `size` (int64) - Trade size (shares)
- `exchange` (string) - Exchange code
- `conditions` (list<int>) - Trade conditions
- `tape` (string) - Tape (A/B/C)

### OHLCV Intraday 1-minute

**Particionado:** `ticker/year=YYYY/month=MM/day=DD/data.parquet`

**Columnas típicas:**
- `timestamp` (int64) - Bar start time (Unix ms)
- `open` (float64)
- `high` (float64)
- `low` (float64)
- `close` (float64)
- `volume` (int64)
- `vwap` (float64) - Volume weighted average price
- `transactions` (int32) - Number of trades in bar

### Quotes (Bid/Ask Level 1)

**Particionado:** `ticker/year=YYYY/month=MM/day=DD/data.parquet`

**Columnas típicas:**
- `timestamp` (int64) - Unix nanoseconds
- `bid_price` (float64)
- `bid_size` (int64)
- `ask_price` (float64)
- `ask_size` (int64)
- `exchange` (string)
- `conditions` (list<int>)

---

## Notas Técnicas

### Performance

- **PyArrow Dataset**: Recomendado para datasets particionados masivos
- **Polars**: 10-100x más rápido que pandas para queries grandes
- **Filtros de partición**: Usar filtros en `year`, `month`, `day` reduce I/O drásticamente
- **Lazy evaluation**: `pl.scan_parquet()` solo carga datos necesarios

### Almacenamiento

- Particionado Hive: `key=value/` permite pushdown de predicados
- Compresión SNAPPY por defecto en Parquet
- Cada partición ~100KB-50MB óptimo para balance I/O vs overhead

### Consideraciones

- Millones de archivos: No usar `glob` directo, usar PyArrow Dataset
- Timestamps en UTC (Unix epoch)
- Trade conditions: Consultar Polygon docs para decodificar
- Survivorship bias: Tickers delistados preservados

---

*Generado por audit_polygon_data_v2.py*
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
    print("AUDITORÍA TÉCNICA DE DATOS POLYGON/MASSIVE")
    print("="*60)
    print(f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    generate_technical_report(root_paths, output_path)

    print(f"\nFin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\nOK Auditoria completada")
