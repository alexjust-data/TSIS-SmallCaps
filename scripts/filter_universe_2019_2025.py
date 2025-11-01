# scripts/filter_universe_2019_2025.py
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
filter_universe_2019_2025.py

Filtra tickers que estuvieron listados entre 2019-2025
desde el snapshot completo.

Uso:
  python scripts/filter_universe_2019_2025.py \
      --input raw/polygon/reference/tickers_snapshot/snapshot_date=2025-10-31/tickers_all.parquet \
      --output processed/universe/tickers_2019_2025.csv
"""

import argparse
import polars as pl
from pathlib import Path
from datetime import date

def log(msg: str):
    print(f"[INFO] {msg}", flush=True)

def was_listed_in_period(df: pl.DataFrame, start_year: int = 2019, end_year: int = 2025) -> pl.DataFrame:
    """
    Filtra tickers que estuvieron listados entre start_year y end_year.

    Lógica (sin list_date disponible):
    - Active=True: Incluir TODOS (están activos hoy)
    - Active=False: Incluir SOLO si delisted_utc >= start_year-01-01
      (fueron delistados durante o después del período, por lo tanto estuvieron activos en el período)

    Limitación: Asumimos que todos los activos hoy ya existían en 2019.
    """
    start_date = date(start_year, 1, 1)
    end_date = date(end_year, 12, 31)

    log(f"Filtrando tickers listados entre {start_date} y {end_date}")
    log(f"  NOTA: Polygon no proporciona 'list_date'")
    log(f"  Estrategia: Activos hoy + Inactivos delistados >= {start_date}")

    # Convertir delisted_utc a Date
    if "delisted_utc" in df.columns:
        df = df.with_columns(
            pl.col("delisted_utc").str.slice(0, 10).str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("delisted_date")
        )
    else:
        df = df.with_columns(pl.lit(None).alias("delisted_date"))

    # Filtrar:
    # 1. Activos hoy (asumimos existían en 2019+)
    # 2. Inactivos delistados durante/después del período
    df_filtered = df.filter(
        (pl.col("active") == True) |
        ((pl.col("active") == False) &
         (pl.col("delisted_date").is_not_null()) &
         (pl.col("delisted_date") >= start_date))
    )

    return df_filtered

def main():
    ap = argparse.ArgumentParser(description="Filtrar tickers 2019-2025")
    ap.add_argument("--input", type=str, required=True, help="Snapshot completo (parquet)")
    ap.add_argument("--output", type=str, required=True, help="Output CSV con tickers filtrados")
    ap.add_argument("--start-year", type=int, default=2019, help="Año inicio (default: 2019)")
    ap.add_argument("--end-year", type=int, default=2025, help="Año fin (default: 2025)")
    args = ap.parse_args()
    
    input_path = Path(args.input)
    output_path = Path(args.output)
    
    if not input_path.exists():
        log(f"❌ ERROR: No existe {input_path}")
        return
    
    # Asegurar directorio output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Cargar snapshot
    log(f"Cargando {input_path}...")
    df = pl.read_parquet(input_path)
    log(f"   Total tickers: {len(df):,}")
    
    # Filtrar por período
    df_filtered = was_listed_in_period(df, args.start_year, args.end_year)
    log(f"   Filtrados: {len(df_filtered):,} tickers ({len(df_filtered)/len(df)*100:.1f}%)")
    
    # Estadísticas
    if "active" in df_filtered.columns:
        n_active = len(df_filtered.filter(pl.col("active") == True))
        n_inactive = len(df_filtered.filter(pl.col("active") == False))
        log(f"   Activos hoy: {n_active:,} ({n_active/len(df_filtered)*100:.1f}%)")
        log(f"   Inactivos hoy: {n_inactive:,} ({n_inactive/len(df_filtered)*100:.1f}%)")
    
    # Contar por año
    log(f"\nDistribucion por año:")
    for year in range(args.start_year, args.end_year + 1):
        df_year = was_listed_in_period(df, year, year)
        log(f"   {year}: {len(df_year):,} tickers")

    # Exportar TODAS las columnas disponibles
    df_export = df_filtered

    # Escribir CSV
    df_export.write_csv(output_path)
    log(f"\nExportado: {output_path} ({len(df_export):,} tickers, {len(df_export.columns)} columnas)")

    # Escribir también parquet (más eficiente)
    parquet_path = output_path.with_suffix('.parquet')
    df_export.write_parquet(parquet_path)
    log(f"Exportado: {parquet_path}")

if __name__ == "__main__":
    main()