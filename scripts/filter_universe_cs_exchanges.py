# scripts/filter_universe_cs_exchanges.py
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
filter_universe_cs_exchanges.py

Filtra tickers por tipo CS (Common Stock) y exchanges XNAS/XNYS.

Uso:
  python scripts/filter_universe_cs_exchanges.py \
      --input processed/universe/tickers_2019_2025.parquet \
      --output processed/universe/tickers_2019_2025_cs_exchanges.csv
"""

import argparse
import polars as pl
from pathlib import Path

def log(msg: str):
    print(f"[INFO] {msg}", flush=True)

def filter_cs_exchanges(
    df: pl.DataFrame,
    ticker_type: str = "CS",
    exchanges: list = None
) -> pl.DataFrame:
    """
    Filtra por tipo de ticker y exchanges específicos.

    Args:
        df: DataFrame con tickers
        ticker_type: Tipo de ticker (default: "CS" = Common Stock)
        exchanges: Lista de exchanges (default: ["XNAS", "XNYS"])

    Returns:
        DataFrame filtrado
    """
    if exchanges is None:
        exchanges = ["XNAS", "XNYS"]

    log(f"Filtrando por type={ticker_type} y primary_exchange IN {exchanges}")

    # Normalizar a mayúsculas por si acaso
    df = df.with_columns([
        pl.col("type").str.to_uppercase().alias("type"),
        pl.col("primary_exchange").str.to_uppercase().alias("primary_exchange")
    ])

    # Verificar nulls antes de filtrar
    nulls_type = len(df.filter(pl.col("type").is_null()))
    nulls_exchange = len(df.filter(pl.col("primary_exchange").is_null()))

    if nulls_type > 0 or nulls_exchange > 0:
        log(f"  WARNING: Nulls encontrados: type={nulls_type:,}, primary_exchange={nulls_exchange:,}")
        log(f"  Los nulls seran excluidos del resultado")

    # Aplicar filtros
    df_filtered = df.filter(
        (pl.col("type") == ticker_type) &
        (pl.col("primary_exchange").is_in(exchanges))
    )

    return df_filtered

def print_statistics(df: pl.DataFrame, label: str = ""):
    """Imprime estadísticas del DataFrame"""
    log(f"\nEstadisticas {label}:")
    log(f"   Total tickers: {len(df):,}")

    if "active" in df.columns:
        n_active = len(df.filter(pl.col("active") == True))
        n_inactive = len(df.filter(pl.col("active") == False))
        log(f"   Activos hoy: {n_active:,} ({n_active/len(df)*100:.1f}%)")
        log(f"   Inactivos hoy: {n_inactive:,} ({n_inactive/len(df)*100:.1f}%)")

    if "type" in df.columns:
        n_types = df["type"].n_unique()
        log(f"   Tipos únicos: {n_types}")

        # Top 5 tipos
        type_dist = df.group_by("type").len().sort("len", descending=True).head(5)
        log(f"   Top 5 tipos:")
        for row in type_dist.iter_rows(named=True):
            log(f"     {row['type']}: {row['len']:,}")

    if "primary_exchange" in df.columns:
        n_exchanges = df["primary_exchange"].n_unique()
        log(f"   Exchanges únicos: {n_exchanges}")

        # Top 5 exchanges
        exchange_dist = df.group_by("primary_exchange").len().sort("len", descending=True).head(5)
        log(f"   Top 5 exchanges:")
        for row in exchange_dist.iter_rows(named=True):
            log(f"     {row['primary_exchange']}: {row['len']:,}")

def main():
    ap = argparse.ArgumentParser(description="Filtrar por CS + XNAS/XNYS")
    ap.add_argument("--input", type=str, required=True, help="Input parquet (tickers_2019_2025.parquet)")
    ap.add_argument("--output", type=str, required=True, help="Output CSV con tickers filtrados")
    ap.add_argument("--type", type=str, default="CS", help="Tipo de ticker (default: CS)")
    ap.add_argument("--exchanges", nargs="+", default=["XNAS", "XNYS"], help="Exchanges (default: XNAS XNYS)")
    args = ap.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        log(f"❌ ERROR: No existe {input_path}")
        return

    # Asegurar directorio output
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Cargar datos
    log(f"Cargando {input_path}...")
    df = pl.read_parquet(input_path)

    # Estadísticas PRE-filtro
    print_statistics(df, "PRE-filtro")

    # Aplicar filtros
    df_filtered = filter_cs_exchanges(df, args.type, args.exchanges)

    # Estadísticas POST-filtro
    print_statistics(df_filtered, "POST-filtro")

    # Porcentaje retenido
    retention = len(df_filtered) / len(df) * 100
    log(f"\n   Retención: {retention:.1f}% ({len(df_filtered):,} / {len(df):,})")

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
