# scripts/filter_splits_dividends_universe.py
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
filter_splits_dividends_universe.py

Filtra splits y dividends GLOBALES para quedarnos SOLO con los de nuestro universo
de 6,405 tickers Small Caps.

Este script toma los datos globales descargados (~26K splits, ~1.8M dividends) y
los filtra para nuestro universo target, reduciendo significativamente el tamaño
de los datos y mejorando la eficiencia en análisis posteriores.

Uso:
  python scripts/filter_splits_dividends_universe.py \
      --universe processed/universe/smallcaps_universe_2025-11-01.parquet \
      --splits-dir raw/polygon/reference/splits \
      --dividends-dir raw/polygon/reference/dividends \
      --output-dir processed/corporate_actions

Output:
  processed/corporate_actions/
  ├── splits/
  │   └── year=*/splits.parquet          # Splits filtrados por universo
  ├── dividends/
  │   └── year=*/dividends.parquet       # Dividends filtrados por universo
  └── summary.csv                         # Estadísticas de filtrado
"""

import argparse
import datetime as dt
from pathlib import Path
import polars as pl

def log(msg: str):
    """Log con timestamp"""
    print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)

def load_universe_tickers(universe_path: Path) -> set:
    """
    Carga el universo y extrae el set de tickers únicos

    Args:
        universe_path: Path al parquet del universo Small Caps

    Returns:
        Set de tickers únicos
    """
    log(f"Cargando universo desde {universe_path}...")
    df_universe = pl.read_parquet(universe_path)

    tickers = set(df_universe["ticker"].to_list())

    log(f"   Total tickers en universo: {len(tickers):,}")
    log(f"   Activos: {len(df_universe.filter(pl.col('active') == True)):,}")
    log(f"   Inactivos: {len(df_universe.filter(pl.col('active') == False)):,}")

    return tickers

def filter_splits(splits_dir: Path, tickers: set, output_dir: Path) -> dict:
    """
    Filtra splits globales por universo de tickers

    Args:
        splits_dir: Directorio con splits globales particionados
        tickers: Set de tickers del universo
        output_dir: Directorio de salida para splits filtrados

    Returns:
        Dict con estadísticas del filtrado
    """
    log("\nFiltrando SPLITS por universo...")

    # Encontrar todos los archivos de splits
    splits_files = sorted(splits_dir.glob("year=*/splits.parquet"))
    log(f"   Archivos de splits encontrados: {len(splits_files)}")

    if not splits_files:
        log("   WARNING: No se encontraron archivos de splits")
        return {"total_global": 0, "total_filtrado": 0, "tickers_con_splits": 0}

    # Cargar todos los splits
    df_splits_global = pl.concat([pl.read_parquet(f) for f in splits_files])
    n_global = len(df_splits_global)
    log(f"   Splits globales cargados: {n_global:,}")

    # Filtrar por tickers del universo
    df_splits_filtered = df_splits_global.filter(pl.col("ticker").is_in(tickers))
    n_filtered = len(df_splits_filtered)
    pct_retained = (n_filtered / n_global * 100) if n_global > 0 else 0

    log(f"   Splits filtrados: {n_filtered:,} ({pct_retained:.1f}% del total)")

    # Tickers únicos con splits
    tickers_con_splits = df_splits_filtered["ticker"].n_unique()
    log(f"   Tickers con splits: {tickers_con_splits:,}/{len(tickers):,} ({tickers_con_splits/len(tickers)*100:.1f}%)")

    if n_filtered > 0:
        # Particionar por año y guardar
        df_splits_filtered = df_splits_filtered.with_columns(
            pl.col("execution_date").str.slice(0, 4).alias("year")
        )

        log(f"\n   Guardando splits filtrados por año...")
        for year_tuple, partition in df_splits_filtered.group_by("year"):
            year = year_tuple[0]
            outdir = output_dir / "splits" / f"year={year}"
            outdir.mkdir(parents=True, exist_ok=True)

            partition_clean = partition.drop("year")
            partition_clean.write_parquet(outdir / "splits.parquet")

            log(f"      year={year}: {len(partition):,} splits")

        # Estadísticas adicionales
        years = sorted(df_splits_filtered["year"].unique().to_list())
        log(f"\n   Período: {years[0]}-{years[-1]} ({len(years)} años)")
        log(f"   Output: {output_dir / 'splits'}")

    return {
        "total_global": n_global,
        "total_filtrado": n_filtered,
        "tickers_con_splits": tickers_con_splits,
        "pct_retenido": pct_retained
    }

def filter_dividends(dividends_dir: Path, tickers: set, output_dir: Path) -> dict:
    """
    Filtra dividends globales por universo de tickers

    Args:
        dividends_dir: Directorio con dividends globales particionados
        tickers: Set de tickers del universo
        output_dir: Directorio de salida para dividends filtrados

    Returns:
        Dict con estadísticas del filtrado
    """
    log("\nFiltrando DIVIDENDS por universo...")

    # Encontrar todos los archivos de dividends
    dividends_files = sorted(dividends_dir.glob("year=*/dividends.parquet"))
    log(f"   Archivos de dividends encontrados: {len(dividends_files)}")

    if not dividends_files:
        log("   WARNING: No se encontraron archivos de dividends")
        return {"total_global": 0, "total_filtrado": 0, "tickers_con_dividends": 0}

    # Cargar todos los dividends
    df_dividends_global = pl.concat([pl.read_parquet(f) for f in dividends_files])
    n_global = len(df_dividends_global)
    log(f"   Dividends globales cargados: {n_global:,}")

    # Filtrar por tickers del universo
    df_dividends_filtered = df_dividends_global.filter(pl.col("ticker").is_in(tickers))
    n_filtered = len(df_dividends_filtered)
    pct_retained = (n_filtered / n_global * 100) if n_global > 0 else 0

    log(f"   Dividends filtrados: {n_filtered:,} ({pct_retained:.1f}% del total)")

    # Tickers únicos con dividends
    tickers_con_dividends = df_dividends_filtered["ticker"].n_unique()
    log(f"   Tickers con dividends: {tickers_con_dividends:,}/{len(tickers):,} ({tickers_con_dividends/len(tickers)*100:.1f}%)")

    if n_filtered > 0:
        # Particionar por año y guardar
        df_dividends_filtered = df_dividends_filtered.with_columns(
            pl.col("ex_dividend_date").str.slice(0, 4).alias("year")
        )

        log(f"\n   Guardando dividends filtrados por año...")
        for year_tuple, partition in df_dividends_filtered.group_by("year"):
            year = year_tuple[0]
            outdir = output_dir / "dividends" / f"year={year}"
            outdir.mkdir(parents=True, exist_ok=True)

            partition_clean = partition.drop("year")
            partition_clean.write_parquet(outdir / "dividends.parquet")

            log(f"      year={year}: {len(partition):,} dividends")

        # Estadísticas adicionales
        years = sorted(df_dividends_filtered["year"].unique().to_list())
        log(f"\n   Período: {years[0]}-{years[-1]} ({len(years)} años)")

        # Distribución por tipo de dividendo
        if "dividend_type" in df_dividends_filtered.columns:
            log(f"\n   Distribución por tipo:")
            type_dist = (df_dividends_filtered
                .group_by("dividend_type")
                .len()
                .sort("len", descending=True)
                .head(5)
            )
            for row in type_dist.iter_rows(named=True):
                dtype = row['dividend_type'] if row['dividend_type'] else "NULL"
                count = row['len']
                pct = count / n_filtered * 100
                log(f"      {dtype}: {count:,} ({pct:.1f}%)")

        log(f"\n   Output: {output_dir / 'dividends'}")

    return {
        "total_global": n_global,
        "total_filtrado": n_filtered,
        "tickers_con_dividends": tickers_con_dividends,
        "pct_retenido": pct_retained
    }

def create_summary(
    universe_size: int,
    splits_stats: dict,
    dividends_stats: dict,
    output_path: Path
):
    """
    Crea archivo de resumen con estadísticas del filtrado

    Args:
        universe_size: Tamaño del universo (número de tickers)
        splits_stats: Estadísticas de splits
        dividends_stats: Estadísticas de dividends
        output_path: Path al archivo de salida (summary.csv)
    """
    log("\nCreando resumen...")

    summary_data = {
        "metric": [
            "universe_size",
            "splits_global",
            "splits_filtrado",
            "splits_pct_retenido",
            "tickers_con_splits",
            "tickers_con_splits_pct",
            "dividends_global",
            "dividends_filtrado",
            "dividends_pct_retenido",
            "tickers_con_dividends",
            "tickers_con_dividends_pct",
        ],
        "value": [
            str(universe_size),
            str(splits_stats["total_global"]),
            str(splits_stats["total_filtrado"]),
            f"{splits_stats['pct_retenido']:.2f}%",
            str(splits_stats["tickers_con_splits"]),
            f"{splits_stats['tickers_con_splits']/universe_size*100:.1f}%",
            str(dividends_stats["total_global"]),
            str(dividends_stats["total_filtrado"]),
            f"{dividends_stats['pct_retenido']:.2f}%",
            str(dividends_stats["tickers_con_dividends"]),
            f"{dividends_stats['tickers_con_dividends']/universe_size*100:.1f}%",
        ]
    }

    df_summary = pl.DataFrame(summary_data, schema={"metric": pl.Utf8, "value": pl.Utf8})
    df_summary.write_csv(output_path)

    log(f"   Resumen guardado: {output_path}")

    # Imprimir resumen
    log("\n" + "="*80)
    log("RESUMEN DE FILTRADO - CORPORATE ACTIONS")
    log("="*80)
    log(f"\nUniverso Small Caps: {universe_size:,} tickers")
    log(f"\nSPLITS:")
    log(f"   Global: {splits_stats['total_global']:,}")
    log(f"   Filtrado: {splits_stats['total_filtrado']:,} ({splits_stats['pct_retenido']:.1f}%)")
    log(f"   Tickers con splits: {splits_stats['tickers_con_splits']:,} ({splits_stats['tickers_con_splits']/universe_size*100:.1f}%)")
    log(f"\nDIVIDENDS:")
    log(f"   Global: {dividends_stats['total_global']:,}")
    log(f"   Filtrado: {dividends_stats['total_filtrado']:,} ({dividends_stats['pct_retenido']:.1f}%)")
    log(f"   Tickers con dividends: {dividends_stats['tickers_con_dividends']:,} ({dividends_stats['tickers_con_dividends']/universe_size*100:.1f}%)")
    log("="*80)

def main():
    ap = argparse.ArgumentParser(
        description="Filtrar splits y dividends globales por universo Small Caps"
    )
    ap.add_argument("--universe", type=str, required=True,
                    help="Path al universo Small Caps (smallcaps_universe_2025-11-01.parquet)")
    ap.add_argument("--splits-dir", type=str, required=True,
                    help="Directorio con splits globales (raw/polygon/reference/splits)")
    ap.add_argument("--dividends-dir", type=str, required=True,
                    help="Directorio con dividends globales (raw/polygon/reference/dividends)")
    ap.add_argument("--output-dir", type=str, required=True,
                    help="Directorio de salida (processed/corporate_actions)")
    args = ap.parse_args()

    # Validar inputs
    universe_path = Path(args.universe)
    splits_dir = Path(args.splits_dir)
    dividends_dir = Path(args.dividends_dir)
    output_dir = Path(args.output_dir)

    if not universe_path.exists():
        log(f"ERROR: No existe el universo: {universe_path}")
        return

    if not splits_dir.exists():
        log(f"ERROR: No existe directorio de splits: {splits_dir}")
        return

    if not dividends_dir.exists():
        log(f"ERROR: No existe directorio de dividends: {dividends_dir}")
        return

    # Crear directorio de salida
    output_dir.mkdir(parents=True, exist_ok=True)

    log("="*80)
    log("FILTRADO DE SPLITS & DIVIDENDS POR UNIVERSO SMALL CAPS")
    log("="*80)

    # Cargar universo de tickers
    tickers = load_universe_tickers(universe_path)

    # Filtrar splits
    splits_stats = filter_splits(splits_dir, tickers, output_dir)

    # Filtrar dividends
    dividends_stats = filter_dividends(dividends_dir, tickers, output_dir)

    # Crear resumen
    create_summary(
        universe_size=len(tickers),
        splits_stats=splits_stats,
        dividends_stats=dividends_stats,
        output_path=output_dir / "summary.csv"
    )

    log("\nHecho! Corporate actions filtrados exitosamente.")
    log(f"Output: {output_dir}")

if __name__ == "__main__":
    main()
