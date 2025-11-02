# scripts/filter_smallcaps_population.py
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
filter_smallcaps_population.py

Filtra la población target de Small Caps (< $2B) preservando survivorship bias.

ESTRATEGIA DUAL:
1. Filtrar ACTIVOS con market_cap < $2B
2. Preservar TODOS los INACTIVOS (sin filtro de market_cap, no disponible para ellos)
3. Resultado: Small Caps activos + sus contrapartes inactivos

Uso:
  python scripts/filter_smallcaps_population.py \
      --input processed/universe/hybrid_enriched_2025-11-01.parquet \
      --output processed/universe/smallcaps_universe_2025-11-01.parquet \
      --market-cap-threshold 2000000000
"""

import argparse
import polars as pl
from pathlib import Path
import datetime as dt

def log(msg: str):
    """Log con timestamp"""
    print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)

def main():
    ap = argparse.ArgumentParser(description="Filtrar población target de Small Caps")
    ap.add_argument("--input", type=str, required=True,
                    help="Input parquet enriquecido (hybrid_enriched_2025-11-01.parquet)")
    ap.add_argument("--output", type=str, required=True,
                    help="Output parquet con Small Caps target")
    ap.add_argument("--market-cap-threshold", type=float, default=2_000_000_000,
                    help="Umbral de market cap para Small Caps (default: $2B)")
    args = ap.parse_args()

    # Validar input
    input_path = Path(args.input)
    if not input_path.exists():
        log(f"ERROR: No existe {input_path}")
        return

    # Cargar universo enriquecido
    log(f"Cargando universo enriquecido...")
    df = pl.read_parquet(input_path)
    log(f"   Total tickers: {len(df):,}")

    # Estadísticas PRE-filtro
    n_active = len(df.filter(pl.col("active") == True))
    n_inactive = len(df.filter(pl.col("active") == False))
    log(f"   Activos: {n_active:,}")
    log(f"   Inactivos: {n_inactive:,}")

    # Contar activos con market_cap
    if "market_cap" in df.columns:
        n_with_mcap = len(df.filter(
            (pl.col("active") == True) &
            (~pl.col("market_cap").is_null())
        ))
        log(f"   Activos con market_cap: {n_with_mcap:,}/{n_active:,} ({n_with_mcap/n_active*100:.1f}%)")

    # FILTRO DUAL: Small Caps activos + TODOS los inactivos
    log(f"\nAplicando filtro dual...")
    log(f"   Umbral Small Caps: ${args.market_cap_threshold:,.0f}")

    # Filtrar ACTIVOS por market cap
    df_active_small_caps = df.filter(
        (pl.col("active") == True) &
        (~pl.col("market_cap").is_null()) &
        (pl.col("market_cap") < args.market_cap_threshold)
    )

    # Preservar TODOS los inactivos (sin filtro de market cap)
    df_inactive_all = df.filter(pl.col("active") == False)

    # Combinar ambos grupos
    df_target = pl.concat([df_active_small_caps, df_inactive_all])

    log(f"\nResultado del filtro:")
    log(f"   Small Caps activos (< ${args.market_cap_threshold:,.0f}): {len(df_active_small_caps):,}")
    log(f"   Inactivos preservados (sin filtro): {len(df_inactive_all):,}")
    log(f"   TOTAL poblacion target: {len(df_target):,}")

    # Estadísticas de market cap
    if "market_cap" in df_target.columns:
        log(f"\nEstadisticas market cap (solo activos):")
        mcap_stats = df_active_small_caps.select("market_cap").describe()
        log(f"   Mean: ${mcap_stats['market_cap'][1]:,.0f}")
        log(f"   Median: ${mcap_stats['market_cap'][5]:,.0f}")
        log(f"   Min: ${mcap_stats['market_cap'][3]:,.0f}")
        log(f"   Max: ${mcap_stats['market_cap'][7]:,.0f}")

        # Distribución por rangos
        log(f"\n   Distribucion por rangos:")
        ranges = [
            ("< $300M", 0, 300_000_000),
            ("$300M - $1B", 300_000_000, 1_000_000_000),
            ("$1B - $2B", 1_000_000_000, 2_000_000_000),
        ]

        for label, min_cap, max_cap in ranges:
            count = len(df_active_small_caps.filter(
                (pl.col("market_cap") >= min_cap) & (pl.col("market_cap") < max_cap)
            ))
            pct = count / len(df_active_small_caps) * 100
            log(f"   {label:<15} {count:>5,} ({pct:>5.1f}%)")

    # Distribución por exchange
    log(f"\nDistribucion por exchange:")
    for exchange in ["XNAS", "XNYS"]:
        count = len(df_target.filter(pl.col("primary_exchange") == exchange))
        pct = count / len(df_target) * 100
        log(f"   {exchange}: {count:,} ({pct:.1f}%)")

    # Guardar resultado
    log(f"\nGuardando poblacion target...")
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df_target.write_parquet(output_path)
    log(f"   Parquet: {output_path}")
    log(f"   Tamano: {output_path.stat().st_size / 1024 / 1024:.2f} MB")

    # También CSV (sin campos anidados)
    try:
        output_csv = output_path.with_suffix('.csv')
        df_target.write_csv(output_csv)
        log(f"   CSV: {output_csv}")
    except Exception as e:
        log(f"   WARNING: No se pudo exportar CSV (datos anidados): {e}")

    # Resumen final
    log(f"\nResumen final:")
    log(f"   Total poblacion target: {len(df_target):,}")
    log(f"   Small Caps activos: {len(df_active_small_caps):,} ({len(df_active_small_caps)/len(df_target)*100:.1f}%)")
    log(f"   Inactivos preservados: {len(df_inactive_all):,} ({len(df_inactive_all)/len(df_target)*100:.1f}%)")
    log(f"   Sin survivorship bias: SI")

    log(f"\nHecho! Poblacion target de Small Caps lista.")

if __name__ == "__main__":
    main()
