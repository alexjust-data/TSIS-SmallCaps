# scripts/create_hybrid_enriched_universe.py
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
create_hybrid_enriched_universe.py

Crea universo híbrido enriquecido combinando:
1. Base: tickers_2019_2025_cs_exchanges.parquet (8,307 tickers CS filtrados)
2. Enriquecimiento activos: ticker_details (market_cap, description, etc.)
3. Enriquecimiento inactivos: snapshot original (delisted_utc, cik, etc.)

Estrategia Dual:
- ACTIVOS: Obtienen market_cap desde ticker_details para filtrar < $2B
- INACTIVOS: NO tienen market_cap, pero tienen delisted_utc (crítico para ML)

Uso:
  python scripts/create_hybrid_enriched_universe.py \
      --base processed/universe/tickers_2019_2025_cs_exchanges.parquet \
      --details raw/polygon/reference/ticker_details/as_of_date=2025-11-01/details.parquet \
      --snapshot raw/polygon/reference/tickers_snapshot/snapshot_date=2025-11-01/tickers_all.parquet \
      --output processed/universe/hybrid_enriched_2025-11-01.parquet
"""

import argparse
import polars as pl
from pathlib import Path
import datetime as dt

def log(msg: str):
    """Log con timestamp"""
    print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)

def main():
    ap = argparse.ArgumentParser(description="Crear universo híbrido enriquecido")
    ap.add_argument("--base", type=str, required=True,
                    help="Base filtrada (tickers_2019_2025_cs_exchanges.parquet)")
    ap.add_argument("--details", type=str, required=True,
                    help="Ticker details descargados (details.parquet)")
    ap.add_argument("--snapshot", type=str, required=True,
                    help="Snapshot original completo (tickers_all.parquet)")
    ap.add_argument("--output", type=str, required=True,
                    help="Output parquet enriquecido")
    args = ap.parse_args()

    # Validar inputs
    base_path = Path(args.base)
    details_path = Path(args.details)
    snapshot_path = Path(args.snapshot)

    if not base_path.exists():
        log(f"ERROR: No existe {base_path}")
        return
    if not details_path.exists():
        log(f"ERROR: No existe {details_path}")
        return
    if not snapshot_path.exists():
        log(f"ERROR: No existe {snapshot_path}")
        return

    # Cargar datos
    log(f"Cargando datos base...")
    df_base = pl.read_parquet(base_path)
    log(f"   Base: {len(df_base):,} tickers")

    log(f"\nCargando ticker details...")
    df_details_raw = pl.read_parquet(details_path)
    log(f"   Details descargados: {len(df_details_raw):,}")

    # Filtrar solo tickers con datos válidos (sin error)
    df_details = df_details_raw.filter(
        pl.col("error").is_null() | (pl.col("error") == "")
    )
    log(f"   Details válidos: {len(df_details):,}")
    log(f"   Not found (404): {len(df_details_raw) - len(df_details):,} (esperado para inactivos)")

    log(f"\nCargando snapshot original...")
    df_snapshot = pl.read_parquet(snapshot_path)
    log(f"   Snapshot total: {len(df_snapshot):,} tickers")

    # Estadísticas PRE-merge
    log(f"\nEstadisticas PRE-merge:")
    n_active_base = len(df_base.filter(pl.col("active") == True))
    n_inactive_base = len(df_base.filter(pl.col("active") == False))
    log(f"   Base activos: {n_active_base:,}")
    log(f"   Base inactivos: {n_inactive_base:,}")

    # PASO 1: LEFT JOIN con ticker_details (para enriquecer activos)
    log(f"\nPASO 1: Enriqueciendo con ticker_details (activos)...")

    # Seleccionar solo columnas relevantes de details
    details_cols = [
        "ticker",
        "market_cap",
        "weighted_shares_outstanding",
        "share_class_shares_outstanding",
        "total_employees",
        "description",
        "sic_code",
        "sic_description",
        "homepage_url",
        "phone_number",
        "address"
    ]

    # Filtrar columnas que existen
    details_cols_exist = [c for c in details_cols if c in df_details.columns]
    df_details_select = df_details.select(details_cols_exist)

    # LEFT JOIN: mantiene todos de base, enriquece los que puede
    df_enriched = df_base.join(
        df_details_select,
        on="ticker",
        how="left",
        suffix="_details"  # Evitar conflictos de nombres
    )

    log(f"   Tickers enriquecidos con details: {len(df_enriched):,}")

    # Contar cuántos activos tienen market_cap
    if "market_cap" in df_enriched.columns:
        n_with_mcap = len(df_enriched.filter(
            (pl.col("active") == True) &
            (~pl.col("market_cap").is_null())
        ))
        log(f"   Activos con market_cap: {n_with_mcap:,}/{n_active_base:,} ({n_with_mcap/n_active_base*100:.1f}%)")

    # PASO 2: LEFT JOIN con snapshot original (para datos adicionales de inactivos)
    log(f"\nPASO 2: Enriqueciendo con snapshot original (todos)...")

    # Seleccionar columnas del snapshot que no estén ya en base
    snapshot_cols = [
        "ticker",
        "delisted_utc",
        "cik",
        "composite_figi",
        "share_class_figi",
        "list_date"
    ]

    # Filtrar columnas que existen en snapshot pero no en base
    snapshot_cols_exist = [
        c for c in snapshot_cols
        if c in df_snapshot.columns and c not in df_base.columns
    ]
    snapshot_cols_exist.insert(0, "ticker")  # Asegurar que ticker esté

    df_snapshot_select = df_snapshot.select(snapshot_cols_exist)

    # LEFT JOIN con snapshot
    df_enriched = df_enriched.join(
        df_snapshot_select,
        on="ticker",
        how="left",
        suffix="_snapshot"
    )

    log(f"   Tickers enriquecidos con snapshot: {len(df_enriched):,}")

    # Contar cuántos inactivos tienen delisted_utc
    if "delisted_utc" in df_enriched.columns:
        n_with_delisted = len(df_enriched.filter(
            (pl.col("active") == False) &
            (~pl.col("delisted_utc").is_null())
        ))
        log(f"   Inactivos con delisted_utc: {n_with_delisted:,}/{n_inactive_base:,} ({n_with_delisted/n_inactive_base*100:.1f}%)")

    # PASO 3: Estadísticas POST-merge
    log(f"\nEstadisticas POST-merge:")
    log(f"   Total tickers: {len(df_enriched):,}")
    log(f"   Total columnas: {len(df_enriched.columns)}")

    # Completitud de campos clave
    log(f"\nCompletitud de campos clave:")
    key_fields = {
        "market_cap": "Market Cap (activos)",
        "delisted_utc": "Delisted UTC (inactivos)",
        "weighted_shares_outstanding": "Shares Outstanding",
        "description": "Description",
        "sic_code": "SIC Code",
        "homepage_url": "Homepage URL",
        "total_employees": "Employees",
        "cik": "CIK",
        "composite_figi": "FIGI"
    }

    for field, label in key_fields.items():
        if field in df_enriched.columns:
            non_null = len(df_enriched.filter(~pl.col(field).is_null()))
            pct = non_null / len(df_enriched) * 100
            log(f"   {label:30s}: {non_null:,}/{len(df_enriched):,} ({pct:.1f}%)")

    # PASO 4: Guardar resultado
    log(f"\nGuardando universo enriquecido...")
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df_enriched.write_parquet(output_path)
    log(f"   Parquet: {output_path}")
    log(f"   Tamaño: {output_path.stat().st_size / 1024 / 1024:.2f} MB")

    # También guardar CSV para inspección (si es posible)
    try:
        output_csv = output_path.with_suffix('.csv')
        df_enriched.write_csv(output_csv)
        log(f"   CSV: {output_csv}")
    except Exception as e:
        log(f"   WARNING: No se pudo exportar CSV (datos anidados): {e}")

    # PASO 5: Resumen final
    log(f"\nResumen final:")
    log(f"   Total tickers enriquecidos: {len(df_enriched):,}")
    log(f"   Activos: {n_active_base:,}")
    log(f"   Inactivos: {n_inactive_base:,}")

    if "market_cap" in df_enriched.columns:
        # Estadísticas de market cap (solo activos con datos)
        df_active_with_mcap = df_enriched.filter(
            (pl.col("active") == True) &
            (~pl.col("market_cap").is_null())
        )

        if len(df_active_with_mcap) > 0:
            mcap_stats = df_active_with_mcap.select("market_cap").describe()
            log(f"\nEstadisticas Market Cap (activos con datos):")
            log(f"   Count: {len(df_active_with_mcap):,}")
            log(f"   Mean: ${mcap_stats['market_cap'][1]:,.0f}")
            log(f"   Median: ${mcap_stats['market_cap'][5]:,.0f}")
            log(f"   Max: ${mcap_stats['market_cap'][7]:,.0f}")

            # Contar small caps (< $2B)
            n_small_caps = len(df_active_with_mcap.filter(
                pl.col("market_cap") < 2_000_000_000
            ))
            log(f"\n   Small caps (< $2B): {n_small_caps:,} ({n_small_caps/len(df_active_with_mcap)*100:.1f}%)")

    log(f"\nHecho! Universo híbrido enriquecido creado exitosamente.")

if __name__ == "__main__":
    main()
