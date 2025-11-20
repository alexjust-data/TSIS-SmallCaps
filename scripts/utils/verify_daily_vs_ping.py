#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
verify_daily_vs_ping.py - Verifica daily descargado contra ping (SIN consultar Polygon)

Usa el ping_range que ya tienes como referencia en lugar de hacer requests a Polygon.
MUCHO MÁS RÁPIDO (segundos en lugar de minutos).

USO:
    python scripts/utils/verify_daily_vs_ping.py \
        --ping processed/universe/ping_range_2004_2018.parquet \
        --daily-root raw/polygon/ohlcv_daily \
        --period-start 2004-01-01 \
        --period-end 2018-12-31 \
        --output-prefix verify_daily_2004_2018
"""

import argparse
import polars as pl
from pathlib import Path
from datetime import datetime
import sys
import io

# Force UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

def log(msg):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)

def parse_args():
    p = argparse.ArgumentParser(description="Verificar daily vs ping (rápido)")
    p.add_argument("--ping", required=True, help="Archivo ping_range.parquet")
    p.add_argument("--daily-root", required=True, help="Directorio daily OHLCV")
    p.add_argument("--period-start", required=True, help="Fecha inicio (YYYY-MM-DD)")
    p.add_argument("--period-end", required=True, help="Fecha fin (YYYY-MM-DD)")
    p.add_argument("--output-prefix", default="verify_daily", help="Prefijo archivos salida")
    return p.parse_args()

def check_local_daily(ticker: str, daily_root: Path, period_start: str, period_end: str) -> dict:
    """Verifica qué daily tenemos localmente"""
    ticker_dir = daily_root / ticker

    if not ticker_dir.exists():
        return {
            "local_has_data": False,
            "local_first": None,
            "local_last": None,
            "local_days": 0,
            "local_years": []
        }

    dfs = []
    years_found = []

    for year_dir in ticker_dir.glob("year=*"):
        year_num = year_dir.name.split("=")[1]
        years_found.append(year_num)

        daily_file = year_dir / "daily.parquet"
        if daily_file.exists():
            try:
                df = pl.read_parquet(daily_file, columns=["date"])
                dfs.append(df)
            except Exception:
                continue

    if not dfs:
        return {
            "local_has_data": False,
            "local_first": None,
            "local_last": None,
            "local_days": 0,
            "local_years": sorted(years_found)
        }

    df = pl.concat(dfs, how="vertical_relaxed")
    df_period = df.filter(
        (pl.col("date") >= period_start) &
        (pl.col("date") <= period_end)
    )

    if len(df_period) == 0:
        return {
            "local_has_data": False,
            "local_first": None,
            "local_last": None,
            "local_days": 0,
            "local_years": sorted(years_found)
        }

    return {
        "local_has_data": True,
        "local_first": df_period["date"].min(),
        "local_last": df_period["date"].max(),
        "local_days": len(df_period),
        "local_years": sorted(years_found)
    }

def main():
    args = parse_args()

    log("=" * 80)
    log("VERIFICACIÓN DAILY vs PING (rápida, sin consultar Polygon)")
    log("=" * 80)

    # Cargar ping
    log(f"Cargando ping desde {args.ping}")
    ping_df = pl.read_parquet(args.ping)

    # Filtrar solo con datos
    tickers_with_data = ping_df.filter(pl.col("has_data") == True)
    log(f"  > {len(tickers_with_data):,} tickers con datos según ping")

    daily_root = Path(args.daily_root)

    # Verificar cada ticker
    log("Verificando tickers...")
    results = []

    for i, row in enumerate(tickers_with_data.iter_rows(named=True), 1):
        ticker = row["ticker"]
        ping_first = row["first_day"]
        ping_last = row["last_day"]

        # Verificar local
        local_info = check_local_daily(ticker, daily_root, args.period_start, args.period_end)

        result = {
            "ticker": ticker,
            "ping_first": ping_first,
            "ping_last": ping_last,
            **local_info
        }

        # Determinar status
        if not local_info["local_has_data"]:
            result["status"] = "MISSING"
            result["issue"] = "Ping dice que hay datos pero no descargamos"
        else:
            # Comparar rangos
            if (local_info["local_first"] == ping_first and
                local_info["local_last"] == ping_last):
                result["status"] = "COMPLETE"
                result["issue"] = ""
            elif local_info["local_first"] > ping_first:
                result["status"] = "LATE_START"
                result["issue"] = f"Comienza tarde: esperado {ping_first}, tenemos {local_info['local_first']}"
            elif local_info["local_last"] < ping_last:
                result["status"] = "EARLY_END"
                result["issue"] = f"Termina antes: esperado {ping_last}, tenemos {local_info['local_last']}"
            else:
                result["status"] = "MISMATCH"
                result["issue"] = "Rango no coincide con ping"

        results.append(result)

        if i % 500 == 0 or i == len(tickers_with_data):
            log(f"  Progreso: {i}/{len(tickers_with_data)} ({i/len(tickers_with_data)*100:.1f}%)")

    # Crear DataFrame
    results_df = pl.DataFrame(results)

    # Estadísticas
    log("")
    log("=" * 80)
    log("RESULTADOS")
    log("=" * 80)

    total = len(results_df)
    complete = len(results_df.filter(pl.col("status") == "COMPLETE"))
    missing = len(results_df.filter(pl.col("status") == "MISSING"))
    late_start = len(results_df.filter(pl.col("status") == "LATE_START"))
    early_end = len(results_df.filter(pl.col("status") == "EARLY_END"))
    mismatch = len(results_df.filter(pl.col("status") == "MISMATCH"))

    log(f"Total tickers verificados:       {total:,}")
    log(f"  ✅ COMPLETOS (match perfecto):  {complete:,} ({complete/total*100:.1f}%)")
    log(f"  ❌ FALTANTES (no descargados):  {missing:,} ({missing/total*100:.1f}%)")
    log(f"  ⚠️  COMIENZA TARDE:             {late_start:,} ({late_start/total*100:.1f}%)")
    log(f"  ⚠️  TERMINA ANTES:              {early_end:,} ({early_end/total*100:.1f}%)")
    log(f"  ⚠️  DESAJUSTE:                  {mismatch:,} ({mismatch/total*100:.1f}%)")
    log("")

    # Guardar
    log("Guardando archivos...")

    # Convertir local_years a string
    results_df_csv = results_df.with_columns(
        pl.col("local_years").list.join(",").alias("local_years")
    )

    full_report = f"{args.output_prefix}_full.csv"
    results_df_csv.write_csv(full_report)
    log(f"  ✅ {full_report}")

    problems_df = results_df_csv.filter(pl.col("status") != "COMPLETE")
    if len(problems_df) > 0:
        problems_report = f"{args.output_prefix}_problems.csv"
        problems_df.write_csv(problems_report)
        log(f"  ✅ {problems_report} ({len(problems_df):,} tickers)")

    log("")
    log("=" * 80)
    log("VERIFICACIÓN COMPLETADA")
    log("=" * 80)

if __name__ == "__main__":
    main()
