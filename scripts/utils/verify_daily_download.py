#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
verify_daily_download.py - Verifica daily OHLCV consultando Polygon directamente

Hace ping a Polygon por cada ticker para verificar:
1. ¿Qué rango de fechas tiene Polygon realmente? (first_day, last_day)
2. ¿Qué descargamos nosotros?
3. ¿Coinciden?

USO:
    python scripts/utils/verify_daily_download.py \
        --tickers processed/universe/smallcaps_universe_2004_2018.parquet \
        --daily-root raw/polygon/ohlcv_daily \
        --period-start 2004-01-01 \
        --period-end 2018-12-31 \
        --output-prefix verify_daily_2004_2018 \
        --workers 8
"""

import argparse
import polars as pl
from pathlib import Path
from datetime import datetime
import sys
import io
import requests
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import cpu_count

# Force UTF-8 for stdout/stderr
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

def log(msg):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)

def parse_args():
    p = argparse.ArgumentParser(description="Verificar descarga de daily OHLCV vs Polygon")
    p.add_argument("--tickers", required=True, help="CSV/Parquet con tickers a verificar")
    p.add_argument("--daily-root", required=True, help="Directorio con daily OHLCV descargado")
    p.add_argument("--period-start", required=True, help="Fecha inicio (YYYY-MM-DD)")
    p.add_argument("--period-end", required=True, help="Fecha fin (YYYY-MM-DD)")
    p.add_argument("--output-prefix", default="verify_daily", help="Prefijo para archivos")
    p.add_argument("--workers", type=int, default=8, help="Workers paralelos")
    return p.parse_args()

def ping_polygon_ticker(ticker: str, start_date: str, end_date: str, api_key: str) -> dict:
    """Hace ping a Polygon para saber qué datos tiene realmente"""
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}"

    try:
        response = requests.get(
            url,
            params={"apiKey": api_key, "limit": 1, "sort": "asc"},
            timeout=10
        )

        if response.status_code == 429:
            time.sleep(2)
            return ping_polygon_ticker(ticker, start_date, end_date, api_key)

        if response.status_code != 200:
            return {
                "ticker": ticker,
                "polygon_has_data": False,
                "polygon_first": None,
                "polygon_last": None,
                "polygon_days": 0,
                "error": f"HTTP {response.status_code}"
            }

        data = response.json()
        results_count = data.get("resultsCount", 0)

        if results_count == 0:
            return {
                "ticker": ticker,
                "polygon_has_data": False,
                "polygon_first": None,
                "polygon_last": None,
                "polygon_days": 0,
                "error": None
            }

        # Obtener primer y último día
        results = data.get("results", [])
        if results:
            first_ts = results[0].get("t")
            first_date = datetime.fromtimestamp(first_ts / 1000).strftime("%Y-%m-%d")

            # Para obtener el último, necesitamos otra query
            url_desc = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}"
            response_desc = requests.get(
                url_desc,
                params={"apiKey": api_key, "limit": 1, "sort": "desc"},
                timeout=10
            )

            if response_desc.status_code == 200:
                data_desc = response_desc.json()
                results_desc = data_desc.get("results", [])
                if results_desc:
                    last_ts = results_desc[0].get("t")
                    last_date = datetime.fromtimestamp(last_ts / 1000).strftime("%Y-%m-%d")
                else:
                    last_date = first_date
            else:
                last_date = first_date

            return {
                "ticker": ticker,
                "polygon_has_data": True,
                "polygon_first": first_date,
                "polygon_last": last_date,
                "polygon_days": results_count,
                "error": None
            }

        return {
            "ticker": ticker,
            "polygon_has_data": False,
            "polygon_first": None,
            "polygon_last": None,
            "polygon_days": 0,
            "error": None
        }

    except Exception as e:
        return {
            "ticker": ticker,
            "polygon_has_data": False,
            "polygon_first": None,
            "polygon_last": None,
            "polygon_days": 0,
            "error": str(e)
        }

def check_local_daily(ticker: str, daily_root: Path, period_start: str, period_end: str) -> dict:
    """Verifica qué daily tenemos descargado localmente"""
    ticker_dir = daily_root / ticker

    if not ticker_dir.exists():
        return {
            "local_has_data": False,
            "local_first": None,
            "local_last": None,
            "local_days": 0,
            "local_years": []
        }

    # Leer todos los daily.parquet
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

    # Concatenar y filtrar por periodo
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

def verify_ticker(args_tuple):
    """Verifica un ticker: consulta Polygon y compara con local"""
    ticker, daily_root, period_start, period_end, api_key = args_tuple

    # 1. Consultar Polygon
    polygon_info = ping_polygon_ticker(ticker, period_start, period_end, api_key)
    time.sleep(0.12)  # Rate limit

    # 2. Verificar local
    local_info = check_local_daily(ticker, daily_root, period_start, period_end)

    # 3. Comparar y determinar status
    result = {
        "ticker": ticker,
        **polygon_info,
        **local_info
    }

    # Determinar status
    if not polygon_info["polygon_has_data"]:
        if not local_info["local_has_data"]:
            result["status"] = "OK_NO_DATA"
            result["issue"] = "Polygon no tiene datos (correcto)"
        else:
            result["status"] = "ERROR_EXTRA_DATA"
            result["issue"] = "Tenemos datos pero Polygon no"
    else:
        if not local_info["local_has_data"]:
            result["status"] = "MISSING"
            result["issue"] = "Polygon tiene datos pero no descargamos"
        else:
            # Ambos tienen datos, comparar rangos Y cantidad de días
            if (local_info["local_first"] == polygon_info["polygon_first"] and
                local_info["local_last"] == polygon_info["polygon_last"]):

                # Verificar cantidad de días (auditoría día por día)
                if local_info["local_days"] == polygon_info["polygon_days"]:
                    result["status"] = "COMPLETE"
                    result["issue"] = None
                else:
                    result["status"] = "MISSING_DAYS"
                    days_diff = polygon_info["polygon_days"] - local_info["local_days"]
                    result["issue"] = f"Faltan {days_diff} días internos: Polygon={polygon_info['polygon_days']}, local={local_info['local_days']}"

            elif local_info["local_first"] > polygon_info["polygon_first"]:
                result["status"] = "LATE_START"
                days_diff = polygon_info["polygon_days"] - local_info["local_days"] if polygon_info["polygon_days"] else 0
                result["issue"] = f"Comienza tarde: esperado {polygon_info['polygon_first']}, tenemos {local_info['local_first']} (faltan ~{days_diff} días)"
            elif local_info["local_last"] < polygon_info["polygon_last"]:
                result["status"] = "EARLY_END"
                days_diff = polygon_info["polygon_days"] - local_info["local_days"] if polygon_info["polygon_days"] else 0
                result["issue"] = f"Termina antes: esperado {polygon_info['polygon_last']}, tenemos {local_info['local_last']} (faltan ~{days_diff} días)"
            else:
                result["status"] = "MISMATCH"
                days_diff = polygon_info["polygon_days"] - local_info["local_days"] if polygon_info["polygon_days"] else 0
                result["issue"] = f"Rango no coincide con Polygon (diferencia: {days_diff} días)"

    return result

def main():
    args = parse_args()

    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        log("ERROR: Variable POLYGON_API_KEY no establecida")
        sys.exit(1)

    log("=" * 80)
    log("VERIFICACIÓN DAILY vs POLYGON (con ping directo)")
    log("=" * 80)

    # Cargar tickers
    log(f"Cargando tickers desde {args.tickers}")
    if args.tickers.endswith(".parquet"):
        df = pl.read_parquet(args.tickers)
    else:
        df = pl.read_csv(args.tickers)

    tickers = df["ticker"].unique().to_list()
    log(f"  > {len(tickers):,} tickers a verificar")
    log(f"  > Periodo: {args.period_start} → {args.period_end}")
    log(f"  > Workers: {args.workers}")

    daily_root = Path(args.daily_root)

    # Verificar tickers en paralelo
    log("")
    log("Verificando tickers (consultando Polygon + local)...")

    verify_args = [
        (ticker, daily_root, args.period_start, args.period_end, api_key)
        for ticker in tickers
    ]

    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(verify_ticker, arg): arg[0] for arg in verify_args}

        for i, future in enumerate(as_completed(futures), 1):
            try:
                result = future.result()
                results.append(result)

                if i % 100 == 0 or i == len(verify_args):
                    log(f"  Progreso: {i}/{len(verify_args)} ({i/len(verify_args)*100:.1f}%)")
            except Exception as e:
                ticker = futures[future]
                log(f"  ERROR en {ticker}: {e}")
                results.append({
                    "ticker": ticker,
                    "status": "ERROR",
                    "issue": str(e)
                })

    # Crear DataFrame
    results_df = pl.DataFrame(results)

    # Estadísticas
    log("")
    log("=" * 80)
    log("RESULTADOS")
    log("=" * 80)

    total = len(results_df)
    complete = len(results_df.filter(pl.col("status") == "COMPLETE"))
    ok_no_data = len(results_df.filter(pl.col("status") == "OK_NO_DATA"))
    missing = len(results_df.filter(pl.col("status") == "MISSING"))
    missing_days = len(results_df.filter(pl.col("status") == "MISSING_DAYS"))
    late_start = len(results_df.filter(pl.col("status") == "LATE_START"))
    early_end = len(results_df.filter(pl.col("status") == "EARLY_END"))
    mismatch = len(results_df.filter(pl.col("status") == "MISMATCH"))
    extra_data = len(results_df.filter(pl.col("status") == "ERROR_EXTRA_DATA"))
    errors = len(results_df.filter(pl.col("status") == "ERROR"))

    log(f"Total tickers:                   {total:,}")
    log(f"  ✅ COMPLETOS (match perfecto):  {complete:,} ({complete/total*100:.1f}%)")
    log(f"  ✅ OK SIN DATOS:                {ok_no_data:,} ({ok_no_data/total*100:.1f}%)")
    log(f"  ❌ FALTANTES (no descargados):  {missing:,} ({missing/total*100:.1f}%)")
    log(f"  ⚠️  DÍAS FALTANTES (huecos):    {missing_days:,} ({missing_days/total*100:.1f}%)")
    log(f"  ⚠️  COMIENZA TARDE:             {late_start:,} ({late_start/total*100:.1f}%)")
    log(f"  ⚠️  TERMINA ANTES:              {early_end:,} ({early_end/total*100:.1f}%)")
    log(f"  ⚠️  DESAJUSTE:                  {mismatch:,} ({mismatch/total*100:.1f}%)")
    log(f"  ⚠️  DATOS EXTRA:                {extra_data:,} ({extra_data/total*100:.1f}%)")
    log(f"  ❌ ERRORES:                     {errors:,} ({errors/total*100:.1f}%)")
    log("")

    # Guardar reportes
    log("Guardando archivos de salida...")

    # Convertir local_years (lista) a string para CSV
    results_df_csv = results_df.with_columns(
        pl.col("local_years").list.join(",").alias("local_years")
    )

    full_report = f"{args.output_prefix}_full.csv"
    results_df_csv.write_csv(full_report)
    log(f"  ✅ {full_report}")

    problems_df = results_df_csv.filter(~pl.col("status").is_in(["COMPLETE", "OK_NO_DATA"]))
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
