#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
audit_and_repair_2004_2018.py - Auditoria completa y reparacion de descarga 2004-2018

WORKFLOW COMPLETO:
1. Cargar lista de tickers con datos confirmados en Polygon (ping_binary_2004_2018.parquet)
2. Escanear C:/TSIS_Data/trades_ticks_2004_2018_v2 para identificar tickers descargados
3. Identificar tickers faltantes (no descargados aun)
4. Auditar tickers existentes para detectar dias sin _SUCCESS
5. Generar reportes y listas para re-descarga

OUTPUT:
- audit_2004_2018_full.csv: Reporte completo de auditoria
- tickers_missing.csv: Tickers con datos pero no descargados
- tickers_incomplete.csv: Tickers descargados pero con dias sin _SUCCESS
- tickers_to_download.csv: Lista combinada para descargar/reparar
- audit_summary.txt: Resumen ejecutivo

USO:
    python scripts/utils/audit_and_repair_2004_2018.py \
        --ping processed/universe/ping_binary_2004_2018.parquet \
        --outdir C:/TSIS_Data/trades_ticks_2004_2018_v2 \
        --period-start 2004-01-01 \
        --period-end 2018-12-31 \
        --output-prefix audit_2004_2018
"""

import argparse
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
import sys
import io
from multiprocessing import Pool, cpu_count

# Force UTF-8 for stdout/stderr
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

def log(msg):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)

def parse_args():
    p = argparse.ArgumentParser(description="Auditoría completa 2004-2018")
    p.add_argument("--ping", required=True, help="Archivo ping_range_2004_2018.parquet con has_data/first_day/last_day")
    p.add_argument("--outdir", required=True, help="Directorio de datos descargados (ticks)")
    p.add_argument("--daily-root", required=True, help="Directorio con OHLCV daily (fuente de verdad)")
    p.add_argument("--period-start", required=True, help="Fecha inicio (YYYY-MM-DD)")
    p.add_argument("--period-end", required=True, help="Fecha fin (YYYY-MM-DD)")
    p.add_argument("--output-prefix", default="audit_2004_2018", help="Prefijo para archivos de salida")
    p.add_argument("--workers", type=int, default=max(4, cpu_count() // 2), help="Workers para paralelización")
    return p.parse_args()

def load_tickers_with_data(ping_file):
    """Carga lista de tickers que tienen datos confirmados en Polygon"""
    log(f"Cargando tickers con datos desde {ping_file}")

    df = pl.read_parquet(ping_file)

    # Filtrar solo tickers con datos
    # Soportar ambos formatos: ping_binary (has_history_2004_2018) y ping_range (has_data)
    has_data_col = "has_data" if "has_data" in df.columns else "has_history_2004_2018"
    tickers_with_data = df.filter(
        pl.col(has_data_col) == True
    )["ticker"].to_list()

    log(f"  > {len(tickers_with_data):,} tickers con datos confirmados en Polygon")

    return tickers_with_data, df

def scan_downloaded_tickers(outdir):
    """Escanea directorio de descarga para identificar tickers ya descargados"""
    log(f"Escaneando tickers descargados en {outdir}")

    # Normalize path for Windows
    outdir_normalized = outdir.replace("\\", "/")
    outdir_path = Path(outdir_normalized)

    if not outdir_path.exists():
        log(f"  WARNING: Directorio no existe: {outdir}")
        log(f"  Path normalizado: {outdir_normalized}")
        log(f"  Path absoluto: {outdir_path.absolute()}")
        return []

    downloaded = []
    for item in outdir_path.iterdir():
        if item.is_dir() and not item.name.startswith("_"):
            downloaded.append(item.name)

    log(f"  > {len(downloaded):,} tickers encontrados en disco")

    return sorted(downloaded)

def get_expected_days_from_daily(ticker: str, daily_root: Path, start_date: str, end_date: str) -> list:
    """
    Obtiene lista de días esperados desde OHLCV daily (fuente de verdad Polygon).

    Si el ticker tiene barras daily en Polygon, esos son los días que DEBEN tener ticks.
    Esto es más preciso que un calendario genérico porque:
    - Respeta IPO date (no espera ticks antes del listado)
    - Respeta delisting date (no espera ticks después de delist)
    - Usa el calendario real de trading que Polygon conoce

    Args:
        ticker: Símbolo del ticker
        daily_root: Ruta al directorio de daily OHLCV (ej: raw/polygon/ohlcv_daily)
        start_date: Fecha inicio (YYYY-MM-DD)
        end_date: Fecha fin (YYYY-MM-DD)

    Returns:
        Lista de fechas (str YYYY-MM-DD) donde se esperan ticks
    """
    ticker_dir = daily_root / ticker

    if not ticker_dir.exists():
        # Si no hay daily, no esperamos ticks (el ticker no tiene datos en Polygon)
        return []

    # Leer todos los daily.parquet del ticker
    dfs = []
    for year_dir in ticker_dir.glob("year=*"):
        daily_file = year_dir / "daily.parquet"
        if daily_file.exists():
            try:
                df = pl.read_parquet(daily_file, columns=["date"])
                dfs.append(df)
            except Exception:
                continue

    if not dfs:
        return []

    # Concatenar y filtrar por rango
    df = pl.concat(dfs, how="vertical_relaxed")
    df = df.filter(
        (pl.col("date") >= start_date) &
        (pl.col("date") <= end_date)
    )

    return sorted(df["date"].unique().to_list())

def audit_single_ticker(args_tuple):
    """Audita un ticker individual (para paralelización)"""
    ticker, outdir, daily_root, ticker_first_day, ticker_last_day = args_tuple

    ticker_dir = Path(outdir) / ticker

    # FUENTE DE VERDAD: usar OHLCV daily de Polygon
    # Solo esperamos ticks en días donde Polygon tiene barra daily
    trading_days_for_ticker = get_expected_days_from_daily(
        ticker, daily_root, ticker_first_day, ticker_last_day
    )

    # Si no hay daily para este rango, no podemos auditar confiablemente
    if len(trading_days_for_ticker) == 0:
        return {
            "ticker": ticker,
            "status": "NO_DAILY_REFERENCE",
            "days_expected": 0,
            "days_with_success": 0,
            "days_with_parquet_no_success": 0,
            "days_missing": 0,
            "first_day_expected": ticker_first_day,
            "first_day_found": None,
            "last_day_found": None,
            "issue_summary": "Sin OHLCV daily 2004-2018 para auditar"
        }

    # Caso 1: Ticker sin directorio
    if not ticker_dir.exists():
        return {
            "ticker": ticker,
            "status": "MISSING",
            "days_expected": len(trading_days_for_ticker),
            "days_with_success": 0,
            "days_with_parquet_no_success": 0,
            "days_missing": len(trading_days_for_ticker),
            "first_day_expected": ticker_first_day,
            "first_day_found": None,
            "last_day_found": None,
            "issue_summary": "TICKER_DIR_MISSING"
        }

    # Escanear estructura de días
    days_with_success = 0
    days_with_parquet_no_success = 0
    days_missing = 0
    first_day_found = None
    last_day_found = None

    for day_str in trading_days_for_ticker:
        year = day_str.split("-")[0]
        month = day_str.split("-")[1]

        day_dir = ticker_dir / f"year={year}" / f"month={month}" / f"day={day_str}"
        success_marker = day_dir / "_SUCCESS"
        premarket_fp = day_dir / "premarket.parquet"
        market_fp = day_dir / "market.parquet"

        # Caso: Día completo con _SUCCESS
        if success_marker.exists():
            days_with_success += 1
            if first_day_found is None:
                first_day_found = day_str
            last_day_found = day_str

        # Caso: Día con parquet pero sin _SUCCESS (problema!)
        elif (premarket_fp.exists() or market_fp.exists()):
            days_with_parquet_no_success += 1
            if first_day_found is None:
                first_day_found = day_str
            last_day_found = day_str

        # Caso: Día faltante
        else:
            days_missing += 1

    # Determinar status
    if days_with_success == len(trading_days_for_ticker):
        status = "COMPLETE"
        issue = None
    elif days_with_parquet_no_success > 0:
        status = "INCOMPLETE_NO_SUCCESS"
        issue = f"{days_with_parquet_no_success} dias sin _SUCCESS"
    elif days_with_success > 0:
        status = "INCOMPLETE_PARTIAL"
        issue = f"{days_missing} dias faltantes"
    else:
        status = "EMPTY"
        issue = "Directorio vacio"

    return {
        "ticker": ticker,
        "status": status,
        "days_expected": len(trading_days_for_ticker),
        "days_with_success": days_with_success,
        "days_with_parquet_no_success": days_with_parquet_no_success,
        "days_missing": days_missing,
        "first_day_expected": ticker_first_day,
        "first_day_found": first_day_found,
        "last_day_found": last_day_found,
        "issue_summary": issue
    }

def process_missing_ticker(args_tuple):
    """Procesa un ticker missing para calcular days_expected (para paralelización)"""
    ticker, daily_root, ticker_first_day, ticker_last_day = args_tuple

    trading_days = get_expected_days_from_daily(
        ticker, daily_root, ticker_first_day, ticker_last_day
    )

    return {
        "ticker": ticker,
        "status": "MISSING",
        "days_expected": len(trading_days),
        "days_with_success": 0,
        "days_with_parquet_no_success": 0,
        "days_missing": len(trading_days),
        "first_day_expected": ticker_first_day,
        "first_day_found": None,
        "last_day_found": None,
        "issue_summary": "TICKER_DIR_MISSING",
    }

def main():
    args = parse_args()

    log("=" * 80)
    log("AUDITORÍA COMPLETA 2004-2018")
    log("=" * 80)

    # Paso 1: Cargar tickers con datos confirmados
    tickers_with_data, ping_df = load_tickers_with_data(args.ping)

    # Paso 2: Escanear tickers descargados
    downloaded_tickers = scan_downloaded_tickers(args.outdir)

    # Paso 3: Identificar tickers faltantes
    tickers_with_data_set = set(tickers_with_data)
    downloaded_set = set(downloaded_tickers)

    missing_tickers = sorted(tickers_with_data_set - downloaded_set)

    log("")
    log("=" * 80)
    log("RESUMEN INICIAL")
    log("=" * 80)
    log(f"Tickers con datos (Polygon):     {len(tickers_with_data):,}")
    log(f"Tickers descargados:             {len(downloaded_tickers):,}")
    log(f"Tickers faltantes (no descargados): {len(missing_tickers):,}")
    log("")

    # Paso 4: Crear diccionario de first_day y last_day por ticker
    log("Preparando datos de first_day y last_day por ticker...")
    ticker_first_day_map = {}
    ticker_last_day_map = {}
    # Soportar ambos formatos: ping_binary (has_history_2004_2018) y ping_range (has_data)
    has_data_col = "has_data" if "has_data" in ping_df.columns else "has_history_2004_2018"
    for row in ping_df.filter(pl.col(has_data_col) == True).iter_rows(named=True):
        ticker_first_day_map[row["ticker"]] = row["first_day"]
        ticker_last_day_map[row["ticker"]] = row["last_day"]

    log(f"  > {len(ticker_first_day_map):,} tickers con first_day/last_day mapeados")
    log("")

    # Paso 5: Auditar tickers descargados (paralelizado)
    log("Auditando tickers descargados...")
    log(f"  > Usando {args.workers} workers")

    # Solo auditar tickers que están en la lista de con-datos
    tickers_to_audit = sorted(downloaded_set & tickers_with_data_set)

    # Preparar argumentos con first_day y last_day específico por ticker (del ping)
    # Convertir daily_root a Path
    daily_root = Path(args.daily_root)
    audit_args = [
        (ticker, args.outdir, daily_root, ticker_first_day_map[ticker], ticker_last_day_map[ticker])
        for ticker in tickers_to_audit
    ]

    with Pool(processes=args.workers) as pool:
        results = []
        total = len(audit_args)

        for i, result in enumerate(pool.imap_unordered(audit_single_ticker, audit_args), 1):
            results.append(result)
            if i % 100 == 0 or i == total:
                log(f"  Progreso: {i}/{total} ({i/total*100:.1f}%)")

    # Paso 6: Compilar resultados
    log("")
    log("Compilando resultados...")

    # Crear DataFrame con resultados de auditoría
    audit_df = pl.DataFrame(results)

    # Paso 6: Procesar tickers faltantes (paralelizado)
    log(f"Procesando {len(missing_tickers):,} tickers faltantes...")

    # Preparar argumentos para paralelización
    missing_args = [
        (ticker, daily_root, ticker_first_day_map[ticker], ticker_last_day_map[ticker])
        for ticker in missing_tickers
    ]

    # Procesar en paralelo
    with Pool(processes=args.workers) as pool:
        missing_data = []
        total_missing = len(missing_args)

        for i, result in enumerate(pool.imap_unordered(process_missing_ticker, missing_args), 1):
            missing_data.append(result)
            if i % 100 == 0 or i == total_missing:
                log(f"  Progreso missing: {i}/{total_missing} ({i/total_missing*100:.1f}%)")

    missing_df = pl.DataFrame(missing_data)
    log(f"  > {len(missing_data):,} tickers missing procesados")

    # Combinar resultados - asegurar schemas compatibles
    # Convertir columnas problemáticas a String explícitamente
    if audit_df.height > 0:
        for col in ["first_day_found", "last_day_found", "issue_summary"]:
            if col in audit_df.columns:
                audit_df = audit_df.with_columns(pl.col(col).cast(pl.Utf8, strict=False))

    if missing_df.height > 0:
        for col in ["first_day_found", "last_day_found", "issue_summary"]:
            if col in missing_df.columns:
                missing_df = missing_df.with_columns(pl.col(col).cast(pl.Utf8, strict=False))

    full_audit_df = pl.concat([audit_df, missing_df], how="diagonal")

    # Ordenar por ticker
    full_audit_df = full_audit_df.sort("ticker")

    # Paso 7: Generar estadísticas
    log("")
    log("=" * 80)
    log("RESULTADOS DE AUDITORÍA")
    log("=" * 80)

    status_counts = full_audit_df.group_by("status").agg(pl.len()).sort("status")

    total_tickers = len(full_audit_df)
    complete = len(full_audit_df.filter(pl.col("status") == "COMPLETE"))
    incomplete_no_success = len(full_audit_df.filter(pl.col("status") == "INCOMPLETE_NO_SUCCESS"))
    incomplete_partial = len(full_audit_df.filter(pl.col("status") == "INCOMPLETE_PARTIAL"))
    empty = len(full_audit_df.filter(pl.col("status") == "EMPTY"))
    missing = len(full_audit_df.filter(pl.col("status") == "MISSING"))
    no_daily_ref = len(full_audit_df.filter(pl.col("status") == "NO_DAILY_REFERENCE"))

    log(f"Total tickers con datos:         {total_tickers:,}")
    log(f"  ✅ COMPLETOS:                   {complete:,} ({complete/total_tickers*100:.1f}%)")
    log(f"  ⚠️  INCOMPLETOS (sin _SUCCESS): {incomplete_no_success:,} ({incomplete_no_success/total_tickers*100:.1f}%)")
    log(f"  ⚠️  INCOMPLETOS (días faltantes): {incomplete_partial:,} ({incomplete_partial/total_tickers*100:.1f}%)")
    log(f"  ❌ VACÍOS (dir sin datos):      {empty:,} ({empty/total_tickers*100:.1f}%)")
    log(f"  ❌ FALTANTES (no descargados):  {missing:,} ({missing/total_tickers*100:.1f}%)")
    log(f"  ⚠️  SIN DAILY 2004-2018:         {no_daily_ref:,} ({no_daily_ref/total_tickers*100:.1f}%)")
    log("")

    # Estadísticas de días (ahora suma days_expected por ticker, no asume todos iguales)
    total_days_with_success = full_audit_df["days_with_success"].sum()
    total_days_no_success = full_audit_df["days_with_parquet_no_success"].sum()
    total_days_missing = full_audit_df["days_missing"].sum()
    total_days_expected = full_audit_df["days_expected"].sum()

    log("Estadísticas de días:")
    log(f"  Total días esperados:            {total_days_expected:,}")

    if total_days_expected > 0:
        log(f"  ✅ Días con _SUCCESS:            {total_days_with_success:,} ({total_days_with_success/total_days_expected*100:.1f}%)")
        log(f"  ⚠️  Días sin _SUCCESS:           {total_days_no_success:,} ({total_days_no_success/total_days_expected*100:.1f}%)")
        log(f"  ❌ Días faltantes:               {total_days_missing:,} ({total_days_missing/total_days_expected*100:.1f}%)")
    else:
        log(f"  ✅ Días con _SUCCESS:            {total_days_with_success:,}")
        log(f"  ⚠️  Días sin _SUCCESS:           {total_days_no_success:,}")
        log(f"  ❌ Días faltantes:               {total_days_missing:,}")
        log("  ⚠️  NOTA: Los tickers missing no tienen OHLCV daily para calcular días esperados")
    log("")

    # Paso 8: Guardar archivos de salida
    log("Guardando archivos de salida...")

    # 1. Reporte completo
    full_report_file = f"{args.output_prefix}_full.csv"
    full_audit_df.write_csv(full_report_file)
    log(f"  ✅ {full_report_file}")

    # 2. Tickers faltantes (no descargados)
    missing_tickers_file = f"{args.output_prefix}_missing.csv"
    pl.DataFrame({"ticker": missing_tickers}).write_csv(missing_tickers_file)
    log(f"  ✅ {missing_tickers_file} ({len(missing_tickers):,} tickers)")

    # 3. Tickers incompletos (sin _SUCCESS)
    incomplete_no_success_tickers = full_audit_df.filter(
        pl.col("status") == "INCOMPLETE_NO_SUCCESS"
    )["ticker"].to_list()

    incomplete_file = f"{args.output_prefix}_incomplete.csv"
    pl.DataFrame({"ticker": incomplete_no_success_tickers}).write_csv(incomplete_file)
    log(f"  ✅ {incomplete_file} ({len(incomplete_no_success_tickers):,} tickers)")

    # 4. Tickers incompletos parciales
    incomplete_partial_tickers = full_audit_df.filter(
        pl.col("status") == "INCOMPLETE_PARTIAL"
    )["ticker"].to_list()

    incomplete_partial_file = f"{args.output_prefix}_incomplete_partial.csv"
    pl.DataFrame({"ticker": incomplete_partial_tickers}).write_csv(incomplete_partial_file)
    log(f"  ✅ {incomplete_partial_file} ({len(incomplete_partial_tickers):,} tickers)")

    # 5. Tickers vacíos
    empty_tickers = full_audit_df.filter(
        pl.col("status") == "EMPTY"
    )["ticker"].to_list()

    empty_file = f"{args.output_prefix}_empty.csv"
    pl.DataFrame({"ticker": empty_tickers}).write_csv(empty_file)
    log(f"  ✅ {empty_file} ({len(empty_tickers):,} tickers)")

    # 6. Lista combinada para descargar/reparar
    tickers_to_download = sorted(set(missing_tickers + incomplete_no_success_tickers + incomplete_partial_tickers + empty_tickers))

    download_file = f"{args.output_prefix}_to_download.csv"
    pl.DataFrame({"ticker": tickers_to_download}).write_csv(download_file)
    log(f"  ✅ {download_file} ({len(tickers_to_download):,} tickers)")

    # 7. Resumen ejecutivo
    summary_file = f"{args.output_prefix}_summary.txt"
    with open(summary_file, "w", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("AUDITORÍA COMPLETA 2004-2018 - RESUMEN EJECUTIVO\n")
        f.write("=" * 80 + "\n")
        f.write(f"Fecha: {datetime.now():%Y-%m-%d %H:%M:%S}\n")
        f.write(f"Periodo: {args.period_start} -> {args.period_end}\n")
        f.write(f"NOTA: Dias esperados calculados por ticker segun first_day\n")
        f.write("\n")
        f.write("TICKERS:\n")
        f.write(f"  Total con datos (Polygon):       {total_tickers:,}\n")
        f.write(f"  ✅ Completos:                     {complete:,} ({complete/total_tickers*100:.1f}%)\n")
        f.write(f"  ⚠️  Incompletos (sin _SUCCESS):   {incomplete_no_success:,} ({incomplete_no_success/total_tickers*100:.1f}%)\n")
        f.write(f"  ⚠️  Incompletos (días faltantes):  {incomplete_partial:,} ({incomplete_partial/total_tickers*100:.1f}%)\n")
        f.write(f"  ❌ Vacíos:                        {empty:,} ({empty/total_tickers*100:.1f}%)\n")
        f.write(f"  ❌ Faltantes:                     {missing:,} ({missing/total_tickers*100:.1f}%)\n")
        f.write("\n")
        f.write("DÍAS:\n")
        f.write(f"  Total días esperados:            {total_days_expected:,}\n")
        if total_days_expected > 0:
            f.write(f"  ✅ Días con _SUCCESS:            {total_days_with_success:,} ({total_days_with_success/total_days_expected*100:.1f}%)\n")
            f.write(f"  ⚠️  Días sin _SUCCESS:           {total_days_no_success:,} ({total_days_no_success/total_days_expected*100:.1f}%)\n")
            f.write(f"  ❌ Días faltantes:               {total_days_missing:,} ({total_days_missing/total_days_expected*100:.1f}%)\n")
        else:
            f.write(f"  ✅ Días con _SUCCESS:            {total_days_with_success:,}\n")
            f.write(f"  ⚠️  Días sin _SUCCESS:           {total_days_no_success:,}\n")
            f.write(f"  ❌ Días faltantes:               {total_days_missing:,}\n")
            f.write("  ⚠️  NOTA: Tickers missing sin OHLCV daily\n")
        f.write("\n")
        f.write("ACCIÓN REQUERIDA:\n")
        f.write(f"  Tickers a descargar/reparar:     {len(tickers_to_download):,}\n")
        f.write(f"  Ver archivo: {download_file}\n")
        f.write("\n")
        f.write("COMANDO PARA REPARACIÓN:\n")
        f.write("```bash\n")
        f.write(f"python scripts/01_agregation_OHLCV/batch_trades_wrapper.py \\\n")
        f.write(f"    --tickers-csv {download_file} \\\n")
        f.write(f"    --outdir {args.outdir} \\\n")
        f.write(f"    --from {args.period_start} \\\n")
        f.write(f"    --to {args.period_end} \\\n")
        f.write(f"    --batch-size 100 \\\n")
        f.write(f"    --max-concurrent 50 \\\n")
        f.write(f"    --rate-limit 0.05 \\\n")
        f.write(f"    --ingest-script scripts/01_agregation_OHLCV/ingest_trades_ticks.py\n")
        f.write("```\n")
        f.write("\n")
        f.write("=" * 80 + "\n")

    log(f"  ✅ {summary_file}")
    log("")
    log("=" * 80)
    log("AUDITORÍA COMPLETADA")
    log("=" * 80)
    log(f"Archivos generados:")
    log(f"  - {full_report_file}")
    log(f"  - {missing_tickers_file}")
    log(f"  - {incomplete_file}")
    log(f"  - {incomplete_partial_file}")
    log(f"  - {empty_file}")
    log(f"  - {download_file} ⚡ USAR ESTE PARA DESCARGA")
    log(f"  - {summary_file}")
    log("")

    # Mostrar resumen de summary
    with open(summary_file, "r", encoding="utf-8") as f:
        print(f.read())

if __name__ == "__main__":
    main()
