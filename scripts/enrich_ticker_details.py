# scripts/enrich_ticker_details.py
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
enrich_ticker_details.py

Descarga ticker details desde Polygon API para enriquecer universo CS+XNAS/XNYS.
Descarga datos corporativos (market_cap, employees, description) para activos e inactivos.

NOTA: Polygon API solo retorna datos completos para tickers ACTIVOS.
      Los tickers INACTIVOS retornarán error "not_found".

Uso:
  python scripts/enrich_ticker_details.py \
      --input processed/universe/tickers_2019_2025_cs_exchanges.parquet \
      --output raw/polygon/reference/ticker_details \
      --as-of-date 2025-11-01 \
      --max-workers 16
"""

import os
import sys
import time
import argparse
import datetime as dt
from pathlib import Path
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import polars as pl
from dotenv import load_dotenv

# Configuración
BASE_URL = "https://api.polygon.io"
TIMEOUT = 20
MAX_WORKERS = 16
RETRY_MAX = 6
RETRY_BACKOFF = 1.5

def log(msg: str):
    """Log con timestamp"""
    print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)

def http_get(api_key: str, url: str) -> Dict[str, Any]:
    """
    GET request con reintentos y manejo de rate limiting

    Returns:
        dict: Response JSON, o dict con campo 'error' si falla
    """
    headers = {"Authorization": f"Bearer {api_key}"}

    for attempt in range(1, RETRY_MAX + 1):
        try:
            r = requests.get(url, timeout=TIMEOUT, headers=headers)

            # Rate limiting (429)
            if r.status_code == 429:
                retry_after = int(r.headers.get("Retry-After", "2"))
                log(f"429 Too Many Requests. Sleeping {retry_after}s...")
                time.sleep(retry_after)
                continue

            # Ticker no encontrado (404) - común para inactivos
            if r.status_code == 404:
                return {"error": "not_found", "status": "NOT_FOUND"}

            # Otros errores HTTP
            r.raise_for_status()

            # Success
            data = r.json()
            return data.get("results", {})

        except requests.exceptions.Timeout:
            log(f"Timeout en intento {attempt}/{RETRY_MAX}")
            time.sleep(RETRY_BACKOFF * attempt)

        except Exception as e:
            log(f"Error en intento {attempt}/{RETRY_MAX}: {e}")
            time.sleep(RETRY_BACKOFF * attempt)

    # Si agota reintentos
    return {"error": "max_retries_exceeded"}

def fetch_ticker_details(api_key: str, ticker: str, as_of_date: str) -> Dict[str, Any]:
    """
    Descarga ticker details para un ticker específico

    Args:
        api_key: Polygon API key
        ticker: Símbolo del ticker
        as_of_date: Fecha de snapshot (YYYY-MM-DD)

    Returns:
        dict: Ticker details o dict con campo 'error'
    """
    url = f"{BASE_URL}/v3/reference/tickers/{ticker}"
    details = http_get(api_key, url)

    # Añadir metadatos
    details["ticker"] = ticker
    details["fetch_date"] = as_of_date

    return details

def main():
    ap = argparse.ArgumentParser(description="Enriquecer tickers con datos corporativos")
    ap.add_argument("--input", type=str, required=True,
                    help="Input parquet (tickers_2019_2025_cs_exchanges.parquet)")
    ap.add_argument("--output", type=str, required=True,
                    help="Output dir (raw/polygon/reference/ticker_details)")
    ap.add_argument("--as-of-date", type=str, default=dt.date.today().isoformat(),
                    help="Fecha de snapshot (default: hoy)")
    ap.add_argument("--max-workers", type=int, default=MAX_WORKERS,
                    help="Workers paralelos (default: 16)")
    ap.add_argument("--filter-active", type=str, choices=["true", "false", "both"], default="both",
                    help="Filtrar por activos (true), inactivos (false) o ambos (both)")
    args = ap.parse_args()

    # Cargar .env
    load_dotenv()
    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        log("ERROR: POLYGON_API_KEY no encontrada en .env")
        sys.exit(1)

    # Validar input
    input_path = Path(args.input)
    if not input_path.exists():
        log(f"ERROR: No existe {input_path}")
        sys.exit(1)

    # Cargar universo
    log(f"Cargando {input_path}...")
    df = pl.read_parquet(input_path)
    log(f"   Total tickers: {len(df):,}")

    # Filtrar por active si se especifica
    if args.filter_active == "true":
        df = df.filter(pl.col("active") == True)
        log(f"   Filtrado solo activos: {len(df):,}")
    elif args.filter_active == "false":
        df = df.filter(pl.col("active") == False)
        log(f"   Filtrado solo inactivos: {len(df):,}")
    else:
        n_active = len(df.filter(pl.col("active") == True))
        n_inactive = len(df.filter(pl.col("active") == False))
        log(f"   Activos: {n_active:,}, Inactivos: {n_inactive:,}")

    # Extraer lista de tickers
    tickers = df["ticker"].drop_nulls().unique().to_list()
    log(f"\nTickers a enriquecer: {len(tickers):,}")

    # Crear directorio de salida
    output_dir = Path(args.output) / f"as_of_date={args.as_of_date}"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Descarga paralela
    log(f"\nIniciando descarga paralela ({args.max_workers} workers)...")
    log(f"NOTA: Los tickers inactivos retornarán error 'not_found' (esperado)")

    rows = []
    errors = 0
    not_found = 0

    t0 = time.time()

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        # Submit all tasks
        futures = {
            executor.submit(fetch_ticker_details, api_key, ticker, args.as_of_date): ticker
            for ticker in tickers
        }

        # Process as completed
        for i, future in enumerate(as_completed(futures), 1):
            ticker = futures[future]
            try:
                details = future.result()
                rows.append(details)

                # Contar errores
                if "error" in details:
                    if details["error"] == "not_found":
                        not_found += 1
                    else:
                        errors += 1

                # Log progreso cada 500 tickers
                if i % 500 == 0:
                    elapsed = time.time() - t0
                    rate = i / elapsed
                    eta = (len(tickers) - i) / rate
                    log(f"Progreso: {i:,}/{len(tickers):,} ({i/len(tickers)*100:.1f}%) "
                        f"- Rate: {rate:.1f} tickers/s - ETA: {eta/60:.1f}min")

            except Exception as e:
                log(f"ERROR procesando {ticker}: {e}")
                rows.append({
                    "ticker": ticker,
                    "fetch_date": args.as_of_date,
                    "error": str(e)
                })
                errors += 1

    t1 = time.time()
    elapsed = t1 - t0

    log(f"\nDescarga completada en {elapsed/60:.1f} minutos")
    log(f"   Total procesados: {len(rows):,}")
    log(f"   Not found (404): {not_found:,} (esperado para inactivos)")
    log(f"   Otros errores: {errors - not_found:,}")
    log(f"   Con datos: {len(rows) - not_found - (errors - not_found):,}")

    # Convertir a DataFrame y guardar
    log(f"\nGuardando resultados...")
    df_details = pl.from_dicts(rows)

    output_parquet = output_dir / "details.parquet"
    df_details.write_parquet(output_parquet)
    log(f"   Parquet: {output_parquet}")

    # Guardar también CSV para inspección manual (si es posible)
    try:
        output_csv = output_dir / "details.csv"
        df_details.write_csv(output_csv)
        log(f"   CSV: {output_csv}")
    except Exception as e:
        log(f"   WARNING: No se pudo exportar CSV (datos anidados): {e}")

    # Estadísticas de completitud
    log(f"\nEstadisticas de completitud:")

    # Contar tickers con/sin error
    df_success = df_details.filter(~pl.col("ticker").is_null() & pl.col("error").is_null())
    df_not_found = df_details.filter(pl.col("error") == "not_found")
    df_errors = df_details.filter(
        (~pl.col("error").is_null()) &
        (pl.col("error") != "not_found")
    )

    log(f"   Con datos completos: {len(df_success):,} ({len(df_success)/len(df_details)*100:.1f}%)")
    log(f"   Not found (404): {len(df_not_found):,} ({len(df_not_found)/len(df_details)*100:.1f}%)")
    log(f"   Otros errores: {len(df_errors):,} ({len(df_errors)/len(df_details)*100:.1f}%)")

    # Completitud de campos clave (solo para success)
    if len(df_success) > 0:
        log(f"\nCompletitud de campos clave (solo tickers con datos):")
        key_fields = ["market_cap", "weighted_shares_outstanding", "total_employees",
                      "description", "sic_code", "homepage_url"]

        for field in key_fields:
            if field in df_success.columns:
                non_null = len(df_success.filter(~pl.col(field).is_null()))
                pct = non_null / len(df_success) * 100
                log(f"   {field:30s}: {non_null:,}/{len(df_success):,} ({pct:.1f}%)")

    log(f"\nHecho!")

if __name__ == "__main__":
    main()
