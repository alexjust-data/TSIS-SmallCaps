#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ping paralelo ultrarrápido para detectar si un ticker tiene datos 2007–2018.

Objetivo:
- Procesar 6000 tickers en ~2–3 minutos
- Sin descargar trades
- Sin loops por días reales
- Llamada mínima: 1 día representativo por año

Requisitos:
    pip install polars requests
"""

import argparse
import polars as pl
import requests
from datetime import date
from functools import partial
from multiprocessing import Pool, cpu_count
import os
from dotenv import load_dotenv
import sys

# Años a testear
YEARS = list(range(2007, 2019))
TEST_MONTH = 6
TEST_DAY = 15


def init_worker(api_key):
    """Inicializa sesión global por worker."""
    global SESSION
    global API_KEY

    SESSION = requests.Session()
    SESSION.headers.update({"Connection": "keep-alive"})
    API_KEY = api_key


def ping_day(ticker, day):
    """Ping mínimo a un día: retorna True si hay datos."""
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{day}/{day}"
    params = {"apiKey": API_KEY}

    try:
        r = SESSION.get(url, params=params, timeout=2)
        if r.status_code != 200:
            return False
        js = r.json()
        return js.get("resultsCount", 0) > 0
    except Exception:
        return False


def check_ticker(ticker):
    """Evalúa un ticker en paralelo. Retorna (ticker, True/False)."""
    for y in YEARS:
        d = date(y, TEST_MONTH, TEST_DAY)
        if ping_day(ticker, d):
            return ticker, True
    return ticker, False


def main():
    # Cargar variables de entorno
    load_dotenv()
    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        sys.exit("ERROR: POLYGON_API_KEY no encontrada en .env")

    p = argparse.ArgumentParser()
    p.add_argument("--tickers", required=True, help="CSV o Parquet con columna 'ticker'")
    p.add_argument("--out", required=True)
    p.add_argument("--workers", type=int, default=max(4, cpu_count() // 2),
                   help="Número de procesos paralelos (default: CPU/2)")
    args = p.parse_args()

    # Cargar tickers
    if args.tickers.endswith(".csv"):
        df = pl.read_csv(args.tickers)
    else:
        df = pl.read_parquet(args.tickers)

    if "ticker" not in df.columns:
        raise ValueError("El archivo debe tener columna 'ticker'.")

    tickers = df["ticker"].unique().to_list()
    total = len(tickers)
    print(f"Tickers cargados: {total:,}")
    print(f"Workers: {args.workers}")

    # Paralelización
    with Pool(
        processes=args.workers,
        initializer=init_worker,
        initargs=(api_key,)
    ) as pool:

        results_iter = pool.imap_unordered(check_ticker, tickers)
        results = []
        processed = 0

        for res in results_iter:
            results.append(res)
            processed += 1

            if processed % 200 == 0 or processed == total:
                pct = processed / total * 100
                print(f"[{processed}/{total}] ({pct:.1f}%)", flush=True)

    # Guardar salida
    out_df = pl.DataFrame({
        "ticker": [t for t, ok in results],
        "has_history_2007_2018": [ok for t, ok in results]
    })

    if args.out.endswith(".csv"):
        out_df.write_csv(args.out)
    else:
        out_df.write_parquet(args.out)

    print(f"\nGuardado: {args.out}")
    print(out_df.head())


if __name__ == "__main__":
    main()
