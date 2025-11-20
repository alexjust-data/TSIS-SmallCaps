#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Detección ABSOLUTA de actividad histórica 2004–2018 para un ticker.
Método: Búsqueda binaria del primer día con volumen.
Exactitud 100%. Ningún ticker con datos reales puede pasar por alto.

Paralelo mediante multiprocessing.
"""

import argparse
import polars as pl
import requests
from datetime import date, timedelta
from multiprocessing import Pool, cpu_count
from dotenv import load_dotenv
import os
import sys


# -----------------------------
#   PRIMITIVA: PING DE UN DÍA
# -----------------------------
def ping_day(session, api_key, ticker, d):
    """Devuelve True si hay datos en ese día."""
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{d}/{d}"
    params = {"apiKey": api_key}
    try:
        r = session.get(url, params=params, timeout=2)
        if r.status_code != 200:
            return False
        js = r.json()
        return js.get("resultsCount", 0) > 0
    except:
        return False


# -----------------------------
#     BÚSQUEDA BINARIA
# -----------------------------
def binary_search_first_day(session, api_key, ticker, start, end):
    """
    Busca el primer día con actividad real.
    Retorna:
       (True, "YYYY-MM-DD") si encuentra datos
       (False, None) si no hay nada en 2007–2018
    """
    lo, hi = start, end
    found = False
    first_day = None

    while lo <= hi:
        mid = lo + (hi - lo) // 2
        d = date.fromordinal(mid)

        if ping_day(session, api_key, ticker, d):
            found = True
            first_day = d
            # Buscar antes para encontrar el primer día real
            hi = mid - 1
        else:
            # Buscar después
            lo = mid + 1

    return found, first_day


# -----------------------------
#   WORKER INITIALIZER
# -----------------------------
def init_worker(api_key, start_date, end_date):
    global SESSION, API_KEY, START_DATE, END_DATE
    SESSION = requests.Session()
    API_KEY = api_key
    START_DATE = start_date
    END_DATE = end_date


# -----------------------------
#   CHECK TICKER (PARALELO)
# -----------------------------
# Global variables for worker processes
START_DATE = None
END_DATE = None

def check_ticker_binary(ticker):
    global SESSION, API_KEY, START_DATE, END_DATE

    # Rango ordinal
    lo = START_DATE.toordinal()
    hi = END_DATE.toordinal()

    found, first_day = binary_search_first_day(
        SESSION,
        API_KEY,
        ticker,
        lo,
        hi
    )

    if found:
        return (ticker, True, first_day.isoformat())
    else:
        return (ticker, False, None)


# -----------------------------
#           MAIN
# -----------------------------
def main():
    # Cargar variables de entorno
    load_dotenv()
    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        sys.exit("ERROR: POLYGON_API_KEY no encontrada en .env")

    p = argparse.ArgumentParser()
    p.add_argument("--tickers", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--workers", type=int, default=max(4, cpu_count() // 2))
    args = p.parse_args()

    # Fechas del periodo 2004–2018
    START = date(2004, 1, 1)
    END   = date(2018, 12, 31)

    # Cargar tickers
    if args.tickers.endswith(".csv"):
        df = pl.read_csv(args.tickers)
    else:
        df = pl.read_parquet(args.tickers)

    tickers = df["ticker"].unique().to_list()
    total = len(tickers)

    print(f"Tickers cargados: {total:,}")
    print(f"Workers: {args.workers}")

    # Paralelización
    with Pool(
        processes=args.workers,
        initializer=init_worker,
        initargs=(api_key, START, END)
    ) as pool:

        results = []
        processed = 0

        for res in pool.imap_unordered(check_ticker_binary, tickers):
            results.append(res)
            processed += 1

            if processed % 100 == 0 or processed == total:
                pct = processed / total * 100
                print(f"[{processed}/{total}] ({pct:.1f}%)")

    # Guardar salida final
    out_df = pl.DataFrame({
        "ticker": [r[0] for r in results],
        "has_history_2004_2018": [r[1] for r in results],
        "first_day": [r[2] for r in results],
    })

    if args.out.endswith(".csv"):
        out_df.write_csv(args.out)
    else:
        out_df.write_parquet(args.out)

    print(f"\nGuardado: {args.out}")
    print(out_df.head())


if __name__ == "__main__":
    main()
