#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ping Polygon para encontrar el PRIMER y el ÚLTIMO día con datos
para cada ticker, dentro de un rango [start_date, end_date].

- Usa /v2/aggs/ticker/{ticker}/range/1/day/... con:
    sort=asc,  limit=1  -> primer día
    sort=desc, limit=1  -> último día

- Es MUCHO más barato que la búsqueda binaria (2 requests por ticker).

OUTPUT (Parquet):
    ticker, has_data, first_day, last_day
"""

import argparse
import os
import sys
from datetime import datetime
from multiprocessing import Pool, cpu_count

import polars as pl
import requests
from dotenv import load_dotenv


# ----------------------
#   LOG HELPER
# ----------------------
def log(msg: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}", flush=True)


# ----------------------
#   WORKER GLOBALS
# ----------------------
SESSION = None
API_KEY = None
START_DATE_STR = None
END_DATE_STR = None


def init_worker(api_key: str, start_date: str, end_date: str) -> None:
    """
    Inicializador de cada worker del Pool.
    Crea una sesión HTTP y guarda API key y fechas en globals.
    """
    global SESSION, API_KEY, START_DATE_STR, END_DATE_STR
    SESSION = requests.Session()
    API_KEY = api_key
    START_DATE_STR = start_date
    END_DATE_STR = end_date


# ----------------------
#   LÓGICA POR TICKER
# ----------------------
def _fetch_edge_day(ticker: str, sort: str, max_retries: int = 5):
    """
    Devuelve el primer (sort='asc') o último (sort='desc') día con datos
    para un ticker, o None si no hay datos en el rango.
    """
    global SESSION, API_KEY, START_DATE_STR, END_DATE_STR

    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{ticker}"
        f"/range/1/day/{START_DATE_STR}/{END_DATE_STR}"
    )

    params = {
        "adjusted": "true",
        "sort": sort,   # "asc" -> earliest, "desc" -> latest
        "limit": 1,
        "apiKey": API_KEY,
    }

    for attempt in range(1, max_retries + 1):
        try:
            r = SESSION.get(url, params=params, timeout=10)
            if r.status_code != 200:
                # 429 u otros: reintentar un poco, pero sin bloquear
                if r.status_code in (429, 500, 502, 503, 504):
                    continue
                # Errores duros -> salir
                return None

            js = r.json()
            results = js.get("results")
            if not results:
                return None

            # Polygon devuelve timestamp ms en "t"
            ts_ms = results[0].get("t")
            if ts_ms is None:
                return None

            dt = datetime.utcfromtimestamp(ts_ms / 1000.0)
            return dt.date().isoformat()

        except Exception:
            # Cualquier error de red/timeout -> reintentar un poco
            continue

    # Si agotamos reintentos sin éxito, lo consideramos sin datos
    return None


def check_ticker_range(ticker: str):
    """
    Worker principal: obtiene primer y último día para un ticker.
    Retorna (ticker, has_data, first_day, last_day).
    """
    first_day = _fetch_edge_day(ticker, sort="asc")
    if first_day is None:
        # No hay datos en el rango
        return (ticker, False, None, None)

    last_day = _fetch_edge_day(ticker, sort="desc")
    # En teoría, si hay first_day debería haber last_day; pero por seguridad:
    if last_day is None:
        return (ticker, True, first_day, first_day)

    return (ticker, True, first_day, last_day)


# ----------------------
#   MAIN
# ----------------------
def parse_args():
    p = argparse.ArgumentParser(
        description="Ping Polygon para obtener primer y último día con datos por ticker"
    )
    p.add_argument("--tickers", required=True,
                   help="Archivo con universo de tickers (parquet o csv, debe tener columna 'ticker')")
    p.add_argument("--out", required=True,
                   help="Ruta de salida parquet (ej: processed/universe/ping_range_2004_2018.parquet)")
    p.add_argument("--start-date", required=True,
                   help="Fecha inicio (YYYY-MM-DD)")
    p.add_argument("--end-date", required=True,
                   help="Fecha fin (YYYY-MM-DD)")
    p.add_argument("--workers", type=int, default=max(4, cpu_count() // 2),
                   help="Número de workers (default: CPU/2, min 4)")
    return p.parse_args()


def load_tickers(path: str):
    """
    Carga la lista de tickers desde parquet o csv.
    Requiere columna 'ticker'.
    """
    ext = os.path.splitext(path)[1].lower()
    if ext == ".parquet":
        df = pl.read_parquet(path)
    else:
        df = pl.read_csv(path)

    if "ticker" not in df.columns:
        raise SystemExit(f"ERROR: el archivo {path} no tiene columna 'ticker'")

    # Unicos, por si acaso
    tickers = df["ticker"].unique().to_list()
    return tickers


def main():
    # 1. Args
    args = parse_args()

    # 2. API key
    load_dotenv()
    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        sys.exit("ERROR: POLYGON_API_KEY no encontrada (ni en entorno ni en .env)")

    # 3. Validar fechas
    try:
        start_dt = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    except ValueError:
        sys.exit("ERROR: start-date y end-date deben tener formato YYYY-MM-DD")

    if end_dt < start_dt:
        sys.exit("ERROR: end-date < start-date")

    start_str = start_dt.isoformat()
    end_str = end_dt.isoformat()

    # 4. Cargar tickers
    tickers = load_tickers(args.tickers)
    log(f"Tickers cargados: {len(tickers):,}")
    log(f"Rango: {start_str} -> {end_str}")
    log(f"Workers: {args.workers}")

    # 5. Pool
    worker_args = (api_key, start_str, end_str)
    results = []

    with Pool(
        processes=args.workers,
        initializer=init_worker,
        initargs=worker_args,
    ) as pool:
        for idx, res in enumerate(pool.imap_unordered(check_ticker_range, tickers, chunksize=16), start=1):
            results.append(res)
            if idx % 100 == 0:
                log(f"Progreso: {idx:,} / {len(tickers):,} tickers")

    # 6. DataFrame de salida
    df_out = pl.DataFrame(
        results,
        schema={
            "ticker": pl.Utf8,
            "has_data": pl.Boolean,
            "first_day": pl.Utf8,
            "last_day": pl.Utf8,
        },
    )

    df_out.write_parquet(args.out)
    log(f"Guardado: {args.out}")
    log(
        f"Resumen: {df_out['has_data'].sum()} con datos / "
        f"{len(df_out) - df_out['has_data'].sum()} sin datos"
    )


if __name__ == "__main__":
    main()
