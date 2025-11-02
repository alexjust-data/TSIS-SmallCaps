#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ingest_ohlcv_daily.py
Descarga OHLCV diario (1 day bars) desde Polygon.io para lista de tickers.
Soporta paginación correcta con cursor y escritura idempotente por ticker/year.

Adaptado para TSIS_SmallCaps:
- Acepta CSV o Parquet como input (auto-detecta)
- Período: 2019-2025
- Universo: 6,405 tickers Small Caps

Uso:
    export POLYGON_API_KEY="tu_api_key"
    python scripts/ingest_ohlcv_daily.py \
        --tickers-csv processed/universe/smallcaps_universe_2025-11-01.parquet \
        --outdir raw/polygon/ohlcv_daily \
        --from 2019-01-01 \
        --to 2025-11-01 \
        --max-workers 12
"""
import os
import sys
import io
import time
import argparse
import datetime as dt
from pathlib import Path
from typing import Dict, Any, List, Optional
import urllib.parse as urlparse
import requests
import polars as pl
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# Configure UTF-8 encoding for stdout/stderr
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Load environment variables from .env file
load_dotenv()

# Configuración
BASE_URL = "https://api.polygon.io"
TIMEOUT = 35
RETRY_MAX = 8
BACKOFF = 1.6
PAGE_LIMIT = 50000
ADJUSTED = True

def log(m):
    """Log con timestamp"""
    print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] {m}", flush=True)

def load_tickers(path: str) -> List[str]:
    """
    Carga lista de tickers desde CSV o Parquet (auto-detecta)

    Args:
        path: Ruta al archivo CSV o Parquet con columna 'ticker'

    Returns:
        Lista de tickers únicos
    """
    path_obj = Path(path)

    if not path_obj.exists():
        raise FileNotFoundError(f"No existe el archivo: {path}")

    # Auto-detectar formato por extensión
    if path.endswith('.parquet'):
        log(f"Detectado formato Parquet: {path}")
        df = pl.read_parquet(path)
    elif path.endswith('.csv'):
        log(f"Detectado formato CSV: {path}")
        df = pl.read_csv(path)
    else:
        # Intentar leer como CSV por defecto
        log(f"Extensión desconocida, intentando como CSV: {path}")
        df = pl.read_csv(path)

    if "ticker" not in df.columns:
        raise ValueError(f"El archivo debe tener columna 'ticker'. Columnas encontradas: {df.columns}")

    tickers = df["ticker"].drop_nulls().unique().to_list()
    log(f"Cargados {len(tickers):,} tickers únicos")

    return tickers

def parse_next_cursor(next_url: Optional[str]) -> Optional[str]:
    """Extrae cursor de URL completa retornada por Polygon"""
    if not next_url:
        return None
    try:
        q = urlparse.urlparse(next_url).query
        qs = urlparse.parse_qs(q)
        cur = qs.get("cursor")
        return cur[0] if cur else None
    except Exception:
        return None

def http_get_json(url: str, params: Dict[str, Any], headers: Dict[str, str]) -> Dict[str, Any]:
    """GET con reintentos exponenciales y manejo de rate limits"""
    last_error = None
    for attempt in range(1, RETRY_MAX + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=TIMEOUT)

            if r.status_code == 429:
                sleep_time = int(r.headers.get("Retry-After", "2"))
                log(f"429 Rate Limit -> sleep {sleep_time}s")
                time.sleep(sleep_time)
                continue

            if 500 <= r.status_code < 600:
                sleep_time = min(30, BACKOFF ** attempt)
                log(f"{r.status_code} Server Error -> backoff {sleep_time:.1f}s")
                time.sleep(sleep_time)
                continue

            r.raise_for_status()
            return r.json()

        except Exception as e:
            last_error = e
            sleep_time = min(30, BACKOFF ** attempt)
            log(f"GET error {e} -> backoff {sleep_time:.1f}s")
            time.sleep(sleep_time)

    raise RuntimeError(f"Failed after {RETRY_MAX} attempts: {last_error}")

def fetch_daily(api_key: str, ticker: str, from_date: str, to_date: str) -> List[Dict[str, Any]]:
    """Descarga OHLCV diario para un ticker con paginación completa"""
    url = f"{BASE_URL}/v2/aggs/ticker/{ticker}/range/1/day/{from_date}/{to_date}"
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {
        "adjusted": str(ADJUSTED).lower(),
        "sort": "asc",
        "limit": PAGE_LIMIT
    }

    rows: List[Dict[str, Any]] = []
    cursor = None
    pages = 0

    while True:
        p = params.copy()
        if cursor:
            p["cursor"] = cursor

        data = http_get_json(url, p, headers) or {}
        results = data.get("results") or []
        rows.extend(results)
        pages += 1

        # Extraer cursor de next_url
        cursor = (parse_next_cursor(data.get("next_url")) or
                 data.get("next_url_cursor") or
                 data.get("cursor"))

        if not cursor:
            break

    log(f"{ticker}: {len(rows):,} rows ({pages} pages)")
    return rows

def rows_to_df(rows: List[Dict[str, Any]], ticker: str) -> pl.DataFrame:
    """Convierte resultados JSON a DataFrame Polars"""
    if not rows:
        return pl.DataFrame({
            "ticker": [], "date": [], "t": [], "o": [], "h": [],
            "l": [], "c": [], "v": [], "n": [], "vw": []
        })

    df = pl.from_dicts(rows)

    # Asegurar tipos correctos
    picks = {}
    for col, typ in [("t", pl.Int64), ("o", pl.Float64), ("h", pl.Float64),
                     ("l", pl.Float64), ("c", pl.Float64), ("v", pl.Float64),
                     ("n", pl.Int64), ("vw", pl.Float64)]:
        picks[col] = (df[col].cast(typ) if col in df.columns
                     else pl.Series(name=col, values=[], dtype=typ))

    out = pl.DataFrame(picks)

    if out.height > 0:
        # Convertir timestamp a date
        out = out.with_columns([
            pl.from_epoch(pl.col("t") / 1000, time_unit="s").dt.strftime("%Y-%m-%d").alias("date"),
            pl.lit(ticker).alias("ticker")
        ])
    else:
        out = out.with_columns([
            pl.Series(name="date", values=[], dtype=pl.Utf8),
            pl.lit(ticker).alias("ticker")
        ])

    return out.select(["ticker", "date", "t", "o", "h", "l", "c", "v", "n", "vw"])

def write_by_year(df: pl.DataFrame, outdir: Path, ticker: str) -> int:
    """Escribe datos particionados por year, merge idempotente"""
    if df.height == 0:
        return 0

    df = df.with_columns(pl.col("date").str.slice(0, 4).alias("year"))
    files_written = 0

    for year, part in df.group_by("year"):
        y = year[0]
        part = part.drop("year").sort("date")

        # Crear directorio ticker/year=YYYY
        pdir = outdir / ticker / f"year={y}"
        pdir.mkdir(parents=True, exist_ok=True)

        outp = pdir / "daily.parquet"

        # Merge idempotente
        if outp.exists():
            old = pl.read_parquet(outp)
            merged = pl.concat([old, part], how="vertical_relaxed")\
                      .unique(subset=["date"], keep="last")\
                      .sort("date")
            merged.write_parquet(outp)
        else:
            part.write_parquet(outp)

        files_written += 1

    return files_written

def main():
    ap = argparse.ArgumentParser(description="Descarga OHLCV diario desde Polygon")
    ap.add_argument("--tickers-csv", required=True,
                    help="CSV o Parquet con columna 'ticker'")
    ap.add_argument("--outdir", required=True,
                    help="Directorio de salida (ej: raw/polygon/ohlcv_daily)")
    ap.add_argument("--from", dest="date_from", required=True,
                    help="Fecha inicio YYYY-MM-DD")
    ap.add_argument("--to", dest="date_to", required=True,
                    help="Fecha fin YYYY-MM-DD")
    ap.add_argument("--max-workers", type=int, default=12,
                    help="Workers paralelos (default: 12)")
    args = ap.parse_args()

    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        sys.exit("ERROR: variable POLYGON_API_KEY no establecida")

    # Leer lista de tickers (CSV o Parquet)
    tickers = load_tickers(args.tickers_csv)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    log(f"Descargando DAILY para {len(tickers):,} tickers [{args.date_from} → {args.date_to}]")
    log(f"Workers: {args.max_workers} | Outdir: {outdir}")

    results = []

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {
            executor.submit(fetch_daily, api_key, t, args.date_from, args.date_to): t
            for t in tickers
        }

        for i, future in enumerate(as_completed(futures), 1):
            ticker = futures[future]
            try:
                rows = future.result()
                df = rows_to_df(rows, ticker)
                files = write_by_year(df, outdir, ticker)
                results.append(f"{ticker}: {df.height:,} rows, {files} files")
            except Exception as e:
                results.append(f"{ticker}: ERROR {e}")

            if i % 200 == 0:
                log(f"Progreso {i:,}/{len(tickers):,}")

    # Guardar log de resultados
    ok = sum("ERROR" not in r for r in results)
    err = len(results) - ok

    log_file = outdir / "daily_download.log"
    log_file.write_text("\n".join(results), encoding="utf-8")

    log(f"\n=== COMPLETADO ===")
    log(f"OK: {ok:,} | ERRORES: {err:,}")
    log(f"Log: {log_file}")

if __name__ == "__main__":
    main()
