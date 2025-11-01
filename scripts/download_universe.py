# scripts/download_universe.py
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
download_universe.py

Descarga snapshot completo de tickers desde Polygon API (31/10/2025)
Para proyecto de descarga de TODOS los ticks 2019-2025.

Uso:
  python scripts/download_universe.py \
      --outdir raw/polygon/reference/tickers_snapshot \
      --snapshot-date 2025-10-31
"""

from __future__ import annotations
import os, sys, time, json, argparse, datetime as dt
from typing import Dict, Any, Iterable, List, Optional
from urllib.parse import urlparse, parse_qs
import requests
import polars as pl
from pathlib import Path
from dotenv import load_dotenv

# ----------------------------
# Config
# ----------------------------
DEFAULT_BASE_URL = "https://api.polygon.io"
DEFAULT_LIMIT = 1000
DEFAULT_TIMEOUT = 30
RETRY_MAX = 8
RETRY_BACKOFF = 1.6

# ----------------------------
# Utilidades
# ----------------------------
def log(msg: str) -> None:
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

def normalize_ticker(t: Optional[str]) -> Optional[str]:
    if t is None:
        return None
    return t.strip().upper()

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

# ----------------------------
# Cliente HTTP con reintentos
# ----------------------------
def http_get(url: str, params: Dict[str, Any], headers: Dict[str, str], timeout: int = DEFAULT_TIMEOUT) -> Dict[str, Any]:
    last_err = None
    for attempt in range(1, RETRY_MAX + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            if r.status_code == 429:
                retry_after = int(r.headers.get("Retry-After", "2"))
                log(f"429 Rate Limit. Sleeping {retry_after}s...")
                time.sleep(retry_after)
                continue
            if 500 <= r.status_code < 600:
                delay = min(30, RETRY_BACKOFF ** attempt)
                log(f"{r.status_code} server error. Backoff {delay:.1f}s (attempt {attempt}/{RETRY_MAX})")
                time.sleep(delay)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            delay = min(30, RETRY_BACKOFF ** attempt)
            log(f"GET error: {e}. Backoff {delay:.1f}s (attempt {attempt}/{RETRY_MAX})")
            time.sleep(delay)
    raise RuntimeError(f"Failed GET {url} after {RETRY_MAX} attempts: {last_err}")

# ----------------------------
# Descarga paginada
# ----------------------------
def fetch_tickers_by_status(
    api_key: str,
    active: bool,
    market: str = "stocks",
    locale: str = "us",
    base_url: str = DEFAULT_BASE_URL,
    limit: int = DEFAULT_LIMIT,
) -> List[Dict[str, Any]]:
    """Descarga tickers por status (activos o inactivos)"""
    url = f"{base_url}/v3/reference/tickers"
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {
        "market": market,
        "locale": locale,
        "active": "true" if active else "false",
        "limit": limit,
    }

    rows = []
    next_cursor = None
    page_idx = 0
    status_label = "ACTIVOS" if active else "INACTIVOS"

    log(f"Descargando tickers {status_label}...")

    while True:
        page_idx += 1
        p = params.copy()
        if next_cursor:
            p["cursor"] = next_cursor

        data = http_get(url, p, headers=headers)
        results = data.get("results") or []
        rows.extend(results)

        log(f"  [{status_label}] Page {page_idx}: +{len(results)} (total {len(rows):,})")

        # Extraer cursor para siguiente página
        next_cursor = data.get("next_url")
        if next_cursor and next_cursor.startswith("http"):
            parsed = urlparse(next_cursor)
            cursor_params = parse_qs(parsed.query)
            next_cursor = cursor_params.get("cursor", [None])[0]

        if not next_cursor:
            break

    return rows

def fetch_all_tickers(
    api_key: str,
    market: str = "stocks",
    locale: str = "us",
    base_url: str = DEFAULT_BASE_URL,
    limit: int = DEFAULT_LIMIT,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Descarga TODOS los tickers (activos + inactivos) en dos llamadas separadas"""
    # Descarga activos
    activos = fetch_tickers_by_status(
        api_key=api_key,
        active=True,
        market=market,
        locale=locale,
        base_url=base_url,
        limit=limit
    )

    # Descarga inactivos
    inactivos = fetch_tickers_by_status(
        api_key=api_key,
        active=False,
        market=market,
        locale=locale,
        base_url=base_url,
        limit=limit
    )

    return activos, inactivos

# ----------------------------
# Procesamiento y escritura
# ----------------------------
def process_and_write(
    activos: List[Dict[str, Any]],
    inactivos: List[Dict[str, Any]],
    outdir: Path,
    snapshot_date: str
) -> None:
    """Procesa y escribe snapshot a parquet (activos + inactivos con esquemas diferentes)"""
    if not activos and not inactivos:
        raise ValueError("No se recibieron filas")

    log(f"Procesando tickers...")
    log(f"  Activos: {len(activos):,}")
    log(f"  Inactivos: {len(inactivos):,}")

    # Crear DataFrames separados
    df_active = pl.from_dicts(activos) if activos else pl.DataFrame()
    df_inactive = pl.from_dicts(inactivos) if inactivos else pl.DataFrame()

    # Normalizar ticker en ambos
    for df_name, df in [("activos", df_active), ("inactivos", df_inactive)]:
        if len(df) > 0 and "ticker" in df.columns:
            if df_name == "activos":
                df_active = df_active.with_columns(
                    pl.col("ticker").map_elements(normalize_ticker, return_dtype=pl.Utf8).alias("ticker")
                )
            else:
                df_inactive = df_inactive.with_columns(
                    pl.col("ticker").map_elements(normalize_ticker, return_dtype=pl.Utf8).alias("ticker")
                )

    # Añadir snapshot_date a ambos
    if len(df_active) > 0:
        df_active = df_active.with_columns(pl.lit(snapshot_date).alias("snapshot_date"))
    if len(df_inactive) > 0:
        df_inactive = df_inactive.with_columns(pl.lit(snapshot_date).alias("snapshot_date"))

    # Concatenación diagonal (maneja esquemas diferentes)
    df_all = pl.concat([df_active, df_inactive], how="diagonal")

    # Deduplicar por ticker (keep last)
    if "last_updated_utc" in df_all.columns:
        df_all = df_all.sort(by=["ticker", "last_updated_utc"]).unique(subset=["ticker"], keep="last")
    else:
        df_all = df_all.unique(subset=["ticker"], keep="last")

    # Crear particiones
    part_dir = outdir / f"snapshot_date={snapshot_date}"
    ensure_dir(part_dir)

    # Escribir archivo principal
    all_path = part_dir / "tickers_all.parquet"
    df_all.write_parquet(all_path)
    log(f"Escrito: {all_path} ({len(df_all):,} tickers)")

    # Escribir archivos separados
    active_path = part_dir / "tickers_active.parquet"
    inactive_path = part_dir / "tickers_inactive.parquet"

    if len(df_active) > 0:
        df_active.write_parquet(active_path)
        log(f"Activos: {len(df_active):,} tickers -> {active_path}")

    if len(df_inactive) > 0:
        df_inactive.write_parquet(inactive_path)
        log(f"Inactivos: {len(df_inactive):,} tickers -> {inactive_path}")

    # CSV resumen
    summary = pl.DataFrame({
        "category": ["activos", "inactivos", "total"],
        "count": [len(df_active), len(df_inactive), len(df_all)]
    })
    summary_path = part_dir / "summary.csv"
    summary.write_csv(summary_path)
    log(f"Resumen: {summary_path}")

# ----------------------------
# Main
# ----------------------------
def main():
    ap = argparse.ArgumentParser(description="Descarga snapshot completo de tickers (2025-10-31)")
    ap.add_argument("--outdir", type=str, default="raw/polygon/reference/tickers_snapshot",
                    help="Directorio de salida")
    ap.add_argument("--snapshot-date", type=str, default="2025-10-31",
                    help="Fecha snapshot (YYYY-MM-DD)")
    ap.add_argument("--market", type=str, default="stocks", help="Market (default: stocks)")
    ap.add_argument("--locale", type=str, default="us", help="Locale (default: us)")
    ap.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Tickers por página")
    args = ap.parse_args()
    
    # Cargar .env
    load_dotenv()
    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        log("ERROR: POLYGON_API_KEY no encontrada en .env")
        sys.exit(1)
    
    outdir = Path(args.outdir)
    ensure_dir(outdir)
    
    log(f"Descargando snapshot completo...")
    log(f"   Market: {args.market}")
    log(f"   Locale: {args.locale}")
    log(f"   Fecha: {args.snapshot_date}")
    log(f"   Output: {outdir}")
    
    # Descarga
    t0 = time.time()
    activos, inactivos = fetch_all_tickers(
        api_key=api_key,
        market=args.market,
        locale=args.locale,
        limit=args.limit
    )
    t1 = time.time()

    total = len(activos) + len(inactivos)
    log(f"Descarga completada en {t1-t0:.1f}s")
    log(f"  Total: {total:,} tickers")
    log(f"  Activos: {len(activos):,}")
    log(f"  Inactivos: {len(inactivos):,}")

    # Procesar y escribir
    process_and_write(activos, inactivos, outdir, args.snapshot_date)

    log("Hecho!")

if __name__ == "__main__":
    main()