# scripts/ingest_splits_dividends.py
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ingest_splits_dividends.py

Descarga splits y dividends GLOBALES (sin filtros) desde Polygon API.
Este script descarga TODOS los eventos corporativos disponibles y los particiona por año.

IMPORTANTE: Esto es una descarga GLOBAL que sirve como backup.
Los datos se filtrarán posteriormente para nuestro universo (6,405 tickers).

Endpoints:
- /v3/reference/splits (sin filtros - todos los tickers)
- /v3/reference/dividends (sin filtros - todos los tickers)

Uso:
  python scripts/ingest_splits_dividends.py \
      --outdir raw/polygon/reference

Output:
  raw/polygon/reference/
  ├── splits/
  │   └── year=*/splits.parquet
  └── dividends/
      └── year=*/dividends.parquet
"""

import os
import sys
import time
import argparse
import datetime as dt
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import requests
import polars as pl
from dotenv import load_dotenv

# Configuración
BASE_URL = "https://api.polygon.io"
LIMIT = 1000
TIMEOUT = 25
RETRY_MAX = 8
RETRY_BACKOFF = 1.6

def log(msg: str):
    """Log con timestamp"""
    print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)

def http_get(url: str, api_key: str, params: dict = None) -> dict:
    """
    GET request con reintentos y manejo de rate limiting

    Returns:
        dict: Response JSON o dict vacío si falla
    """
    headers = {"Authorization": f"Bearer {api_key}"}

    for attempt in range(RETRY_MAX):
        try:
            r = requests.get(url, headers=headers, params=params or {}, timeout=TIMEOUT)

            # Rate limiting (429)
            if r.status_code == 429:
                retry_after = int(r.headers.get("Retry-After", "2"))
                log(f"429 Too Many Requests. Sleeping {retry_after}s...")
                time.sleep(retry_after)
                continue

            # Server errors (500-599)
            if 500 <= r.status_code < 600:
                log(f"Server error {r.status_code}. Retrying...")
                time.sleep(RETRY_BACKOFF ** attempt)
                continue

            r.raise_for_status()
            return r.json()

        except Exception as e:
            log(f"Error en intento {attempt + 1}/{RETRY_MAX}: {e}")
            time.sleep(RETRY_BACKOFF ** attempt)

    return {}

def fetch_paged(path: str, api_key: str, extra_params: dict = None):
    """
    Descarga datos paginados desde Polygon API

    Yields:
        dict: Cada resultado individual
    """
    url = f"{BASE_URL}{path}"
    params = {"limit": LIMIT}
    if extra_params:
        params.update(extra_params)

    cursor = None
    total = 0
    page = 0

    while True:
        page += 1
        p = params.copy()
        if cursor:
            p["cursor"] = cursor

        data = http_get(url, api_key, p) or {}
        results = data.get("results") or []

        for item in results:
            yield item

        total += len(results)

        # Log progreso cada 10K registros
        if total % 10000 == 0 and total > 0:
            log(f"{path}: {total:,} filas (pagina {page})")

        # Extraer cursor de next_url
        next_cursor = (data.get("next_url") or
                      data.get("next_url_cursor") or
                      data.get("cursor") or
                      data.get("next_cursor"))

        if next_cursor and next_cursor.startswith("http"):
            # Extraer cursor de URL completa
            parsed = urlparse(next_cursor)
            cursor_params = parse_qs(parsed.query)
            next_cursor = cursor_params.get("cursor", [None])[0]

        cursor = next_cursor

        if not cursor:
            break

    log(f"{path}: {total:,} filas TOTAL")

def clean_splits(df: pl.DataFrame) -> pl.DataFrame:
    """
    Limpia y normaliza datos de splits

    Args:
        df: DataFrame con splits raw

    Returns:
        DataFrame limpio con tipos correctos y duplicados eliminados
    """
    if df.height == 0:
        return df

    # Cast de tipos
    cast_map = {
        "ticker": pl.Utf8,
        "execution_date": pl.Utf8,
        "split_from": pl.Float64,
        "split_to": pl.Float64,
        "declared_date": pl.Utf8
    }

    for col, dtype in cast_map.items():
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(dtype))

    # Calcular ratio (ej: 2-for-1 split = 2.0)
    if all(c in df.columns for c in ("split_from", "split_to")):
        df = df.with_columns(
            (pl.col("split_from") / pl.col("split_to")).alias("ratio")
        )

    # Eliminar duplicados (mantener último)
    if "execution_date" in df.columns:
        df = df.sort(["ticker", "execution_date"]).unique(
            subset=["ticker", "execution_date", "split_from", "split_to"],
            keep="last"
        )

    return df

def clean_dividends(df: pl.DataFrame) -> pl.DataFrame:
    """
    Limpia y normaliza datos de dividends

    Args:
        df: DataFrame con dividends raw

    Returns:
        DataFrame limpio con tipos correctos y duplicados eliminados
    """
    if df.height == 0:
        return df

    # Cast de tipos
    cast_map = {
        "ticker": pl.Utf8,
        "ex_dividend_date": pl.Utf8,
        "cash_amount": pl.Float64,
        "declaration_date": pl.Utf8,
        "record_date": pl.Utf8,
        "payable_date": pl.Utf8,
        "frequency": pl.Utf8,
        "dividend_type": pl.Utf8
    }

    for col, dtype in cast_map.items():
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(dtype))

    # Eliminar duplicados (mantener último)
    if "ex_dividend_date" in df.columns and "cash_amount" in df.columns:
        df = df.sort(["ticker", "ex_dividend_date"]).unique(
            subset=["ticker", "ex_dividend_date", "cash_amount"],
            keep="last"
        )

    return df

def main():
    ap = argparse.ArgumentParser(description="Descargar splits y dividends globales")
    ap.add_argument("--outdir", type=str, required=True,
                    help="Directorio base de salida (raw/polygon/reference)")
    args = ap.parse_args()

    # Cargar .env
    load_dotenv()
    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        log("ERROR: POLYGON_API_KEY no encontrada en .env")
        sys.exit(1)

    base_dir = Path(args.outdir)
    base_dir.mkdir(parents=True, exist_ok=True)

    # ========================================================================
    # SPLITS
    # ========================================================================

    log("Descargando SPLITS (global - sin filtros)...")
    log("NOTA: Esto puede tomar varios minutos dependiendo del total de datos")

    splits = list(fetch_paged("/v3/reference/splits", api_key))

    if splits:
        df_splits = clean_splits(pl.from_dicts(splits))

        if df_splits.height > 0:
            log(f"\nProcesando {len(df_splits):,} splits...")

            # Particionar por año de execution_date
            df_splits = df_splits.with_columns(
                pl.col("execution_date").str.slice(0, 4).alias("year")
            )

            # Guardar por año
            for year_tuple, partition in df_splits.group_by("year"):
                year = year_tuple[0]
                outdir = base_dir / "splits" / f"year={year}"
                outdir.mkdir(parents=True, exist_ok=True)

                partition_clean = partition.drop("year")
                partition_clean.write_parquet(outdir / "splits.parquet")

                log(f"   year={year}: {len(partition):,} splits")

            # Estadísticas
            n_tickers = df_splits["ticker"].n_unique()
            min_year = df_splits["year"].min()
            max_year = df_splits["year"].max()

            log(f"\nSplits guardados en {base_dir / 'splits'}")
            log(f"   Total splits: {len(df_splits):,}")
            log(f"   Tickers únicos: {n_tickers:,}")
            log(f"   Período: {min_year}-{max_year}")
    else:
        log("No se descargaron splits")

    # ========================================================================
    # DIVIDENDS
    # ========================================================================

    log("\nDescargando DIVIDENDS (global - sin filtros)...")
    log("NOTA: Esto puede tomar varios minutos (dividends son MUCHOS más que splits)")

    dividends = list(fetch_paged("/v3/reference/dividends", api_key))

    if dividends:
        df_dividends = clean_dividends(pl.from_dicts(dividends))

        if df_dividends.height > 0:
            log(f"\nProcesando {len(df_dividends):,} dividends...")

            # Particionar por año de ex_dividend_date
            df_dividends = df_dividends.with_columns(
                pl.col("ex_dividend_date").str.slice(0, 4).alias("year")
            )

            # Guardar por año
            for year_tuple, partition in df_dividends.group_by("year"):
                year = year_tuple[0]
                outdir = base_dir / "dividends" / f"year={year}"
                outdir.mkdir(parents=True, exist_ok=True)

                partition_clean = partition.drop("year")
                partition_clean.write_parquet(outdir / "dividends.parquet")

                log(f"   year={year}: {len(partition):,} dividends")

            # Estadísticas
            n_tickers = df_dividends["ticker"].n_unique()
            min_year = df_dividends["year"].min()
            max_year = df_dividends["year"].max()

            log(f"\nDividends guardados en {base_dir / 'dividends'}")
            log(f"   Total dividends: {len(df_dividends):,}")
            log(f"   Tickers únicos: {n_tickers:,}")
            log(f"   Período: {min_year}-{max_year}")
    else:
        log("No se descargaron dividends")

    log("\nHecho! Datos globales de splits y dividends descargados exitosamente.")
    log("SIGUIENTE PASO: Filtrar estos datos para nuestro universo (6,405 tickers)")

if __name__ == "__main__":
    main()
