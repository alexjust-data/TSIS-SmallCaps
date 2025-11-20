#!/usr/bin/env python3
"""
=============================================================================
POLYGON TRADES INGESTOR V2 - Streaming con rate limit adaptativo
=============================================================================
Descarga trades tick-by-tick desde Polygon.io con escritura streaming
y manejo robusto de rate limits y errores.

USO:
    python ingest_trades_ticks.py \
        --tickers SPY AAPL MSFT \
        --outdir /path/to/data \
        --from 2023-01-01 \
        --to 2023-12-31 \
        --rate-limit 0.05
"""

import argparse
import time
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
import json

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import polars as pl

# =============================================================================
# CONFIGURACION Y CONSTANTES
# =============================================================================

DEFAULT_RATE_LIMIT = 0.05  # segundos entre requests (20 req/s)
BATCH_SIZE = 50000  # maximo de resultados por pagina
MAX_RETRIES = 10
BACKOFF_FACTOR = 0.8  # factor de backoff exponencial
TIMEOUT_SECONDS = 45  # timeout para cada request

# Configurar logging
logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


def load_tickers(tickers_arg) -> List[str]:
    """Carga tickers desde archivo CSV o lista de argumentos"""
    # Si es un solo string que podria ser archivo
    if isinstance(tickers_arg, str):
        path = Path(tickers_arg)
        if path.exists():
            logger.info(f"Detectado formato CSV: {path}")
            df = pd.read_csv(path)
            tickers = df["ticker"].unique().tolist()
            logger.info(f"Cargados {len(tickers)} tickers únicos")
            return tickers
        else:
            # No es archivo, tratarlo como ticker individual
            return [tickers_arg]

    # Si es una lista (multiples tickers por CLI)
    elif isinstance(tickers_arg, list):
        # Verificar si el primer elemento es un archivo
        path = Path(tickers_arg[0])
        if len(tickers_arg) == 1 and path.exists():
            logger.info(f"Detectado formato CSV: {path}")
            df = pd.read_csv(path)
            tickers = df["ticker"].unique().tolist()
            logger.info(f"Cargados {len(tickers)} tickers únicos")
            return tickers
        else:
            # Es una lista de tickers directamente
            return tickers_arg
    else:
        raise ValueError(f"Formato de tickers no reconocido: {type(tickers_arg)}")


def create_session(max_retries: int = MAX_RETRIES) -> requests.Session:
    """Crea session con retry logic"""
    session = requests.Session()
    retry = Retry(
        total=max_retries,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=20, pool_connections=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def fetch_trades_batch(
    session: requests.Session,
    api_key: str,
    ticker: str,
    day: str,
    next_url: Optional[str] = None,
) -> Dict[str, Any]:
    """Fetches a single batch of trades from Polygon"""
    # Parse next_url or build initial URL
    if next_url:
        url = next_url
        # Add API key if not present
        if "apiKey=" not in url:
            separator = "&" if "?" in url else "?"
            url = f"{url}{separator}apiKey={api_key}"
    else:
        # Build timestamp range for the day (4am to 8pm ET)
        ts_start = int(pd.Timestamp(f"{day} 04:00:00", tz="America/New_York").timestamp() * 1e9)
        ts_end = int(pd.Timestamp(f"{day} 20:00:00", tz="America/New_York").timestamp() * 1e9)

        url = (
            f"https://api.polygon.io/v3/trades/{ticker}"
            f"?timestamp.gte={ts_start}"
            f"&timestamp.lte={ts_end}"
            f"&limit={BATCH_SIZE}"
            f"&sort=timestamp"
            f"&apiKey={api_key}"
        )

    # Make request with timeout
    response = session.get(url, timeout=TIMEOUT_SECONDS)
    response.raise_for_status()

    return response.json()


def stream_trades_to_parquet(
    trades: List[Dict],
    file_path: Path,
    is_first_batch: bool = True,
    compression: str = "snappy",
):
    """Streams trades to parquet file"""
    if not trades:
        return

    df = pl.DataFrame(trades)

    # Select only required columns if they exist
    columns_to_keep = []
    for col in ["participant_timestamp", "price", "size", "conditions", "exchange"]:
        if col in df.columns:
            columns_to_keep.append(col)

    if columns_to_keep:
        df = df.select(columns_to_keep)

    # Rename columns to shorter names
    rename_map = {
        "participant_timestamp": "t",
        "price": "p",
        "size": "s",
        "conditions": "c",
        "exchange": "i",
    }

    for old_name, new_name in rename_map.items():
        if old_name in df.columns:
            df = df.rename({old_name: new_name})

    # Write or append to parquet
    if is_first_batch:
        df.write_parquet(file_path, compression=compression)
    else:
        # Read existing, concatenate, and write back
        existing_df = pl.read_parquet(file_path)
        combined_df = pl.concat([existing_df, df])
        combined_df.write_parquet(file_path, compression=compression)


def process_batch_polars(batch: Dict[str, Any]) -> Optional[pl.DataFrame]:
    """Process a batch of trades into a Polars DataFrame"""
    if not batch.get("results"):
        return None

    # FIX: Manejar columnas opcionales de forma robusta
    t_data = batch.get("participant_timestamp", [])
    p_data = batch.get("price", [])
    s_data = batch.get("size", [])
    c_data = batch.get("conditions", [])
    i_data = batch.get("exchange", [])
    
    # Si no hay datos de timestamp, no hay nada que procesar
    if not t_data:
        return None
    
    # Asegurar que todas las columnas tengan el mismo tamaño
    data_len = len(t_data)
    
    # Si las columnas opcionales están vacías o tienen diferente tamaño, rellenar con None
    if not c_data or len(c_data) != data_len:
        c_data = [None] * data_len
    if not i_data or len(i_data) != data_len:
        i_data = [None] * data_len
    
    # Asegurar que p y s también tienen el tamaño correcto
    if not p_data or len(p_data) != data_len:
        p_data = [None] * data_len
    if not s_data or len(s_data) != data_len:
        s_data = [None] * data_len
    
    df = pl.DataFrame({
        "t": t_data,
        "p": p_data,
        "s": s_data,
        "c": c_data,
        "i": i_data
    })

    return df


def fetch_and_stream_write_trades(
    session: requests.Session,
    api_key: str,
    ticker: str,
    day: str,
    output_dir: Path,
    rate_limit: float,
    stats: Dict[str, int],
) -> int:
    """Fetch and write trades for a single day with streaming"""
    # Create directory structure
    date_obj = pd.Timestamp(day)
    year_dir = output_dir / ticker / f"year={date_obj.year:04d}"
    month_dir = year_dir / f"month={date_obj.month:02d}"
    day_dir = month_dir / f"day={day}"
    day_dir.mkdir(parents=True, exist_ok=True)

    # File paths
    premarket_fp = day_dir / "premarket.parquet"
    market_fp = day_dir / "market.parquet"
    afterhours_fp = day_dir / "afterhours.parquet"

    total_trades = 0
    next_url = None
    batch_count = 0
    retry_count = 0

    # Lists to accumulate trades by session
    premarket_trades = []
    market_trades = []
    afterhours_trades = []

    while True:
        try:
            # Fetch batch
            data = fetch_trades_batch(session, api_key, ticker, day, next_url)

            if "results" in data and data["results"]:
                batch_count += 1
                trades_in_batch = len(data["results"])
                total_trades += trades_in_batch

                # Split trades by market session
                for trade in data["results"]:
                    ts_ns = trade.get("participant_timestamp", 0)
                    if ts_ns:
                        # Convert nanoseconds to datetime
                        ts = pd.Timestamp(ts_ns, tz="America/New_York")
                        hour = ts.hour

                        if hour < 9 or (hour == 9 and ts.minute < 30):
                            premarket_trades.append(trade)
                        elif hour < 16:
                            market_trades.append(trade)
                        else:
                            afterhours_trades.append(trade)

                # Update stats
                stats["requests"] += 1

                # Progress logging every 5 batches
                if batch_count % 5 == 0:
                    logger.info(f"  {ticker} {day}: Batch {batch_count}, Total {total_trades:,} trades")

            # Check for next page
            next_url = data.get("next_url")
            if not next_url:
                break

            # Rate limiting
            time.sleep(rate_limit)

        except requests.exceptions.RequestException as e:
            retry_count += 1
            if retry_count > MAX_RETRIES:
                logger.error(f"  {ticker} {day}: Max retries exceeded - {e}")
                stats["errors"] += 1
                break

            backoff = BACKOFF_FACTOR * (2**retry_count)
            logger.warning(f"  {ticker} {day}: Retry {retry_count}/{MAX_RETRIES} after {backoff:.1f}s")
            time.sleep(backoff)

    # Write accumulated trades to parquet files
    if premarket_trades:
        write_trades_to_parquet(premarket_trades, premarket_fp)
        logger.debug(f"  Wrote {len(premarket_trades)} premarket trades")

    if market_trades:
        write_trades_to_parquet(market_trades, market_fp)
        logger.debug(f"  Wrote {len(market_trades)} market trades")

    if afterhours_trades:
        write_trades_to_parquet(afterhours_trades, afterhours_fp)
        logger.debug(f"  Wrote {len(afterhours_trades)} afterhours trades")

    return total_trades


def write_trades_to_parquet(trades: List[Dict], file_path: Path):
    """Write trades to parquet file with proper column handling"""
    if not trades:
        return

    # Extract data from trades
    t_data = [t.get("participant_timestamp") for t in trades]
    p_data = [t.get("price") for t in trades]
    s_data = [t.get("size") for t in trades]
    c_data = [t.get("conditions", []) for t in trades]
    i_data = [t.get("exchange") for t in trades]
    
    # Handle missing or inconsistent data
    data_len = len(trades)
    
    # Ensure all lists have the same length
    if not all(len(lst) == data_len for lst in [t_data, p_data, s_data] if lst):
        logger.warning(f"Inconsistent data lengths, normalizing...")
    
    # Fix conditions - it might be a list of lists, we need to handle properly
    fixed_conditions = []
    for c in c_data:
        if isinstance(c, list):
            fixed_conditions.append(c if c else None)
        else:
            fixed_conditions.append(None)
    
    # Create DataFrame with consistent column sizes
    df_data = {
        "t": t_data,
        "p": p_data,
        "s": s_data,
        "c": fixed_conditions,
        "i": i_data
    }
    
    # Create DataFrame
    df = pl.DataFrame(df_data)
    
    # Write to parquet
    df.write_parquet(file_path, compression="snappy")


def count_trades_in_parquet(file_path: Path) -> int:
    """Count number of trades in a parquet file"""
    if not file_path.exists():
        return 0
    try:
        df = pl.read_parquet(file_path, columns=["t"])
        return len(df)
    except Exception as e:
        logger.warning(f"Could not read {file_path}: {e}")
        return 0


def main():
    parser = argparse.ArgumentParser(description="Download trades from Polygon.io")
    parser.add_argument(
        "--tickers", "--tickers-csv", dest="tickers_csv", nargs="+", required=True, help="Tickers or CSV file"
    )
    parser.add_argument("--outdir", required=True, help="Output directory")
    parser.add_argument("--from", dest="from_date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to", dest="to_date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--rate-limit", type=float, default=DEFAULT_RATE_LIMIT, help="Seconds between requests"
    )
    parser.add_argument(
        "--resume", action="store_true", help="Resume from last checkpoint"
    )
    parser.add_argument(
        "--max-tickers-per-process",
        type=int,
        default=None,
        help="Maximum tickers to process before exiting",
    )
    args = parser.parse_args()

    # Load tickers
    tickers = load_tickers(args.tickers_csv)

    # Get API key
    api_key = os.environ.get("POLYGON_API_KEY")
    if not api_key:
        logger.error("POLYGON_API_KEY not found in environment")
        sys.exit(1)

    # Setup paths
    output_dir = Path(args.outdir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Parse dates
    start_date = pd.Timestamp(args.from_date)
    end_date = pd.Timestamp(args.to_date)
    date_range = pd.date_range(start_date, end_date, freq="D")

    # Filter to weekdays only (basic approach)
    date_range = [d for d in date_range if d.weekday() < 5]

    # Create session
    session = create_session()

    # Stats tracking
    global_stats = {"requests": 0, "errors": 0, "ok": 0}
    start_time = time.time()

    # Log configuration
    logger.info(
        f"Tickers: {len(tickers)} | {args.from_date} -> {args.to_date} | rate={args.rate_limit}s/page (adaptativo)"
    )

    # Process tickers
    for ticker_idx, ticker in enumerate(tickers, 1):
        if args.max_tickers_per_process and ticker_idx > args.max_tickers_per_process:
            logger.info(f"Alcanzado --max-tickers-per-process={args.max_tickers_per_process}. Saliendo limpio.")
            break

        logger.info(f"Processing {ticker} (ticker {ticker_idx}/{len(tickers)})...")

        ticker_dir = output_dir / ticker
        if args.resume and ticker_dir.exists():
            # Skip if resuming and ticker directory exists
            logger.info(f"  Skipping {ticker} (already exists)")
            continue

        ticker_start = time.time()
        ticker_trades = 0
        ticker_days = 0
        days_processed = 0

        for day in date_range:
            day_str = day.strftime("%Y-%m-%d")

            # Check if day already has _SUCCESS marker (new checkpointing)
            date_obj = pd.Timestamp(day_str)
            year_dir = output_dir / ticker / f"year={date_obj.year:04d}"
            month_dir = year_dir / f"month={date_obj.month:02d}"
            day_dir = month_dir / f"day={day_str}"
            success_marker = day_dir / "_SUCCESS"

            # New checkpointing logic
            premarket_fp = day_dir / "premarket.parquet"
            market_fp = day_dir / "market.parquet"

            # CASO 1: Dia completo con _SUCCESS
            if success_marker.exists():
                # Count existing trades but don't re-download
                trades_count = 0
                if premarket_fp.exists():
                    trades_count += count_trades_in_parquet(premarket_fp)
                if market_fp.exists():
                    trades_count += count_trades_in_parquet(market_fp)
                afterhours_fp = day_dir / "afterhours.parquet"
                if afterhours_fp.exists():
                    trades_count += count_trades_in_parquet(afterhours_fp)

                ticker_trades += trades_count
                ticker_days += 1
                continue

            # CASO 2: Dia con parquet(s) pero sin _SUCCESS
            # Cambio critico: usar OR en lugar de AND
            if (premarket_fp.exists() or market_fp.exists()) and (not success_marker.exists()):
                logger.warning(f"  {day_str}: parquet(s) found but no _SUCCESS -> re-downloading")

                # RE-DESCARGAR para garantizar integridad
                trades_count = fetch_and_stream_write_trades(
                    session, api_key, ticker, day_str, output_dir, args.rate_limit, global_stats
                )

                ticker_trades += trades_count
                if trades_count > 0:
                    ticker_days += 1

                # Recontar trades tras re-descarga
                # Marcar completo SOLO si descarga exitosa
                if premarket_fp.exists() or market_fp.exists():
                    success_marker.touch()

                days_processed += 1
                continue

            # CASO 3: Dia nuevo (sin parquet ni _SUCCESS)
            # Download this day
            trades_count = fetch_and_stream_write_trades(
                session, api_key, ticker, day_str, output_dir, args.rate_limit, global_stats
            )

            ticker_trades += trades_count
            if trades_count > 0:
                ticker_days += 1

            # Mark day as complete if any trades were downloaded
            # Cambio critico: usar OR en lugar de AND
            if premarket_fp.exists() or market_fp.exists():
                success_marker.touch()

            days_processed += 1

            # Progress indicator every 200 days
            if days_processed % 200 == 0:
                logger.info(f"  {ticker}: {days_processed}/~{len(date_range)} días | {ticker_trades:,} trades")

        ticker_elapsed = time.time() - ticker_start

        # Update global stats
        if ticker_trades > 0:
            global_stats["ok"] += 1
        else:
            global_stats["errors"] += 1

        logger.debug(
            f"  {ticker}: {ticker_trades:,} trades, {ticker_days} days, {ticker_elapsed:.1f}s"
        )

        # Log progress every 10 tickers
        if ticker_idx % 10 == 0:
            elapsed = time.time() - start_time
            req_rate = global_stats["requests"] / elapsed if elapsed > 0 else 0
            logger.info(f"Progreso {ticker_idx}/{len(tickers)} | {req_rate:.1f} req/s | 0.00 MB/s")

    # Final stats
    elapsed = time.time() - start_time
    logger.info(
        f"OK: {global_stats['ok']} | ERRORES: {global_stats['errors']} | "
        f"Log: {output_dir / 'trades_download.log'}"
    )


if __name__ == "__main__":
    main()