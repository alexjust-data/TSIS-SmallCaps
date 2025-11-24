#!/usr/bin/env python3
"""
Descarga quotes (bid/ask NBBO) desde Polygon para todos los tickers.
Estructura: {outdir}/{TICKER}/year={YYYY}/month={MM}/day={YYYY-MM-DD}/quotes.parquet
"""

import polars as pl
import sys
import os
import argparse
import time
from pathlib import Path
from datetime import datetime
from polygon import RESTClient
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)

def download_quotes_for_date(client, ticker, date, outdir, max_retries=3):
    """
    Descarga quotes (NBBO bid/ask) para un ticker en una fecha específica.
    Retorna True si exitoso, False si error.
    Incluye reintentos con backoff exponencial para manejar timeouts.
    """
    year, month, _ = date.split('-')

    # Path de salida
    output_path = Path(outdir) / ticker / f"year={year}" / f"month={month}" / f"day={date}"
    quotes_file = output_path / "quotes.parquet"

    # Skip si ya existe
    if quotes_file.exists():
        return True

    for attempt in range(max_retries):
        try:
            # Descargar quotes (market hours: 09:30-16:00 ET)
            quotes = []
            for quote in client.list_quotes(
                ticker,
                limit=50000,
                timestamp_gte=date + "T09:30:00-05:00",
                timestamp_lt=date + "T16:00:00-05:00"
            ):
                quotes.append({
                    'timestamp': quote.sip_timestamp,
                    'bid_price': quote.bid_price,
                    'bid_size': quote.bid_size,
                    'bid_exchange': quote.bid_exchange,
                    'ask_price': quote.ask_price,
                    'ask_size': quote.ask_size,
                    'ask_exchange': quote.ask_exchange,
                    'conditions': ','.join(str(c) for c in quote.conditions) if quote.conditions else '',
                })

            if not quotes:
                # Sin quotes, crear archivo vacío
                output_path.mkdir(parents=True, exist_ok=True)
                df = pl.DataFrame({
                    'timestamp': [],
                    'bid_price': [],
                    'bid_size': [],
                    'bid_exchange': [],
                    'ask_price': [],
                    'ask_size': [],
                    'ask_exchange': [],
                    'conditions': []
                })
                df.write_parquet(quotes_file)
                return True

            # Guardar
            output_path.mkdir(parents=True, exist_ok=True)
            df = pl.DataFrame(quotes)
            df.write_parquet(quotes_file)

            return True

        except Exception as e:
            if attempt < max_retries - 1:
                # Backoff exponencial: 2, 4, 8 segundos
                wait_time = 2 ** (attempt + 1)
                time.sleep(wait_time)
            else:
                log(f"  ERROR {ticker} {date}: {e}")
                return False

    return False

def main():
    parser = argparse.ArgumentParser(description='Descargar quotes (bid/ask) desde CSV de fechas')
    parser.add_argument('--dates-csv', required=True, help='CSV con columnas ticker,date (todas las fechas a descargar)')
    parser.add_argument('--outdir', required=True, help='Directorio de salida (ej: C:\\TSIS_Data\\quotes_ticks_2004_2018)')
    parser.add_argument('--api-key', help='Polygon API key (o usar POLYGON_API_KEY env var)')
    parser.add_argument('--workers', type=int, default=10, help='Número de workers paralelos (default: 10)')
    parser.add_argument('--limit', type=int, help='Limitar a N fechas (para testing)')
    parser.add_argument('--delay', type=float, default=0.12, help='Delay entre requests en segundos (default: 0.12)')

    args = parser.parse_args()

    # API key
    api_key = args.api_key or os.getenv('POLYGON_API_KEY')
    if not api_key:
        log("ERROR: Necesitas --api-key o variable de entorno POLYGON_API_KEY")
        sys.exit(1)

    client = RESTClient(api_key)

    log("=" * 80)
    log("DESCARGA DE QUOTES (BID/ASK NBBO)")
    log("=" * 80)

    # Cargar CSV de fechas
    log(f"Cargando fechas desde {args.dates_csv}")
    df = pl.read_csv(args.dates_csv)

    # Normalizar nombre de columna de fecha
    if 'missing_date' in df.columns:
        df = df.rename({'missing_date': 'date'})

    if args.limit:
        df = df.head(args.limit)
        log(f"  Limitado a {args.limit} fechas para testing")

    total_dates = len(df)
    log(f"  Total fechas a descargar: {total_dates:,}")

    # Contar tickers únicos
    unique_tickers = df.select('ticker').unique().height
    log(f"  Tickers únicos: {unique_tickers:,}")

    # Agrupar por ticker para mostrar progreso
    ticker_groups = df.group_by('ticker').agg(pl.len().alias('count')).sort('count', descending=True)
    log(f"  Ticker con más fechas: {ticker_groups[0, 'ticker']} ({ticker_groups[0, 'count']} días)")

    log("")
    log(f"Comenzando descarga paralela con {args.workers} workers...")

    # Thread-safe counters
    lock = threading.Lock()
    success_count = 0
    error_count = 0
    completed_count = 0
    start_time = time.time()

    def download_wrapper(task_data):
        """Wrapper para ejecutar descarga en thread pool"""
        api_key, ticker, date, outdir = task_data
        # Cada worker necesita su propio client
        worker_client = RESTClient(api_key)
        success = download_quotes_for_date(worker_client, ticker, date, outdir)
        return (ticker, date, success)

    # Preparar tasks
    tasks = [(api_key, row['ticker'], row['date'], args.outdir)
             for row in df.iter_rows(named=True)]

    # Ejecutar en paralelo
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(download_wrapper, task): i for i, task in enumerate(tasks)}

        for future in as_completed(futures):
            ticker, date, success = future.result()

            with lock:
                completed_count += 1
                if success:
                    success_count += 1
                else:
                    error_count += 1

                # Progreso cada 100 completados
                if completed_count % 100 == 0 or completed_count == 1:
                    elapsed = time.time() - start_time
                    rate = completed_count / elapsed if elapsed > 0 else 0
                    remaining = (total_dates - completed_count) / rate if rate > 0 else 0
                    log(f"  Progreso: {completed_count}/{total_dates} ({completed_count/total_dates*100:.1f}%) | "
                        f"Rate: {rate:.1f} req/s | "
                        f"ETA: {remaining/60:.1f} min | "
                        f"Success: {success_count} | Errors: {error_count}")

            # Rate limiting (delay distribuido entre workers)
            time.sleep(args.delay / args.workers)

    # Resumen final
    elapsed = time.time() - start_time
    log("")
    log("=" * 80)
    log("DESCARGA COMPLETADA")
    log("=" * 80)
    log(f"Total fechas procesadas: {total_dates:,}")
    log(f"  Exitosas: {success_count:,} ({success_count/total_dates*100:.1f}%)")
    log(f"  Errores: {error_count:,} ({error_count/total_dates*100:.1f}%)")
    log(f"Tiempo total: {elapsed/60:.1f} minutos")
    log(f"Velocidad promedio: {total_dates/elapsed:.1f} fechas/segundo")
    log("")

if __name__ == '__main__':
    main()
