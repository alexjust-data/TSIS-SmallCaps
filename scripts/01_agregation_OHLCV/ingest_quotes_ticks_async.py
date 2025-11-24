#!/usr/bin/env python3
"""
Descarga OPTIMIZADA de quotes (bid/ask NBBO) desde Polygon.
Maximiza throughput usando todas las capacidades del API.
"""

import polars as pl
import sys
import os
import argparse
import time
import asyncio
import aiohttp
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional
import json
from dataclasses import dataclass
import queue
import threading
from concurrent.futures import ThreadPoolExecutor
import backoff

# ==========================================
# CONFIGURACIÓN Y LOGGING
# ==========================================

def log(msg, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"[{timestamp}] [{level}] {msg}", flush=True)

@dataclass
class DownloadTask:
    ticker: str
    date: str
    output_path: Path

@dataclass
class DownloadResult:
    task: DownloadTask
    success: bool
    quotes_count: int
    error: Optional[str] = None

# ==========================================
# DESCARGA ASÍNCRONA CON POLYGON API
# ==========================================

class PolygonQuotesDownloader:
    def __init__(self, api_key: str, max_concurrent: int = 50):
        self.api_key = api_key
        self.max_concurrent = max_concurrent
        self.base_url = "https://api.polygon.io/v3/quotes"
        self.semaphore = asyncio.Semaphore(max_concurrent)

        # Métricas
        self.total_requests = 0
        self.total_quotes = 0
        self.start_time = None

    async def fetch_quotes_page(self, session: aiohttp.ClientSession, ticker: str, date: str,
                                next_url: Optional[str] = None) -> Dict[str, Any]:
        """Descarga una página de quotes"""

        if next_url:
            url = next_url
        else:
            # Primera página - usar timestamp range para market hours (9:30 AM - 4:00 PM ET)
            url = f"{self.base_url}/{ticker}"
            params = {
                'timestamp.gte': f'{date}T09:30:00-05:00',
                'timestamp.lt': f'{date}T16:00:00-05:00',
                'limit': 50000,  # Máximo permitido
                'apiKey': self.api_key,
                'order': 'asc'
            }
            url = f"{url}?{'&'.join(f'{k}={v}' for k, v in params.items())}"

        @backoff.on_exception(
            backoff.expo,
            (aiohttp.ClientError, asyncio.TimeoutError),
            max_tries=5,
            max_time=60
        )
        async def make_request():
            async with self.semaphore:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 429:  # Rate limited
                        retry_after = int(response.headers.get('X-Polygon-Retry-After', '5'))
                        await asyncio.sleep(retry_after)
                        raise aiohttp.ClientError("Rate limited")

                    response.raise_for_status()
                    self.total_requests += 1
                    return await response.json()

        return await make_request()

    async def download_all_quotes(self, session: aiohttp.ClientSession, ticker: str, date: str) -> List[Dict]:
        """Descarga TODAS las páginas de quotes para un ticker/fecha"""

        all_quotes = []
        next_url = None
        page = 1

        while True:
            try:
                data = await self.fetch_quotes_page(session, ticker, date, next_url)

                # Extraer quotes de esta página
                quotes = data.get('results', [])
                if quotes:
                    all_quotes.extend(quotes)
                    self.total_quotes += len(quotes)

                # Verificar si hay más páginas
                next_url = data.get('next_url')
                if not next_url:
                    break

                # Agregar API key a next_url si no está presente
                if 'apiKey=' not in next_url:
                    separator = '&' if '?' in next_url else '?'
                    next_url = f"{next_url}{separator}apiKey={self.api_key}"

                page += 1

                # Log cada 10 páginas
                if page % 10 == 0:
                    log(f"  {ticker} {date}: Página {page}, {len(all_quotes):,} quotes hasta ahora")

            except Exception as e:
                log(f"Error descargando {ticker} {date} página {page}: {e}", "ERROR")
                break

        return all_quotes

    async def process_task(self, session: aiohttp.ClientSession, task: DownloadTask) -> DownloadResult:
        """Procesa una tarea de descarga"""

        # Verificar si ya existe
        quotes_file = task.output_path / "quotes.parquet"
        if quotes_file.exists():
            return DownloadResult(task, True, 0)

        try:
            # Descargar todos los quotes
            quotes = await self.download_all_quotes(session, task.ticker, task.date)

            if not quotes:
                # Sin datos - crear archivo vacío
                task.output_path.mkdir(parents=True, exist_ok=True)
                df = pl.DataFrame({
                    'timestamp': [],
                    'bid_price': [],
                    'bid_size': [],
                    'bid_exchange': [],
                    'ask_price': [],
                    'ask_size': [],
                    'ask_exchange': [],
                    'conditions': [],
                    'tape': [],
                    'sequence_number': []
                })
                df.write_parquet(quotes_file)
                return DownloadResult(task, True, 0)

            # Convertir a DataFrame
            processed_quotes = []
            for q in quotes:
                processed_quotes.append({
                    'timestamp': q.get('sip_timestamp', q.get('participant_timestamp')),
                    'bid_price': q.get('bid_price', 0),
                    'bid_size': q.get('bid_size', 0),
                    'bid_exchange': q.get('bid_exchange', 0),
                    'ask_price': q.get('ask_price', 0),
                    'ask_size': q.get('ask_size', 0),
                    'ask_exchange': q.get('ask_exchange', 0),
                    'conditions': ','.join(str(c) for c in q.get('conditions', [])),
                    'tape': q.get('tape', ''),
                    'sequence_number': q.get('sequence_number', 0)
                })

            # Guardar
            task.output_path.mkdir(parents=True, exist_ok=True)
            df = pl.DataFrame(processed_quotes)

            # Ordenar por timestamp
            df = df.sort('timestamp')

            # Guardar con compresión optimizada
            df.write_parquet(quotes_file, compression='zstd', compression_level=3)

            return DownloadResult(task, True, len(quotes))

        except Exception as e:
            log(f"Error procesando {task.ticker} {task.date}: {e}", "ERROR")
            return DownloadResult(task, False, 0, str(e))

    async def run_batch(self, tasks: List[DownloadTask]) -> List[DownloadResult]:
        """Ejecuta un batch de tareas"""

        self.start_time = time.time()

        # Crear sesión HTTP con connection pooling
        connector = aiohttp.TCPConnector(
            limit=100,  # Total connections
            limit_per_host=50,  # Per host
            ttl_dns_cache=300
        )

        async with aiohttp.ClientSession(connector=connector) as session:
            # Procesar tareas en paralelo
            results = await asyncio.gather(
                *[self.process_task(session, task) for task in tasks],
                return_exceptions=True
            )

            # Convertir excepciones a resultados de error
            final_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    final_results.append(
                        DownloadResult(tasks[i], False, 0, str(result))
                    )
                else:
                    final_results.append(result)

            return final_results

# ==========================================
# PROCESAMIENTO PARALELO DE ARCHIVOS
# ==========================================

class ParallelFileWriter:
    """Escribe archivos en paralelo para no bloquear descargas"""

    def __init__(self, num_workers: int = 4):
        self.queue = queue.Queue()
        self.num_workers = num_workers
        self.workers = []
        self.running = True

        # Iniciar workers
        for _ in range(num_workers):
            worker = threading.Thread(target=self._worker)
            worker.start()
            self.workers.append(worker)

    def _worker(self):
        """Worker thread para escribir archivos"""
        while self.running:
            try:
                item = self.queue.get(timeout=1)
                if item is None:
                    break

                df, path = item
                path.parent.mkdir(parents=True, exist_ok=True)
                df.write_parquet(path, compression='zstd', compression_level=3)

            except queue.Empty:
                continue
            except Exception as e:
                log(f"Error escribiendo archivo: {e}", "ERROR")

    def write(self, df: pl.DataFrame, path: Path):
        """Agrega archivo a la cola de escritura"""
        self.queue.put((df, path))

    def shutdown(self):
        """Apaga los workers"""
        self.running = False
        for _ in range(self.num_workers):
            self.queue.put(None)
        for worker in self.workers:
            worker.join()

# ==========================================
# MAIN
# ==========================================

async def main_async():
    parser = argparse.ArgumentParser(description='Descarga OPTIMIZADA de quotes desde Polygon')
    parser.add_argument('--dates-csv', required=True, help='CSV con columnas ticker,date')
    parser.add_argument('--outdir', required=True, help='Directorio de salida')
    parser.add_argument('--api-key', help='Polygon API key (o usar POLYGON_API_KEY env var)')
    parser.add_argument('--concurrent', type=int, default=30, help='Requests concurrentes (default: 30)')
    parser.add_argument('--batch-size', type=int, default=1000, help='Tamaño de batch (default: 1000)')
    parser.add_argument('--limit', type=int, help='Limitar a N fechas (para testing)')
    parser.add_argument('--skip-existing', action='store_true', help='Saltar archivos existentes')

    args = parser.parse_args()

    # API key
    api_key = args.api_key or os.getenv('POLYGON_API_KEY')
    if not api_key:
        log("ERROR: Necesitas --api-key o variable de entorno POLYGON_API_KEY", "ERROR")
        sys.exit(1)

    log("=" * 80)
    log("DESCARGA OPTIMIZADA DE QUOTES (BID/ASK NBBO)")
    log("=" * 80)

    # Cargar CSV
    log(f"Cargando fechas desde {args.dates_csv}")
    df = pl.read_csv(args.dates_csv)

    # Normalizar columnas
    if 'missing_date' in df.columns:
        df = df.rename({'missing_date': 'date'})

    if args.limit:
        df = df.head(args.limit)
        log(f"  Limitado a {args.limit} fechas para testing")

    total_dates = len(df)
    log(f"  Total fechas a descargar: {total_dates:,}")

    # Estadísticas
    unique_tickers = df.select('ticker').unique().height
    log(f"  Tickers únicos: {unique_tickers:,}")

    # Crear tareas
    tasks = []
    for row in df.iter_rows(named=True):
        ticker = row['ticker']
        date = row['date']
        year, month, _ = date.split('-')

        output_path = Path(args.outdir) / ticker / f"year={year}" / f"month={month}" / f"day={date}"

        # Skip si existe y flag activo
        if args.skip_existing and (output_path / "quotes.parquet").exists():
            continue

        tasks.append(DownloadTask(ticker, date, output_path))

    log(f"  Tareas a procesar: {len(tasks):,}")

    if not tasks:
        log("No hay tareas pendientes")
        return

    # Inicializar downloader
    downloader = PolygonQuotesDownloader(api_key, args.concurrent)

    # Procesar en batches
    log("")
    log(f"Comenzando descarga con {args.concurrent} conexiones concurrentes...")
    log(f"Procesando en batches de {args.batch_size}")

    all_results = []
    start_time = time.time()

    for i in range(0, len(tasks), args.batch_size):
        batch = tasks[i:i + args.batch_size]
        batch_num = (i // args.batch_size) + 1
        total_batches = (len(tasks) + args.batch_size - 1) // args.batch_size

        log(f"\nBatch {batch_num}/{total_batches} ({len(batch)} tareas)")

        # Procesar batch
        results = await downloader.run_batch(batch)
        all_results.extend(results)

        # Estadísticas del batch
        success = sum(1 for r in results if r.success)
        total_quotes = sum(r.quotes_count for r in results)

        elapsed = time.time() - start_time
        rate = downloader.total_requests / elapsed if elapsed > 0 else 0
        quotes_rate = downloader.total_quotes / elapsed if elapsed > 0 else 0

        log(f"  Batch completado: {success}/{len(batch)} exitosas")
        log(f"  Quotes descargados en batch: {total_quotes:,}")
        log(f"  Total requests: {downloader.total_requests:,} ({rate:.1f} req/s)")
        log(f"  Total quotes: {downloader.total_quotes:,} ({quotes_rate:.0f} quotes/s)")

        # Estimación de tiempo restante
        if i + args.batch_size < len(tasks):
            remaining_tasks = len(tasks) - (i + args.batch_size)
            eta_seconds = remaining_tasks * (elapsed / (i + args.batch_size))
            log(f"  ETA: {eta_seconds/60:.1f} minutos")

    # Resumen final
    elapsed = time.time() - start_time
    success_count = sum(1 for r in all_results if r.success)
    error_count = sum(1 for r in all_results if not r.success)
    total_quotes_downloaded = sum(r.quotes_count for r in all_results)

    log("")
    log("=" * 80)
    log("DESCARGA COMPLETADA")
    log("=" * 80)
    log(f"Total tareas procesadas: {len(all_results):,}")
    log(f"  Exitosas: {success_count:,} ({success_count/len(all_results)*100:.1f}%)")
    log(f"  Errores: {error_count:,} ({error_count/len(all_results)*100:.1f}%)")
    log(f"Total quotes descargados: {total_quotes_downloaded:,}")
    log(f"Tiempo total: {elapsed/60:.1f} minutos")
    log(f"Velocidad promedio:")
    log(f"  Tareas/segundo: {len(all_results)/elapsed:.1f}")
    log(f"  Requests/segundo: {downloader.total_requests/elapsed:.1f}")
    log(f"  Quotes/segundo: {downloader.total_quotes/elapsed:.0f}")

    # Listar errores si hay
    if error_count > 0:
        log("\nErrores encontrados:")
        for r in all_results:
            if not r.success and r.error:
                log(f"  {r.task.ticker} {r.task.date}: {r.error}")

def main():
    """Entry point sincrónico"""
    asyncio.run(main_async())

if __name__ == '__main__':
    main()
