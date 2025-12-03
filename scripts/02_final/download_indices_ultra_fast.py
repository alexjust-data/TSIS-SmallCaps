#!/usr/bin/env python3
"""
Download Indices/ETFs ULTRA-FAST - Alta concurrencia con detección de rango
Target: Descargar 1m data para 34 tickers en paralelo
"""

import asyncio
import aiohttp
import polars as pl
from pathlib import Path
import os
import sys
import json
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict
import time
import calendar

# Configuración
DEFAULT_CONCURRENT = 50
BATCH_SIZE = 100

# Tickers
INDICES = ["I:NDX", "I:COMP", "I:SOX"]
ETFS = [
    "SPY", "QQQ", "IWM", "DIA", "VIXY", "VXX", "UVXY",
    "TLT", "HYG", "LQD",
    "XLK", "XLF", "XLE", "XLV", "XLI", "XLP", "XLY", "XLB", "XLRE", "XLU", "XLC",
    "GLD", "SLV", "UUP", "FXE", "USO", "UNG",
    "SPSM", "VB", "EFA", "EEM"
]


class UltraFastIndicesDownloader:
    def __init__(self, api_key: str, output_dir: Path, max_concurrent: int = DEFAULT_CONCURRENT):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io"
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)

        self.session = None

        # Cache de rangos de tickers
        self.ticker_ranges: Dict[str, dict] = {}
        self.ranges_file = self.output_dir / "ticker_ranges.json"
        self.load_ranges_cache()

        # Stats
        self.stats = {
            'completed': 0,
            'errors': 0,
            'skipped': 0,
            'no_data': 0,
            'total_rows': 0,
            'start_time': None
        }

    def load_ranges_cache(self):
        """Carga cache de rangos de tickers"""
        if self.ranges_file.exists():
            try:
                with open(self.ranges_file, 'r') as f:
                    self.ticker_ranges = json.load(f)
                    print(f"Cache de rangos cargado: {len(self.ticker_ranges)} tickers")
            except:
                self.ticker_ranges = {}

    def save_ranges_cache(self):
        """Guarda cache de rangos"""
        with open(self.ranges_file, 'w') as f:
            json.dump(self.ticker_ranges, f, indent=2)

    async def init_session(self):
        """Session optimizada para alta concurrencia"""
        connector = aiohttp.TCPConnector(
            limit=200,
            limit_per_host=100,
            ttl_dns_cache=3600,
            enable_cleanup_closed=False,
            force_close=False,
            keepalive_timeout=120
        )

        timeout = aiohttp.ClientTimeout(
            total=60,
            connect=5,
            sock_read=30
        )

        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'Connection': 'keep-alive',
                'Accept-Encoding': 'gzip, deflate'
            }
        )

    async def get_ticker_range(self, ticker: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Obtiene el rango de datos disponible para un ticker.
        Usa cache si existe, sino consulta la API.
        """
        # Si ya está en cache, usar eso
        if ticker in self.ticker_ranges:
            cached = self.ticker_ranges[ticker]
            if cached.get("start") and cached.get("end"):
                return cached["start"], cached["end"]
            elif cached.get("error"):
                return None, None

        print(f"  Detectando rango para {ticker}...")

        # Buscar fecha más antigua y más reciente
        url = f"{self.base_url}/v2/aggs/ticker/{ticker}/range/1/day/1990-01-01/2025-12-31"

        first_date = None
        last_date = None

        try:
            # Primera fecha (más antigua)
            params = {"adjusted": "true", "sort": "asc", "limit": 1, "apiKey": self.api_key}
            async with self.session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("results"):
                        first_timestamp = data["results"][0]["t"]
                        first_date = datetime.fromtimestamp(first_timestamp/1000).strftime("%Y-%m-%d")

            # Última fecha (más reciente)
            params["sort"] = "desc"
            async with self.session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("results"):
                        last_timestamp = data["results"][0]["t"]
                        last_date = datetime.fromtimestamp(last_timestamp/1000).strftime("%Y-%m-%d")

            if first_date and last_date:
                print(f"    {ticker}: {first_date} -> {last_date}")
                self.ticker_ranges[ticker] = {
                    "start": first_date,
                    "end": last_date,
                    "detected_at": datetime.now().isoformat()
                }
                self.save_ranges_cache()
                return first_date, last_date
            else:
                print(f"    {ticker}: Sin datos disponibles")
                self.ticker_ranges[ticker] = {"start": None, "end": None, "error": "no_data"}
                self.save_ranges_cache()

        except Exception as e:
            print(f"    Error detectando rango para {ticker}: {e}")
            self.ticker_ranges[ticker] = {"start": None, "end": None, "error": str(e)}

        return None, None

    async def fetch_month(self, ticker: str, year: int, month: int, timespan: str = "minute") -> Tuple[str, int, int, Optional[pl.DataFrame]]:
        """Descarga un mes de datos para un ticker"""

        # Calcular rango del mes
        start_date = f"{year}-{month:02d}-01"
        last_day = calendar.monthrange(year, month)[1]
        end_date = f"{year}-{month:02d}-{last_day}"

        url = f"{self.base_url}/v2/aggs/ticker/{ticker}/range/1/{timespan}/{start_date}/{end_date}"
        params = {
            'adjusted': 'true',
            'sort': 'asc',
            'limit': 50000,
            'apiKey': self.api_key
        }

        all_results = []

        async with self.semaphore:
            try:
                async with self.session.get(url, params=params) as resp:
                    if resp.status == 429:
                        # Rate limit - esperar y reintentar
                        wait = int(resp.headers.get('X-Polygon-Retry-After', '1'))
                        await asyncio.sleep(wait)
                        return (ticker, year, month, None)

                    if resp.status != 200:
                        print(f"    HTTP {resp.status}: {ticker} {year}-{month:02d}")
                        return (ticker, year, month, None)

                    data = await resp.json()
                    results = data.get('results', [])

                    if not results:
                        return (ticker, year, month, pl.DataFrame())

                    all_results.extend(results)

                    # Paginación
                    next_url = data.get('next_url')
                    pages = 1
                    while next_url and pages < 10:
                        if 'apiKey=' not in next_url:
                            next_url += f"&apiKey={self.api_key}"

                        async with self.session.get(next_url) as page_resp:
                            if page_resp.status != 200:
                                break
                            page_data = await page_resp.json()
                            page_results = page_data.get('results', [])
                            if not page_results:
                                break
                            all_results.extend(page_results)
                            next_url = page_data.get('next_url')
                            pages += 1

            except asyncio.TimeoutError:
                print(f"    TIMEOUT: {ticker} {year}-{month:02d}")
                return (ticker, year, month, None)
            except Exception as e:
                print(f"    ERROR: {ticker} {year}-{month:02d}: {e}")
                return (ticker, year, month, None)

        if all_results:
            df = pl.DataFrame(all_results)

            # Procesar
            if 't' in df.columns:
                df = df.with_columns([
                    (pl.col('t') * 1000000).cast(pl.Datetime('ns')).alias('timestamp'),
                ])

                rename_map = {'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume', 'vw': 'vwap', 'n': 'trades'}
                for old, new in rename_map.items():
                    if old in df.columns:
                        df = df.rename({old: new})

                # Seleccionar columnas
                cols = ['timestamp', 'open', 'high', 'low', 'close']
                if 'volume' in df.columns:
                    cols.append('volume')
                if 'vwap' in df.columns:
                    cols.append('vwap')
                df = df.select(cols)

            return (ticker, year, month, df)

        return (ticker, year, month, pl.DataFrame())

    def generate_tasks_for_ticker(self, ticker: str, start_date: str, end_date: str) -> List[Tuple[str, int, int]]:
        """Genera tareas (ticker, year, month) solo para el rango válido del ticker"""
        tasks = []

        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        current = start.replace(day=1)
        while current <= end:
            tasks.append((ticker, current.year, current.month))
            # Siguiente mes
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)

        return tasks

    async def download_all(self, tickers: List[str], start_year: int, end_year: int, timespan: str = "minute", force: bool = False):
        """Descarga masiva en paralelo con detección de rango"""

        self.stats['start_time'] = time.time()

        print("=" * 70)
        print("DESCARGA ULTRA-RAPIDA DE INDICES/ETFs")
        print("=" * 70)
        print(f"Tickers solicitados: {len(tickers)}")
        print(f"Periodo solicitado: {start_year} - {end_year}")
        print(f"Timespan: {timespan}")
        print(f"Concurrencia: {self.max_concurrent}")
        print("=" * 70)

        # Fase 1: Detectar rangos de todos los tickers
        print("\nFASE 1: Detectando rangos disponibles...")
        print("-" * 70)

        valid_tickers = []
        for ticker in tickers:
            ticker_start, ticker_end = await self.get_ticker_range(ticker)
            if ticker_start and ticker_end:
                valid_tickers.append((ticker, ticker_start, ticker_end))
            await asyncio.sleep(0.1)  # Pequeña pausa entre detecciones

        print(f"\nTickers con datos: {len(valid_tickers)}/{len(tickers)}")

        if not valid_tickers:
            print("No hay tickers válidos para descargar")
            return

        # Fase 2: Generar tareas solo para rangos válidos
        print("\nFASE 2: Generando tareas...")
        print("-" * 70)

        all_tasks = []
        ticker_data = {}

        for ticker, ticker_start, ticker_end in valid_tickers:
            # Ajustar al rango solicitado
            effective_start = max(ticker_start, f"{start_year}-01-01")
            effective_end = min(ticker_end, f"{end_year}-12-31")

            if effective_start <= effective_end:
                tasks = self.generate_tasks_for_ticker(ticker, effective_start, effective_end)
                all_tasks.extend(tasks)
                ticker_data[ticker] = []
                print(f"  {ticker}: {len(tasks)} meses ({effective_start} -> {effective_end})")

        total_tasks = len(all_tasks)
        print(f"\nTotal tareas: {total_tasks:,}")

        # Fase 3: Descargar en paralelo
        print("\nFASE 3: Descargando...")
        print("=" * 70)

        # Procesar en batches
        for i in range(0, total_tasks, BATCH_SIZE):
            batch = all_tasks[i:i + BATCH_SIZE]

            # Crear tareas async
            coros = [self.fetch_month(t, y, m, timespan) for t, y, m in batch]

            # Ejecutar en paralelo
            results = await asyncio.gather(*coros, return_exceptions=True)

            # Procesar resultados
            for result in results:
                if isinstance(result, tuple):
                    ticker, year, month, df = result
                    if df is not None and not df.is_empty():
                        ticker_data[ticker].append(df)
                        self.stats['total_rows'] += len(df)
                        self.stats['completed'] += 1
                    elif df is not None:
                        self.stats['no_data'] += 1
                    else:
                        self.stats['errors'] += 1
                else:
                    self.stats['errors'] += 1

            # Progress
            elapsed = time.time() - self.stats['start_time']
            done = self.stats['completed'] + self.stats['no_data'] + self.stats['errors']
            rate = done / elapsed if elapsed > 0 else 0

            print(f"[{datetime.now():%H:%M:%S}] "
                  f"Progress: {done}/{total_tasks} ({done/total_tasks*100:.1f}%) | "
                  f"Rate: {rate:.1f}/s | "
                  f"Rows: {self.stats['total_rows']:,} | "
                  f"NoData: {self.stats['no_data']} | "
                  f"Errors: {self.stats['errors']}")

        # Guardar archivos consolidados por ticker
        print("\n" + "=" * 70)
        print("GUARDANDO ARCHIVOS...")
        print("=" * 70)

        for ticker in ticker_data:
            if ticker_data[ticker]:
                # Combinar todos los meses (con schema flexible)
                try:
                    combined = pl.concat(ticker_data[ticker], how="diagonal_relaxed")
                    if 'timestamp' in combined.columns:
                        combined = combined.unique(subset=['timestamp']).sort('timestamp')

                    # Guardar
                    category = "indices" if ticker.startswith("I:") else "etfs"
                    safe_ticker = ticker.replace(":", "_")
                    out_file = self.output_dir / category / safe_ticker / f"{timespan}.parquet"
                    out_file.parent.mkdir(parents=True, exist_ok=True)
                    combined.write_parquet(out_file, compression='zstd', compression_level=3)

                    print(f"  {ticker}: {len(combined):,} rows -> {out_file}")
                except Exception as e:
                    print(f"  ERROR guardando {ticker}: {e}")

        # Stats finales
        elapsed = time.time() - self.stats['start_time']
        print("\n" + "=" * 70)
        print("COMPLETADO")
        print("=" * 70)
        print(f"Tiempo: {elapsed/60:.1f} minutos")
        print(f"Meses con datos: {self.stats['completed']:,}")
        print(f"Meses sin datos: {self.stats['no_data']:,}")
        print(f"Errores: {self.stats['errors']:,}")
        print(f"Total rows: {self.stats['total_rows']:,}")
        if elapsed > 0:
            print(f"Velocidad: {(self.stats['completed'] + self.stats['no_data'])/elapsed:.1f} tareas/s")
        print("=" * 70)

    async def close(self):
        if self.session:
            await self.session.close()


async def main():
    import argparse

    parser = argparse.ArgumentParser(description='Download Indices/ETFs ULTRA-FAST')
    parser.add_argument('--output', default='C:/TSIS_Data/regime_indicators')
    parser.add_argument('--start-year', type=int, default=2004)
    parser.add_argument('--end-year', type=int, default=2025)
    parser.add_argument('--timespan', choices=['minute', 'day'], default='minute')
    parser.add_argument('--concurrent', type=int, default=DEFAULT_CONCURRENT)
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--indices-only', action='store_true')
    parser.add_argument('--etfs-only', action='store_true')

    args = parser.parse_args()

    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        print("ERROR: POLYGON_API_KEY no configurada")
        sys.exit(1)

    # Seleccionar tickers
    if args.indices_only:
        tickers = INDICES
    elif args.etfs_only:
        tickers = ETFS
    else:
        tickers = INDICES + ETFS

    downloader = UltraFastIndicesDownloader(
        api_key=api_key,
        output_dir=Path(args.output),
        max_concurrent=args.concurrent
    )

    try:
        await downloader.init_session()
        await downloader.download_all(
            tickers=tickers,
            start_year=args.start_year,
            end_year=args.end_year,
            timespan=args.timespan,
            force=args.force
        )
    finally:
        await downloader.close()


if __name__ == '__main__':
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())
