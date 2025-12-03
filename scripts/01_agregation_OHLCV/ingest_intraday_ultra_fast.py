#!/usr/bin/env python3
"""
ingest_intraday_ultra_fast.py - Versión ULTRA-OPTIMIZADA

Mejoras sobre el script original:
1. Descarga paralela de MESES (no secuencial)
2. Cache inteligente de meses vacíos
3. Compresión asíncrona en threads
4. Skip inteligente basado en volumen daily
5. Priorización de meses recientes
"""

import asyncio
import aiohttp
import polars as pl
from pathlib import Path
import os
import sys
import json
from datetime import datetime, date, timedelta
from typing import Dict, List, Set, Optional, Tuple
import time
from concurrent.futures import ThreadPoolExecutor
import backoff

class UltraFastIntradayDownloader:
    def __init__(self, api_key: str, outdir: Path, daily_dir: Path = None, max_concurrent: int = 50):
        self.api_key = api_key
        self.outdir = outdir
        self.daily_dir = daily_dir  # Para skip inteligente
        self.base_url = "https://api.polygon.io"
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        # Sesión HTTP
        self.session = None
        
        # Cache y checkpoint
        self.cache_file = outdir / ".cache_intraday.json"
        self.empty_months_cache = self.load_cache()
        
        # Thread pool para compresión
        self.compression_executor = ThreadPoolExecutor(max_workers=8)  # Más threads
        
        # Métricas
        self.stats = {
            'total_pages': 0,
            'total_rows': 0,
            'skipped_months': 0,
            'errors': 0,
            'start_time': None
        }
    
    def load_cache(self) -> Dict:
        """Carga cache con metadata"""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r') as f:
                    data = json.load(f)
                    # Convertir listas a sets
                    return {
                        'empty': set(data.get('empty', [])),
                        'low_volume': set(data.get('low_volume', []))
                    }
            except:
                return {'empty': set(), 'low_volume': set()}
        return {'empty': set(), 'low_volume': set()}
    
    def save_cache(self):
        """Guarda cache"""
        cache_data = {
            'empty': list(self.empty_months_cache.get('empty', set())),
            'low_volume': list(self.empty_months_cache.get('low_volume', set())),
            'timestamp': datetime.now().isoformat()
        }
        with open(self.cache_file, 'w') as f:
            json.dump(cache_data, f)
    
    def should_skip_month(self, ticker: str, year: int, month: int) -> bool:
        """
        Determina si debemos saltar un mes basado en:
        1. Cache de meses vacíos
        2. Volumen daily (si disponible)
        """
        cache_key = f"{ticker}_{year:04d}_{month:02d}"
        
        # Check cache
        if cache_key in self.empty_months_cache.get('empty', set()):
            self.stats['skipped_months'] += 1
            return True
        
        if cache_key in self.empty_months_cache.get('low_volume', set()):
            self.stats['skipped_months'] += 1
            return True
        
        # Check volumen daily si tenemos acceso
        if self.daily_dir:
            daily_file = self.daily_dir / ticker / f"year={year}" / "daily.parquet"
            if daily_file.exists():
                try:
                    df = pl.read_parquet(daily_file)
                    # Filtrar días del mes
                    month_str = f"{year:04d}-{month:02d}"
                    df_month = df.filter(pl.col('date').str.starts_with(month_str))
                    
                    if df_month.is_empty():
                        self.empty_months_cache.setdefault('empty', set()).add(cache_key)
                        return True

                    # DESACTIVADO: Skip por volumen bajo
                    # Descargamos TODO sin filtrar por volumen
                    # vol_col = 'volume' if 'volume' in df.columns else 'v'
                    # avg_volume = df_month[vol_col].mean()
                    # if avg_volume < 1000:
                    #     self.empty_months_cache.setdefault('low_volume', set()).add(cache_key)
                    #     return True
                except:
                    pass
        
        return False
    
    async def init_session(self):
        """Sesión con configuración ultra-agresiva"""
        connector = aiohttp.TCPConnector(
            limit=300,  # Más conexiones
            limit_per_host=150,
            ttl_dns_cache=3600,
            enable_cleanup_closed=False,  # Menos overhead
            force_close=False,
            keepalive_timeout=120
        )
        
        timeout = aiohttp.ClientTimeout(
            total=45,
            connect=3,
            sock_read=20
        )
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'Authorization': f'Bearer {self.api_key}',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive'
            }
        )
    
    @backoff.on_exception(
        backoff.constant,
        (aiohttp.ClientError, asyncio.TimeoutError),
        max_tries=3,
        interval=1
    )
    async def fetch_month_data_fast(self, ticker: str, year: int, month: int) -> Optional[List[dict]]:
        """Versión rápida sin conversión a DataFrame"""
        
        # Skip inteligente
        if self.should_skip_month(ticker, year, month):
            return None
        
        # Check si ya existe
        output_file = self.outdir / ticker / f"year={year}" / f"month={month:02d}" / "minute.parquet"
        if output_file.exists():
            return None
        
        # Rango del mes
        if month == 12:
            next_year, next_month = year + 1, 1
        else:
            next_year, next_month = year, month + 1

        from_date = f"{year:04d}-{month:02d}-01"
        # Último día del mes = primer día del mes siguiente - 1 día
        to_date = date(next_year, next_month, 1) - timedelta(days=1)
        to_date_str = to_date.strftime("%Y-%m-%d")
        
        url = f"{self.base_url}/v2/aggs/ticker/{ticker}/range/1/minute/{from_date}/{to_date_str}"
        params = {
            'adjusted': 'true',
            'sort': 'asc',
            'limit': 50000
        }
        
        all_results = []
        cursor = None
        pages = 0
        max_pages = 20  # Límite de páginas para no atascarse
        
        async with self.semaphore:
            while pages < max_pages:
                current_params = params.copy()
                if cursor:
                    current_params['cursor'] = cursor
                
                try:
                    async with self.session.get(url, params=current_params) as resp:
                        if resp.status == 429:
                            await asyncio.sleep(2)
                            continue
                        
                        if resp.status in [404, 400]:
                            cache_key = f"{ticker}_{year:04d}_{month:02d}"
                            self.empty_months_cache.setdefault('empty', set()).add(cache_key)
                            return None
                        
                        if resp.status != 200:
                            self.stats['errors'] += 1
                            return all_results if all_results else None
                        
                        data = await resp.json()
                        results = data.get('results', [])
                        
                        if not results and pages == 0:
                            cache_key = f"{ticker}_{year:04d}_{month:02d}"
                            self.empty_months_cache.setdefault('empty', set()).add(cache_key)
                            return None
                        
                        all_results.extend(results)
                        pages += 1
                        self.stats['total_pages'] += 1
                        self.stats['total_rows'] += len(results)
                        
                        # Next page
                        next_url = data.get('next_url')
                        if not next_url:
                            break
                        
                        if 'cursor=' in next_url:
                            cursor = next_url.split('cursor=')[1].split('&')[0]
                        else:
                            break
                            
                except Exception as e:
                    self.stats['errors'] += 1
                    if pages == 0:
                        return None
                    break
        
        return all_results if all_results else None
    
    def save_month_data_sync(self, data: List[dict], ticker: str, year: int, month: int):
        """Guardar datos en thread separado"""
        if not data:
            return
        
        try:
            # Convertir a DataFrame
            df = pl.DataFrame(data)
            
            # Procesar timestamps (Polygon devuelve milisegundos)
            df = df.with_columns([
                pl.from_epoch(pl.col('t'), time_unit='ms').alias('timestamp'),
                pl.lit(ticker).alias('ticker')
            ])
            
            df = df.with_columns([
                pl.col('timestamp').dt.date().alias('date'),
                pl.col('timestamp').dt.strftime('%Y-%m-%d %H:%M').alias('minute')
            ])
            
            # Renombrar OHLCV
            rename_map = {
                'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close',
                'v': 'volume', 'vw': 'vwap', 'n': 'transactions'
            }
            for old, new in rename_map.items():
                if old in df.columns:
                    df = df.rename({old: new})
            
            # Guardar
            output_dir = self.outdir / ticker / f"year={year}" / f"month={month:02d}"
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / "minute.parquet"
            
            df.sort('timestamp').write_parquet(
                output_file,
                compression='zstd',
                compression_level=1,  # Más rápido
                statistics=False,  # Menos overhead
                row_group_size=200000
            )
        except Exception as e:
            print(f"Error guardando {ticker} {year}-{month}: {e}")
    
    async def process_ticker_ultra_fast(self, ticker: str, start_year: int, end_year: int):
        """Procesa ticker con todas las optimizaciones"""
        
        # Generar tareas priorizadas (meses recientes primero)
        tasks = []
        current_year = datetime.now().year
        current_month = datetime.now().month

        for year in range(end_year, start_year - 1, -1):  # Años recientes primero
            start_month = 1
            # Solo limitar al mes actual si estamos en el año actual
            if year == current_year:
                end_month = current_month
            else:
                end_month = 12  # Todos los meses para años pasados

            for month in range(end_month, start_month - 1, -1):  # Meses recientes primero
                tasks.append((year, month))
        
        # Descargar en paralelo
        download_tasks = []
        for year, month in tasks:
            download_tasks.append(
                self.fetch_month_data_fast(ticker, year, month)
            )
        
        results = await asyncio.gather(*download_tasks, return_exceptions=True)
        
        # Guardar en threads paralelos
        save_futures = []
        for (year, month), result in zip(tasks, results):
            if isinstance(result, list) and result:
                future = self.compression_executor.submit(
                    self.save_month_data_sync, result, ticker, year, month
                )
                save_futures.append(future)
        
        # Esperar a que terminen los saves
        for future in save_futures:
            try:
                future.result(timeout=30)
            except:
                pass
    
    async def download_all_ultra_fast(self, tickers: List[str], start_year: int, end_year: int):
        """Descarga masiva ultra-rápida"""
        
        self.stats['start_time'] = time.time()
        total = len(tickers)
        
        print(f"="*60)
        print(f"DESCARGA ULTRA-RÁPIDA DE INTRADAY 1M")
        print(f"="*60)
        print(f"Tickers: {total:,}")
        print(f"Período: {start_year}-{end_year}")
        print(f"Concurrencia: {self.max_concurrent}")
        print(f"Daily dir: {self.daily_dir}")
        
        # Procesar en micro-batches para máxima velocidad
        batch_size = 5  # Batches pequeños pero muchos en paralelo
        
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]
            
            tasks = [
                self.process_ticker_ultra_fast(t, start_year, end_year)
                for t in batch
            ]
            
            await asyncio.gather(*tasks, return_exceptions=True)
            
            # Save cache cada 50 tickers
            if (i + batch_size) % 50 == 0:
                self.save_cache()
            
            # Progress
            done = min(i + batch_size, total)
            elapsed = time.time() - self.stats['start_time']
            rate = done / elapsed if elapsed > 0 else 0
            eta = (total - done) / rate if rate > 0 else 0
            
            if done % 20 == 0 or done == total:
                print(f"[{datetime.now():%H:%M:%S}] {done}/{total} "
                      f"({done/total*100:.1f}%) | "
                      f"{rate:.2f} tickers/s | "
                      f"ETA: {eta/60:.1f}m | "
                      f"Rows: {self.stats['total_rows']/1e6:.1f}M | "
                      f"Skip: {self.stats['skipped_months']}")
    
    async def close(self):
        if self.session:
            await self.session.close()
        self.compression_executor.shutdown(wait=False)  # No esperar

async def main():
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--tickers-csv', required=True)
    parser.add_argument('--outdir', required=True)
    parser.add_argument('--daily-dir', help='Directorio daily para skip inteligente')
    parser.add_argument('--start-year', type=int, default=2019)
    parser.add_argument('--end-year', type=int, default=2025)
    parser.add_argument('--concurrent', type=int, default=50)
    parser.add_argument('--api-key', help='Polygon API key')
    
    args = parser.parse_args()
    
    api_key = args.api_key or os.getenv('POLYGON_API_KEY')
    if not api_key:
        print("ERROR: API key requerida")
        sys.exit(1)
    
    # Cargar tickers
    if args.tickers_csv.endswith('.parquet'):
        df = pl.read_parquet(args.tickers_csv)
    else:
        df = pl.read_csv(args.tickers_csv)
    
    tickers = df['ticker'].unique().to_list()
    print(f"Tickers: {len(tickers)}")
    
    # Daily dir para skip inteligente
    daily_dir = Path(args.daily_dir) if args.daily_dir else None
    
    # Downloader
    downloader = UltraFastIntradayDownloader(
        api_key=api_key,
        outdir=Path(args.outdir),
        daily_dir=daily_dir,
        max_concurrent=args.concurrent
    )
    
    try:
        await downloader.init_session()
        await downloader.download_all_ultra_fast(
            tickers, args.start_year, args.end_year
        )
        
        elapsed = time.time() - downloader.stats['start_time']
        print(f"\n{'='*60}")
        print(f"COMPLETADO en {elapsed/3600:.2f} horas")
        print(f"Páginas: {downloader.stats['total_pages']:,}")
        print(f"Filas: {downloader.stats['total_rows']:,}")
        print(f"Meses saltados: {downloader.stats['skipped_months']:,}")
        print(f"Errores: {downloader.stats['errors']:,}")
        print(f"Velocidad: {downloader.stats['total_rows']/elapsed:.0f} rows/s")
        
    finally:
        await downloader.close()

if __name__ == '__main__':
    asyncio.run(main())
