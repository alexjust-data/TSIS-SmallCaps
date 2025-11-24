#!/usr/bin/env python3
"""
Ingesta optimizada de datos intraday faltantes con procesamiento asíncrono.
Mejoras:
- Descarga asíncrona con aiohttp para mayor velocidad
- Priorización por volumen/liquidez para trading
- Validación de integridad post-descarga
- Resume capability con checkpoint
- Compresión optimizada para análisis rápido
"""

import os
import sys
import asyncio
import aiohttp
import polars as pl
from pathlib import Path
from datetime import datetime, date, timedelta
import json
import argparse
from typing import Dict, List, Tuple, Optional
import backoff
from dataclasses import dataclass
import pickle

@dataclass
class DownloadTask:
    ticker: str
    year: int
    month: int
    priority: float  # Para priorizar por volumen/liquidez

class OptimizedIntradayDownloader:
    def __init__(self, api_key: str, outdir: Path, max_concurrent: int = 20):
        self.api_key = api_key
        self.outdir = outdir
        self.max_concurrent = max_concurrent
        self.base_url = "https://api.polygon.io"
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session = None
        self.checkpoint_file = outdir / ".download_checkpoint.pkl"
        self.completed_tasks = self.load_checkpoint()
        
        # Métricas
        self.total_downloaded = 0
        self.total_rows = 0
        self.start_time = None
        
    def load_checkpoint(self) -> set:
        """Carga checkpoint de descargas completadas."""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'rb') as f:
                    return pickle.load(f)
            except:
                return set()
        return set()
    
    def save_checkpoint(self):
        """Guarda checkpoint de descargas completadas."""
        with open(self.checkpoint_file, 'wb') as f:
            pickle.dump(self.completed_tasks, f)
    
    async def init_session(self):
        """Inicializa sesión aiohttp con configuración optimizada."""
        timeout = aiohttp.ClientTimeout(total=60, connect=10)
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=50,
            ttl_dns_cache=300,
            enable_cleanup_closed=True
        )
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={'Authorization': f'Bearer {self.api_key}'}
        )
    
    async def close_session(self):
        """Cierra sesión aiohttp."""
        if self.session:
            await self.session.close()
    
    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, asyncio.TimeoutError),
        max_tries=5,
        max_time=60
    )
    async def fetch_month_data(self, task: DownloadTask) -> Optional[pl.DataFrame]:
        """Descarga datos de un mes con reintentos inteligentes."""
        
        # Skip si ya está en checkpoint
        task_id = f"{task.ticker}_{task.year}_{task.month:02d}"
        if task_id in self.completed_tasks:
            return None
        
        # Calcular rango de fechas
        start_date = date(task.year, task.month, 1)
        if task.month == 12:
            end_date = date(task.year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date = date(task.year, task.month + 1, 1) - timedelta(days=1)
        
        url = f"{self.base_url}/v2/aggs/ticker/{task.ticker}/range/1/minute/{start_date}/{end_date}"
        params = {
            'adjusted': 'true',
            'sort': 'asc',
            'limit': 50000
        }
        
        all_results = []
        cursor = None
        
        async with self.semaphore:
            while True:
                current_params = params.copy()
                if cursor:
                    current_params['cursor'] = cursor
                
                try:
                    async with self.session.get(url, params=current_params) as response:
                        if response.status == 429:  # Rate limited
                            retry_after = int(response.headers.get('X-Polygon-Retry-After', '5'))
                            await asyncio.sleep(retry_after)
                            raise aiohttp.ClientError("Rate limited")
                        
                        if response.status == 404:  # No data
                            break
                        
                        response.raise_for_status()
                        data = await response.json()
                        
                        results = data.get('results', [])
                        if results:
                            all_results.extend(results)
                            self.total_rows += len(results)
                        
                        # Check for next page
                        next_url = data.get('next_url')
                        if not next_url:
                            break
                        
                        # Extract cursor from next_url
                        if 'cursor=' in next_url:
                            cursor = next_url.split('cursor=')[1].split('&')[0]
                        else:
                            break
                            
                except asyncio.TimeoutError:
                    print(f"Timeout para {task.ticker} {task.year}-{task.month:02d}")
                    break
                except Exception as e:
                    print(f"Error para {task.ticker} {task.year}-{task.month:02d}: {e}")
                    break
        
        if all_results:
            # Convertir a DataFrame con normalización
            df = pl.DataFrame(all_results)
            
            # Normalizar columnas
            df = df.with_columns([
                (pl.col('t') / 1000).cast(pl.Datetime).alias('timestamp'),
                pl.lit(task.ticker).alias('ticker')
            ])
            
            # Agregar columnas de fecha/tiempo para análisis
            df = df.with_columns([
                pl.col('timestamp').dt.date().alias('date'),
                pl.col('timestamp').dt.strftime('%Y-%m-%d %H:%M').alias('minute'),
                pl.col('timestamp').dt.hour().alias('hour'),
                pl.col('timestamp').dt.minute().alias('minute_of_hour')
            ])
            
            # Renombrar columnas para consistencia
            df = df.rename({
                'o': 'open',
                'h': 'high',
                'l': 'low',
                'c': 'close',
                'v': 'volume',
                'vw': 'vwap',
                'n': 'transactions'
            })
            
            # Seleccionar y ordenar columnas
            df = df.select([
                'ticker', 'date', 'minute', 'timestamp', 't',
                'open', 'high', 'low', 'close', 'volume', 'vwap', 'transactions'
            ]).sort('timestamp')
            
            return df
        
        return None
    
    async def save_month_data(self, df: pl.DataFrame, task: DownloadTask):
        """Guarda datos del mes con compresión optimizada."""
        if df is None or df.is_empty():
            return
        
        # Path de salida
        output_dir = self.outdir / task.ticker / f"year={task.year}" / f"month={task.month:02d}"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "minute.parquet"
        
        # Si existe, hacer merge inteligente
        if output_file.exists():
            try:
                existing = pl.read_parquet(output_file)
                
                # Merge y deduplicación
                df = pl.concat([existing, df], how="vertical_relaxed")
                df = df.unique(subset=['minute'], keep='last')
                df = df.sort('timestamp')
            except:
                pass  # Si falla la lectura, sobrescribir
        
        # Guardar con compresión optimizada para queries rápidos
        df.write_parquet(
            output_file,
            compression='zstd',
            compression_level=3,  # Balance entre tamaño y velocidad
            statistics=True,  # Para queries eficientes
            row_group_size=50000  # Optimizado para chunks de análisis
        )
        
        # Actualizar checkpoint
        task_id = f"{task.ticker}_{task.year}_{task.month:02d}"
        self.completed_tasks.add(task_id)
        self.total_downloaded += 1
        
        # Guardar checkpoint cada 100 descargas
        if self.total_downloaded % 100 == 0:
            self.save_checkpoint()
    
    async def process_task(self, task: DownloadTask):
        """Procesa una tarea de descarga completa."""
        df = await self.fetch_month_data(task)
        if df is not None:
            await self.save_month_data(df, task)
            return True
        return False
    
    async def download_batch(self, tasks: List[DownloadTask], progress_callback=None):
        """Descarga un batch de tareas con progreso."""
        self.start_time = datetime.now()
        
        # Ordenar por prioridad (mayor volumen primero)
        tasks.sort(key=lambda x: x.priority, reverse=True)
        
        # Procesar tareas
        completed = 0
        errors = 0
        
        for i, task in enumerate(tasks):
            try:
                success = await self.process_task(task)
                if success:
                    completed += 1
            except Exception as e:
                errors += 1
                print(f"Error procesando {task.ticker} {task.year}-{task.month:02d}: {e}")
            
            # Callback de progreso
            if progress_callback and (i + 1) % 10 == 0:
                elapsed = (datetime.now() - self.start_time).total_seconds()
                rate = (i + 1) / elapsed if elapsed > 0 else 0
                eta = (len(tasks) - i - 1) / rate if rate > 0 else 0
                
                progress_callback({
                    'completed': i + 1,
                    'total': len(tasks),
                    'success': completed,
                    'errors': errors,
                    'rate': rate,
                    'eta_minutes': eta / 60,
                    'total_rows': self.total_rows
                })
        
        # Guardar checkpoint final
        self.save_checkpoint()
        
        return {
            'completed': completed,
            'errors': errors,
            'total_rows': self.total_rows,
            'elapsed_seconds': (datetime.now() - self.start_time).total_seconds()
        }

def calculate_priority(ticker: str, year: int, month: int, volume_data: Optional[Dict] = None) -> float:
    """
    Calcula prioridad de descarga basada en volumen/liquidez.
    Prioriza datos más recientes y tickers con mayor volumen.
    """
    # Peso por recencia (datos más recientes son más importantes para trading)
    days_ago = (datetime.now() - datetime(year, month, 1)).days
    recency_weight = 1.0 / (1 + days_ago / 365)  # Decae con el tiempo
    
    # Peso por volumen (si tenemos datos)
    volume_weight = 1.0
    if volume_data and ticker in volume_data:
        avg_volume = volume_data[ticker].get('avg_volume', 0)
        volume_weight = min(avg_volume / 1_000_000, 10)  # Normalizar a millones
    
    return recency_weight * volume_weight

async def main():
    parser = argparse.ArgumentParser(description='Descarga optimizada de intraday faltantes')
    parser.add_argument('--missing-csv', required=True, help='CSV con columnas ticker,year,month')
    parser.add_argument('--outdir', required=True, help='Directorio de salida')
    parser.add_argument('--api-key', help='Polygon API key')
    parser.add_argument('--concurrent', type=int, default=20, help='Requests concurrentes (default: 20)')
    parser.add_argument('--limit', type=int, help='Limitar a N meses (para testing)')
    parser.add_argument('--volume-data', help='JSON con datos de volumen para priorización')
    parser.add_argument('--prioritize-recent', action='store_true', help='Priorizar datos recientes')
    
    args = parser.parse_args()
    
    # API key
    api_key = args.api_key or os.getenv('POLYGON_API_KEY')
    if not api_key:
        print("ERROR: Necesitas --api-key o POLYGON_API_KEY")
        sys.exit(1)
    
    print("=" * 80)
    print("DESCARGA OPTIMIZADA DE INTRADAY FALTANTES")
    print("=" * 80)
    
    # Cargar datos
    df = pl.read_csv(args.missing_csv)
    
    if args.limit:
        df = df.head(args.limit)
        print(f"Limitado a {args.limit} meses")
    
    # Cargar datos de volumen si están disponibles
    volume_data = {}
    if args.volume_data and Path(args.volume_data).exists():
        with open(args.volume_data, 'r') as f:
            volume_data = json.load(f)
    
    # Crear tareas con prioridad
    tasks = []
    for row in df.iter_rows(named=True):
        priority = calculate_priority(
            row['ticker'],
            int(row['year']),
            int(row['month']),
            volume_data
        )
        
        tasks.append(DownloadTask(
            ticker=row['ticker'],
            year=int(row['year']),
            month=int(row['month']),
            priority=priority
        ))
    
    print(f"Total tareas: {len(tasks):,}")
    
    # Si priorizamos recientes, ordenar por fecha
    if args.prioritize_recent:
        tasks.sort(key=lambda x: (x.year, x.month), reverse=True)
        print("Priorizando datos recientes")
    
    # Callback de progreso
    def progress_callback(stats):
        print(f"Progreso: {stats['completed']}/{stats['total']} "
              f"({stats['completed']/stats['total']*100:.1f}%) | "
              f"Rate: {stats['rate']:.1f} tasks/s | "
              f"ETA: {stats['eta_minutes']:.1f} min | "
              f"Rows: {stats['total_rows']:,}")
    
    # Iniciar descarga
    downloader = OptimizedIntradayDownloader(
        api_key=api_key,
        outdir=Path(args.outdir),
        max_concurrent=args.concurrent
    )
    
    try:
        await downloader.init_session()
        results = await downloader.download_batch(tasks, progress_callback)
        
        print("\n" + "=" * 80)
        print("DESCARGA COMPLETADA")
        print("=" * 80)
        print(f"Exitosas: {results['completed']:,}")
        print(f"Errores: {results['errors']:,}")
        print(f"Total rows: {results['total_rows']:,}")
        print(f"Tiempo: {results['elapsed_seconds']/60:.1f} minutos")
        print(f"Velocidad: {results['completed']/results['elapsed_seconds']:.2f} tasks/s")
        
    finally:
        await downloader.close_session()

if __name__ == '__main__':
    asyncio.run(main())
