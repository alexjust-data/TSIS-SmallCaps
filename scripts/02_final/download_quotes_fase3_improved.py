#!/usr/bin/env python3
"""
Descarga OPTIMIZADA para FASE 3 - VERSION MEJORADA
Con mejor formato de tiempo y estadísticas
"""

import asyncio
import aiohttp
import polars as pl
from pathlib import Path
import os
import sys
import json
from datetime import datetime
import backoff

class FastQuotesDownloader:
    def __init__(self, api_key: str, max_concurrent: int = 50):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io/v3/quotes"
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session = None
        self.completed = 0
        self.errors = 0
        self.total = 0
        self.start_time = None
        self.checkpoint_file = Path("quotes_download_checkpoint.json")
        
    def save_checkpoint(self):
        """Guarda checkpoint para resume capability"""
        checkpoint = {
            'completed': self.completed,
            'errors': self.errors,
            'elapsed': (datetime.now() - self.start_time).total_seconds() if self.start_time else 0,
            'timestamp': datetime.now().isoformat()
        }
        with open(self.checkpoint_file, 'w') as f:
            json.dump(checkpoint, f)
    
    def load_checkpoint(self):
        """Carga checkpoint si existe"""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r') as f:
                    return json.load(f)
            except:
                pass
        return None
    
    def format_eta(self, seconds):
        """Formatea ETA de manera legible"""
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            return f"{seconds/60:.1f}m"
        elif seconds < 86400:
            hours = int(seconds / 3600)
            minutes = int((seconds % 3600) / 60)
            return f"{hours}h {minutes}m"
        else:
            days = int(seconds / 86400)
            hours = int((seconds % 86400) / 3600)
            return f"{days}d {hours}h"
    
    def print_progress(self, pages_downloaded=0):
        """Imprime progreso con formato mejorado"""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        rate = self.completed / elapsed if elapsed > 0 else 0
        eta = (self.total - self.completed) / rate if rate > 0 else 0
        
        # Calcular porcentajes
        progress_pct = (self.completed / self.total * 100) if self.total > 0 else 0
        success_count = self.completed - self.errors
        success_pct = (success_count / self.completed * 100) if self.completed > 0 else 0
        error_pct = (self.errors / self.completed * 100) if self.completed > 0 else 0
        
        # Formato de tiempo transcurrido
        elapsed_str = self.format_eta(elapsed)
        eta_str = self.format_eta(eta)
        
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] "
              f"Progress: {self.completed:,}/{self.total:,} ({progress_pct:.1f}%) | "
              f"Success: {success_count:,} ({success_pct:.1f}%) | "
              f"Errors: {self.errors:,} ({error_pct:.1f}%) | "
              f"Rate: {rate:.1f}/s | "
              f"ETA: {eta_str} | "
              f"Elapsed: {elapsed_str}", 
              end='')
        
        if pages_downloaded > 0:
            print(f" | Pages: {pages_downloaded}")
        else:
            print()
        
    async def init_session(self):
        """Session con configuración agresiva"""
        connector = aiohttp.TCPConnector(
            limit=200,
            limit_per_host=100,
            ttl_dns_cache=600,
            enable_cleanup_closed=True,
            force_close=False,
            keepalive_timeout=30
        )
        
        timeout = aiohttp.ClientTimeout(
            total=60,
            connect=5,
            sock_read=30
        )
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout
        )
    
    @backoff.on_exception(
        backoff.constant,
        (aiohttp.ClientError, asyncio.TimeoutError),
        max_tries=3,
        interval=1
    )
    async def download_day(self, ticker: str, date: str, output_dir: Path):
        """Descarga quotes de un día con paginación completa"""
        
        # Check si ya existe
        year, month, day = date.split('-')
        output_file = output_dir / ticker / f"year={year}" / f"month={month}" / f"day={date}" / "quotes.parquet"
        if output_file.exists():
            self.completed += 1
            return
        
        url = f"{self.base_url}/{ticker}"
        params = {
            'timestamp.gte': f'{date}T09:30:00-05:00',
            'timestamp.lt': f'{date}T16:00:00-05:00',
            'limit': 50000,
            'apiKey': self.api_key,
            'order': 'asc'
        }
        
        all_data = []
        pages_downloaded = 0
        
        async with self.semaphore:
            try:
                # Primera página
                async with self.session.get(url, params=params) as resp:
                    if resp.status == 429:
                        retry_after = int(resp.headers.get('X-Polygon-Retry-After', '5'))
                        await asyncio.sleep(retry_after)
                        
                        # Reintentar una vez
                        async with self.session.get(url, params=params) as resp2:
                            if resp2.status != 200:
                                self.errors += 1
                                self.completed += 1
                                return
                            data = await resp2.json()
                    elif resp.status != 200:
                        self.errors += 1
                        self.completed += 1
                        return
                    else:
                        data = await resp.json()
                    
                    results = data.get('results', [])
                    
                    if not results:
                        # Crear archivo vacío
                        output_file.parent.mkdir(parents=True, exist_ok=True)
                        pl.DataFrame().write_parquet(output_file)
                        self.completed += 1
                        return
                    
                    all_data.extend(results)
                    pages_downloaded = 1
                    
                    # Paginación completa
                    next_url = data.get('next_url')
                    
                    # Descargar TODAS las páginas
                    while next_url and pages_downloaded < 100:
                        if 'apiKey=' not in next_url:
                            if '?' in next_url:
                                next_url += f"&apiKey={self.api_key}"
                            else:
                                next_url += f"?apiKey={self.api_key}"
                        
                        try:
                            async with self.session.get(next_url) as page_resp:
                                if page_resp.status == 429:
                                    await asyncio.sleep(2)
                                    break
                                
                                if page_resp.status == 200:
                                    page_data = await page_resp.json()
                                    page_results = page_data.get('results', [])
                                    if page_results:
                                        all_data.extend(page_results)
                                        pages_downloaded += 1
                                    next_url = page_data.get('next_url')
                                else:
                                    break
                        except:
                            break
                
                # Guardar datos
                if all_data:
                    df = pl.DataFrame(all_data)
                    
                    if 'sip_timestamp' in df.columns:
                        df = df.rename({'sip_timestamp': 'timestamp'})
                    
                    output_file.parent.mkdir(parents=True, exist_ok=True)
                    df.write_parquet(output_file, compression='zstd', compression_level=1)
                
                self.completed += 1
                
                # Progress con checkpoint cada 100 tareas
                if self.completed % 100 == 0:
                    self.save_checkpoint()
                    self.print_progress(pages_downloaded)
                    
            except Exception as e:
                self.errors += 1
                self.completed += 1
                if self.completed % 1000 == 0:
                    print(f"[{datetime.now():%H:%M:%S}] Error en {ticker} {date}: {str(e)[:100]}")
    
    async def download_all(self, csv_file: str, output_dir: str, resume: bool = False):
        """Descarga masiva paralela con resume capability"""
        # Cargar tareas
        df = pl.read_csv(csv_file)
        tasks_data = df.to_dicts()
        
        # Check resume
        skip_count = 0
        if resume:
            checkpoint = self.load_checkpoint()
            if checkpoint:
                skip_count = checkpoint['completed']
                self.errors = checkpoint.get('errors', 0)
                print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Resumiendo desde: {skip_count:,} completados, {self.errors:,} errores previos")
                tasks_data = tasks_data[skip_count:]
        
        self.total = len(tasks_data) + skip_count
        self.completed = skip_count
        self.start_time = datetime.now()
        
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Iniciando descarga")
        print(f"Total tareas: {self.total:,}")
        print(f"Por procesar: {len(tasks_data):,}")
        print(f"Concurrencia: {self.max_concurrent}")
        print("-" * 80)
        
        output_path = Path(output_dir)
        
        # Crear todas las tareas
        tasks = []
        for row in tasks_data:
            task = self.download_day(row['ticker'], row['date'], output_path)
            tasks.append(task)
        
        # Ejecutar en batches
        batch_size = 5000
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i+batch_size]
            await asyncio.gather(*batch, return_exceptions=True)
            
            # Guardar checkpoint después de cada batch
            self.save_checkpoint()
            
            # Imprimir progreso
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Batch {i//batch_size + 1} completado")
            self.print_progress()
        
        # Estadísticas finales
        elapsed = (datetime.now() - self.start_time).total_seconds()
        print("\n" + "="*80)
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] DESCARGA COMPLETADA")
        print("="*80)
        print(f"Total procesados: {self.completed:,}")
        
        if self.completed > 0:
            success_count = self.completed - self.errors
            success_pct = (success_count / self.completed * 100)
            error_pct = (self.errors / self.completed * 100)
            print(f"Exitosos: {success_count:,} ({success_pct:.1f}%)")
            print(f"Errores: {self.errors:,} ({error_pct:.1f}%)")
        
        print(f"Tiempo total: {self.format_eta(elapsed)}")
        
        if elapsed > 0:
            print(f"Velocidad promedio: {self.completed/elapsed:.1f} días/segundo")
        
        # Limpiar checkpoint si completado exitosamente
        if self.checkpoint_file.exists() and self.errors == 0:
            self.checkpoint_file.unlink()
            print("Checkpoint eliminado (descarga completa)")

async def main():
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', required=True, help='CSV con columnas ticker,date')
    parser.add_argument('--output', required=True, help='Directorio de salida')
    parser.add_argument('--concurrent', type=int, default=50, help='Descargas simultáneas')
    parser.add_argument('--api-key', help='Polygon API key')
    parser.add_argument('--resume', action='store_true', help='Resume desde checkpoint')
    
    args = parser.parse_args()
    
    api_key = args.api_key or os.getenv('POLYGON_API_KEY')
    if not api_key:
        print("ERROR: Necesitas API key")
        sys.exit(1)
    
    downloader = FastQuotesDownloader(api_key, args.concurrent)
    await downloader.init_session()
    
    try:
        await downloader.download_all(args.csv, args.output, args.resume)
    finally:
        await downloader.session.close()

if __name__ == '__main__':
    asyncio.run(main())
