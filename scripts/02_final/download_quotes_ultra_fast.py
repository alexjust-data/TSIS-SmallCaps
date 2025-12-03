#!/usr/bin/env python3
"""
Download Quotes ULTRA-FAST - Optimizado para máxima velocidad
Target: 50-100 tareas/segundo (10-25x más rápido)
"""

import asyncio
import aiohttp
import polars as pl
from pathlib import Path
import os
import sys
import json
from datetime import datetime
from typing import Dict, List, Set, Tuple
import time
from collections import defaultdict
import backoff

class UltraFastQuotesDownloader:
    def __init__(self, api_key: str, output_dir: Path, max_concurrent: int = 100):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io/v3/quotes"
        self.output_dir = Path(output_dir)
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        # Session HTTP
        self.session = None
        
        # Cache de días vacíos para skip rápido
        self.cache_file = self.output_dir / ".quotes_cache.json"
        self.empty_days_cache = self.load_cache()
        self.completed_cache = set()  # En memoria para velocidad
        
        # Checkpoint menos frecuente
        self.checkpoint_file = self.output_dir / ".checkpoint.json"
        self.checkpoint_interval = 5000  # Cada 5000 tareas
        
        # Métricas
        self.stats = {
            'completed': 0,
            'errors': 0,
            'skipped': 0,
            'empty': 0,
            'start_time': None,
            'total_quotes': 0,
            'total_pages': 0
        }
        
        # Buffer de escritura para batch saves
        self.write_buffer = defaultdict(list)
        self.buffer_size = 100  # Acumular 100 días antes de escribir
    
    def load_cache(self) -> Set[str]:
        """Carga cache de días vacíos"""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r') as f:
                    return set(json.load(f).get('empty_days', []))
            except:
                return set()
        return set()
    
    def save_cache(self):
        """Guarda cache eficientemente"""
        cache_data = {
            'empty_days': list(self.empty_days_cache),
            'timestamp': datetime.now().isoformat()
        }
        with open(self.cache_file, 'w') as f:
            json.dump(cache_data, f)
    
    def should_skip(self, ticker: str, date: str) -> bool:
        """Check rápido si debemos saltar este día"""
        # Cache key
        key = f"{ticker}_{date}"
        
        # Skip si está en cache de vacíos
        if key in self.empty_days_cache:
            self.stats['skipped'] += 1
            return True
        
        # Skip si ya completado (en memoria)
        if key in self.completed_cache:
            self.stats['skipped'] += 1
            return True
        
        # Check rápido si archivo existe
        year, month, day = date.split('-')
        output_file = self.output_dir / ticker / f"year={year}" / f"month={month}" / f"day={day}" / "quotes.parquet"
        if output_file.exists():
            self.completed_cache.add(key)
            self.stats['skipped'] += 1
            return True
        
        return False
    
    async def init_session(self):
        """Session con configuración ultra-agresiva"""
        connector = aiohttp.TCPConnector(
            limit=500,  # Más conexiones totales
            limit_per_host=250,  # Más por host
            ttl_dns_cache=3600,
            enable_cleanup_closed=False,
            force_close=False,
            keepalive_timeout=120
        )
        
        timeout = aiohttp.ClientTimeout(
            total=30,  # Timeout más corto
            connect=2,
            sock_read=15
        )
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'Connection': 'keep-alive',
                'Accept-Encoding': 'gzip, deflate'
            },
            skip_auto_headers={'User-Agent'},  # Menos headers = más rápido
            json_serialize=json.dumps  # Más rápido que default
        )
    
    async def fetch_quotes_raw(self, ticker: str, date: str) -> Tuple[str, str, List[dict]]:
        """Descarga quotes sin procesamiento (más rápido)"""
        
        # Skip check
        if self.should_skip(ticker, date):
            return (ticker, date, None)
        
        url = f"{self.base_url}/{ticker}"
        
        # Parámetros optimizados
        params = {
            'timestamp.gte': f'{date}T09:30:00-05:00',
            'timestamp.lt': f'{date}T16:00:00-05:00',
            'limit': 50000,
            'apiKey': self.api_key,
            'order': 'asc'
        }
        
        all_results = []
        pages = 0
        max_pages = 10  # Límite para no atascarse
        
        async with self.semaphore:
            try:
                # Primera página
                async with self.session.get(url, params=params) as resp:
                    if resp.status == 429:
                        await asyncio.sleep(1)
                        return (ticker, date, None)
                    
                    if resp.status != 200:
                        self.stats['errors'] += 1
                        return (ticker, date, None)
                    
                    data = await resp.json()
                    results = data.get('results', [])
                    
                    if not results:
                        # Día vacío - agregar a cache
                        self.empty_days_cache.add(f"{ticker}_{date}")
                        self.stats['empty'] += 1
                        return (ticker, date, [])
                    
                    all_results.extend(results)
                    pages = 1
                    self.stats['total_pages'] += 1
                    
                    # Paginación secuencial pero eficiente (más confiable que paralela)
                    next_url = data.get('next_url')
                    
                    while next_url and pages < max_pages:
                        if 'apiKey=' not in next_url:
                            next_url += f"&apiKey={self.api_key}" if '?' in next_url else f"?apiKey={self.api_key}"
                        
                        try:
                            async with self.session.get(next_url) as page_resp:
                                if page_resp.status == 429:
                                    await asyncio.sleep(1)
                                    break  # Salir si hay rate limit
                                
                                if page_resp.status == 200:
                                    page_data = await page_resp.json()
                                    page_results = page_data.get('results', [])
                                    
                                    if page_results:
                                        all_results.extend(page_results)
                                        pages += 1
                                        self.stats['total_pages'] += 1
                                        next_url = page_data.get('next_url')
                                    else:
                                        break  # No más resultados
                                else:
                                    break  # Error en página
                        except:
                            break  # Error en request
                    self.stats['total_quotes'] += len(all_results)
                    
                    return (ticker, date, all_results)
                    
            except asyncio.TimeoutError:
                self.stats['errors'] += 1
                return (ticker, date, None)
            except Exception:
                self.stats['errors'] += 1
                return (ticker, date, None)
    
    def save_batch(self, batch_data: List[Tuple[str, str, List[dict]]]):
        """Guarda un batch de resultados (en thread para no bloquear)"""
        for ticker, date, results in batch_data:
            if results is None:
                continue
            
            year, month, day = date.split('-')
            output_file = self.output_dir / ticker / f"year={year}" / f"month={month}" / f"day={day}" / "quotes.parquet"
            
            try:
                if results:  # Datos no vacíos
                    df = pl.DataFrame(results)
                    
                    # Mínimo procesamiento
                    if 'sip_timestamp' in df.columns:
                        df = df.rename({'sip_timestamp': 'timestamp'})
                    
                    output_file.parent.mkdir(parents=True, exist_ok=True)
                    df.write_parquet(
                        output_file,
                        compression='zstd',
                        compression_level=1,  # Más rápido
                        statistics=False  # Skip estadísticas
                    )
                else:  # Día vacío
                    output_file.parent.mkdir(parents=True, exist_ok=True)
                    pl.DataFrame().write_parquet(output_file)
                
                # Marcar como completado
                self.completed_cache.add(f"{ticker}_{date}")
                
            except Exception as e:
                print(f"Error guardando {ticker} {date}: {e}")
    
    async def process_batch_ultra_fast(self, tasks_batch: List[Tuple[str, str]]):
        """Procesa un batch completo en paralelo"""
        # Crear todas las tareas de descarga
        download_tasks = []
        for ticker, date in tasks_batch:
            download_tasks.append(self.fetch_quotes_raw(ticker, date))
        
        # Ejecutar todas en paralelo
        results = await asyncio.gather(*download_tasks, return_exceptions=True)
        
        # Filtrar resultados válidos
        valid_results = []
        for result in results:
            if isinstance(result, tuple):
                valid_results.append(result)
                self.stats['completed'] += 1
        
        # Guardar batch (podría ser en thread para no bloquear)
        if valid_results:
            self.save_batch(valid_results)
    
    async def download_all_ultra_fast(self, csv_file: str, resume: bool = False):
        """Descarga masiva con máxima velocidad"""
        
        # Cargar tareas
        print(f"Cargando tareas desde {csv_file}...")
        df = pl.read_csv(csv_file)
        
        # Convertir a lista de tuplas para velocidad
        tasks = [(row['ticker'], row['date']) for row in df.to_dicts()]
        total_tasks = len(tasks)
        
        # Resume logic
        if resume and self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r') as f:
                    checkpoint = json.load(f)
                    skip = checkpoint.get('completed', 0)
                    tasks = tasks[skip:]
                    self.stats['completed'] = skip
                    print(f"Resumiendo desde tarea {skip:,}")
            except:
                pass
        
        self.stats['start_time'] = time.time()
        
        print(f"="*60)
        print(f"DESCARGA ULTRA-RÁPIDA DE QUOTES")
        print(f"="*60)
        print(f"Total tareas: {total_tasks:,}")
        print(f"Por procesar: {len(tasks):,}")
        print(f"Concurrencia: {self.max_concurrent}")
        print(f"Output: {self.output_dir}")
        print("="*60)
        
        # Procesar en micro-batches para máxima velocidad
        batch_size = 500  # Batches más grandes
        
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            
            # Procesar batch completo en paralelo
            await self.process_batch_ultra_fast(batch)
            
            # Checkpoint menos frecuente
            if self.stats['completed'] % self.checkpoint_interval == 0:
                self.save_checkpoint()
                self.save_cache()
            
            # Progress update
            elapsed = time.time() - self.stats['start_time']
            if elapsed > 0:
                rate = self.stats['completed'] / elapsed
                eta = (total_tasks - self.stats['completed']) / rate if rate > 0 else 0
                
                success = self.stats['completed'] - self.stats['errors']
                
                # Calcular ETA más legible
                if eta < 3600:
                    eta_str = f"{eta/60:.1f}m"
                elif eta < 86400:
                    eta_str = f"{eta/3600:.1f}h"
                else:
                    eta_str = f"{eta/86400:.1f}d"
                
                print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] "
                      f"Progress: {self.stats['completed']}/{total_tasks} "
                      f"({self.stats['completed']/total_tasks*100:.1f}%) | "
                      f"Rate: {rate:.1f}/s | "
                      f"ETA: {eta_str} | "
                      f"Success: {success} | "
                      f"Errors: {self.stats['errors']} | "
                      f"Skip: {self.stats['skipped']} | "
                      f"Quotes: {self.stats['total_quotes']/1e6:.1f}M")
        
        # Final stats
        self.print_final_stats(total_tasks)
    
    def save_checkpoint(self):
        """Guarda checkpoint para resume"""
        checkpoint = {
            'completed': self.stats['completed'],
            'errors': self.stats['errors'],
            'timestamp': datetime.now().isoformat()
        }
        with open(self.checkpoint_file, 'w') as f:
            json.dump(checkpoint, f)
    
    def print_final_stats(self, total_tasks: int):
        """Imprime estadísticas finales"""
        elapsed = time.time() - self.stats['start_time']
        
        print("\n" + "="*60)
        print("DESCARGA COMPLETADA")
        print("="*60)
        print(f"Tiempo total: {elapsed/60:.1f} minutos ({elapsed/3600:.2f} horas)")
        print(f"Total procesados: {self.stats['completed']:,}")
        print(f"Exitosos: {self.stats['completed'] - self.stats['errors']:,}")
        print(f"Errores: {self.stats['errors']:,}")
        print(f"Saltados (cache): {self.stats['skipped']:,}")
        print(f"Días vacíos: {self.stats['empty']:,}")
        print(f"Total quotes: {self.stats['total_quotes']:,}")
        print(f"Total páginas: {self.stats['total_pages']:,}")
        
        if elapsed > 0:
            print(f"Velocidad promedio: {self.stats['completed']/elapsed:.1f} tareas/segundo")
            print(f"Quotes/segundo: {self.stats['total_quotes']/elapsed:.0f}")
    
    async def close(self):
        """Cierra recursos"""
        if self.session:
            await self.session.close()
        self.save_cache()
        
        # Limpiar checkpoint si completado
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()

async def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Download Quotes ULTRA-FAST")
    parser.add_argument('--csv', required=True, help='CSV con ticker,date')
    parser.add_argument('--output', required=True, help='Directorio de salida')
    parser.add_argument('--concurrent', type=int, default=100, help='Conexiones simultáneas')
    parser.add_argument('--api-key', help='Polygon API key')
    parser.add_argument('--resume', action='store_true', help='Resume desde checkpoint')
    
    args = parser.parse_args()
    
    # API key
    api_key = args.api_key or os.getenv('POLYGON_API_KEY')
    if not api_key:
        print("ERROR: POLYGON_API_KEY no encontrada")
        sys.exit(1)
    
    # Crear downloader
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    downloader = UltraFastQuotesDownloader(
        api_key=api_key,
        output_dir=output_dir,
        max_concurrent=args.concurrent
    )
    
    try:
        # Inicializar session
        await downloader.init_session()
        
        # Ejecutar descarga
        await downloader.download_all_ultra_fast(args.csv, args.resume)
        
    finally:
        await downloader.close()

if __name__ == '__main__':
    # Optimizaciones de event loop para Windows
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())