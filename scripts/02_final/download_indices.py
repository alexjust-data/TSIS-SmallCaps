#!/usr/bin/env python3
"""
Download Indices with Auto-Detection - Detecta automáticamente el rango de datos disponible
"""

import os
import sys
import json
import asyncio
import aiohttp
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
import polars as pl

# Configuracion
DEFAULT_CONN_LIMIT = 40
DEFAULT_TIMEOUT = 60
MAX_RETRIES = 3

# Indices y ETFs
INDICES_AVAILABLE = ["I:NDX", "I:COMP", "I:SOX"]
ETFS = [
    "SPY", "QQQ", "IWM", "DIA", "VIXY", "VXX", "UVXY",
    "TLT", "HYG", "LQD",
    "XLK", "XLF", "XLE", "XLV", "XLI", "XLP", "XLY", "XLB", "XLRE", "XLU", "XLC",
    "GLD", "SLV", "UUP", "FXE", "USO", "UNG",
    "SPSM", "VB", "EFA", "EEM"
]

def log(msg: str):
    """Log con timestamp"""
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}")

class SmartDownloader:
    def __init__(self, api_key: str, output_dir: Path):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io"
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Cache de rangos de tickers
        self.ticker_ranges = {}
        self.ranges_file = self.output_dir / "ticker_ranges.json"
        self.load_ranges_cache()
        
        # Session
        self.session = None
        self.provider = "polygon"
        self.headers = {}
        
        # Stats
        self.stats = {
            'downloaded_daily': 0,
            'downloaded_minute': 0,
            'skipped': 0,
            'failed': 0,
            'merged': 0
        }
    
    def load_ranges_cache(self):
        """Carga cache de rangos de tickers"""
        if self.ranges_file.exists():
            try:
                with open(self.ranges_file, 'r') as f:
                    self.ticker_ranges = json.load(f)
                    log(f"Cache de rangos cargado: {len(self.ticker_ranges)} tickers")
            except:
                self.ticker_ranges = {}
    
    def save_ranges_cache(self):
        """Guarda cache de rangos"""
        with open(self.ranges_file, 'w') as f:
            json.dump(self.ticker_ranges, f, indent=2)
    
    async def init_session(self):
        """Inicializa la sesion HTTP"""
        if not self.session:
            # Detectar provider
            if self.api_key.startswith("pk_") or self.api_key.startswith("sk_"):
                self.provider = "polygon"
                self.base_url = "https://api.polygon.io"
            else:
                self.provider = "massive"
                self.base_url = "https://api.massive.com"
                self.headers = {"Authorization": f"Bearer {self.api_key}"}
            
            connector = aiohttp.TCPConnector(limit=DEFAULT_CONN_LIMIT)
            timeout = aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT)
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers=self.headers if self.provider == "massive" else {}
            )
            
            log(f"Provider: {self.provider}")
    
    async def close(self):
        """Cierra la sesion"""
        if self.session:
            await self.session.close()
    
    async def ping_ticker(self, ticker: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Hace ping a un ticker para obtener su rango de datos disponible
        Retorna (fecha_inicio, fecha_fin) o (None, None) si no hay datos
        """
        # Si ya está en cache, usar eso
        if ticker in self.ticker_ranges:
            cached = self.ticker_ranges[ticker]
            if cached["start"] and cached["end"]:
                return cached["start"], cached["end"]
        
        log(f"  Ping {ticker}...")
        
        # Intentar obtener un sample de datos recientes
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        
        url = f"{self.base_url}/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}"
        params = {"adjusted": "true", "sort": "desc", "limit": 1}
        
        if self.provider == "polygon":
            params["apiKey"] = self.api_key
        
        try:
            async with self.session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("results"):
                        # Ahora buscar el rango completo
                        return await self.get_full_range(ticker)
                elif resp.status == 403:
                    log(f"    No autorizado para {ticker}")
                    self.ticker_ranges[ticker] = {"start": None, "end": None, "error": "403"}
                else:
                    self.ticker_ranges[ticker] = {"start": None, "end": None, "error": str(resp.status)}
        except Exception as e:
            log(f"    Error ping {ticker}: {e}")
            self.ticker_ranges[ticker] = {"start": None, "end": None, "error": str(e)}
        
        return None, None
    
    async def get_full_range(self, ticker: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Obtiene el rango completo de datos para un ticker
        """
        # Buscar fecha más antigua (desde 1990)
        url = f"{self.base_url}/v2/aggs/ticker/{ticker}/range/1/day/1990-01-01/2025-12-31"
        params = {"adjusted": "true", "sort": "asc", "limit": 1}
        
        if self.provider == "polygon":
            params["apiKey"] = self.api_key
        
        first_date = None
        last_date = None
        
        try:
            # Primera fecha
            async with self.session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("results"):
                        first_timestamp = data["results"][0]["t"]
                        first_date = datetime.fromtimestamp(first_timestamp/1000).strftime("%Y-%m-%d")
            
            # Última fecha
            params["sort"] = "desc"
            async with self.session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("results"):
                        last_timestamp = data["results"][0]["t"]
                        last_date = datetime.fromtimestamp(last_timestamp/1000).strftime("%Y-%m-%d")
            
            if first_date and last_date:
                log(f"    Rango detectado: {first_date} a {last_date}")
                self.ticker_ranges[ticker] = {
                    "start": first_date,
                    "end": last_date,
                    "detected_at": datetime.now().isoformat()
                }
                self.save_ranges_cache()
                return first_date, last_date
                
        except Exception as e:
            log(f"    Error obteniendo rango: {e}")
        
        return None, None
    
    async def fetch_data(self, ticker: str, timespan: str, start_date: str, end_date: str) -> Optional[pl.DataFrame]:
        """
        Descarga datos para un ticker
        """
        url = f"{self.base_url}/v2/aggs/ticker/{ticker}/range/1/{timespan}/{start_date}/{end_date}"
        params = {"adjusted": "true", "sort": "asc", "limit": 50000}
        
        if self.provider == "polygon":
            params["apiKey"] = self.api_key
        
        all_results = []
        
        try:
            async with self.session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    results = data.get("results", [])
                    
                    if results:
                        all_results.extend(results)
                        
                        # Paginacion
                        next_url = data.get("next_url")
                        while next_url:
                            if self.provider == "polygon" and "apiKey=" not in next_url:
                                next_url += f"&apiKey={self.api_key}"
                            
                            async with self.session.get(next_url) as page_resp:
                                if page_resp.status == 200:
                                    page_data = await page_resp.json()
                                    page_results = page_data.get("results", [])
                                    if page_results:
                                        all_results.extend(page_results)
                                    next_url = page_data.get("next_url")
                                else:
                                    break
                elif resp.status == 429:
                    wait_time = int(resp.headers.get('X-Polygon-Retry-After', '5'))
                    log(f"  Rate limit, esperando {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    return await self.fetch_data(ticker, timespan, start_date, end_date)
        
        except Exception as e:
            log(f"  Error descargando {ticker}: {e}")
            return None
        
        if all_results:
            # Convertir a DataFrame
            df = pl.DataFrame(all_results)
            
            # Procesar timestamps
            if "t" in df.columns:
                df = df.with_columns([
                    (pl.col("t") / 1000).cast(pl.Datetime).alias("datetime"),
                    (pl.col("t") / 1000).cast(pl.Datetime).dt.date().alias("date")
                ])
                df = df.drop("t")
            
            # Renombrar columnas
            rename_map = {
                "o": "open", "h": "high", "l": "low", "c": "close",
                "v": "volume", "vw": "vwap", "n": "trades"
            }
            for old, new in rename_map.items():
                if old in df.columns:
                    df = df.rename({old: new})
            
            return df
        
        return None
    
    async def fetch_minute_by_months(self, ticker: str, start_date: str, end_date: str) -> Optional[pl.DataFrame]:
        """
        Descarga datos minute en bloques mensuales para evitar timeouts
        """
        import calendar

        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        all_dfs = []
        current = start
        total_rows = 0

        while current <= end:
            # Calcular inicio y fin del mes
            month_start = current.replace(day=1)
            last_day = calendar.monthrange(month_start.year, month_start.month)[1]
            month_end = month_start.replace(day=last_day)

            if month_end > end:
                month_end = end

            month_start_str = month_start.strftime("%Y-%m-%d")
            month_end_str = month_end.strftime("%Y-%m-%d")

            log(f"    {month_start_str} -> {month_end_str}...")

            df = await self.fetch_data(ticker, "minute", month_start_str, month_end_str)
            if df is not None and not df.is_empty():
                all_dfs.append(df)
                total_rows += len(df)
                log(f"      {len(df):,} rows (total: {total_rows:,})")

            # Siguiente mes
            current = month_end + timedelta(days=1)
            await asyncio.sleep(0.3)  # Pausa entre meses

        if all_dfs:
            combined = pl.concat(all_dfs)
            if 'datetime' in combined.columns:
                combined = combined.unique(subset=['datetime']).sort('datetime')
            return combined
        return None

    async def download_ticker(self, ticker: str, timespan: str = "day", force: bool = False):
        """
        Descarga datos de un ticker con detección automática de rango
        """
        log(f"\nProcesando {ticker}...")

        # Detectar rango disponible
        start_date, end_date = await self.ping_ticker(ticker)

        if not start_date or not end_date:
            log(f"  Sin datos disponibles para {ticker}")
            self.stats['failed'] += 1
            return

        # Archivo de salida
        category = "indices" if ticker.startswith("I:") else "etfs"
        safe_ticker = ticker.replace(":", "_")
        out_file = self.output_dir / category / safe_ticker / f"{timespan}.parquet"

        # Si ya existe y no forzar, saltar
        if out_file.exists() and not force:
            log(f"  Ya existe, saltando")
            self.stats['skipped'] += 1
            return

        # Descargar datos
        log(f"  Descargando {timespan} data: {start_date} a {end_date}")

        if timespan == "minute":
            # Para minute, descargar por meses
            df = await self.fetch_minute_by_months(ticker, start_date, end_date)
        else:
            df = await self.fetch_data(ticker, timespan, start_date, end_date)

        if df is not None and not df.is_empty():
            # Guardar
            out_file.parent.mkdir(parents=True, exist_ok=True)
            df.write_parquet(out_file, compression="zstd", compression_level=3)

            log(f"  Guardado: {len(df)} registros")
            self.stats['downloaded_minute' if timespan == 'minute' else 'downloaded_daily'] += len(df)
            self.stats['merged'] += 1
        else:
            log(f"  No se obtuvieron datos")
            self.stats['failed'] += 1
    
    async def download_all(self, tickers: List[str], timespan: str = "day", force: bool = False):
        """
        Descarga todos los tickers
        """
        print("\n" + "="*60)
        print("DESCARGA CON AUTO-DETECCION DE RANGO")
        print("="*60)
        print(f"Tickers: {len(tickers)}")
        print(f"Timespan: {timespan}")
        print(f"Force: {force}")
        print("="*60)
        
        for ticker in tickers:
            await self.download_ticker(ticker, timespan, force)
            await asyncio.sleep(0.1)
        
        # Resumen
        print("\n" + "="*60)
        print("RESUMEN")
        print("="*60)
        print(f"Descargados: {self.stats['downloaded_daily']} registros")
        print(f"Archivos creados: {self.stats['merged']}")
        print(f"Saltados: {self.stats['skipped']}")
        print(f"Fallidos: {self.stats['failed']}")
        print("="*60)

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="C:/TSIS_Data/regime_indicators")
    parser.add_argument("--daily", action="store_true", help="Descargar daily")
    parser.add_argument("--minute", action="store_true", help="Descargar 1-minute")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--indices-only", action="store_true")
    parser.add_argument("--etfs-only", action="store_true")
    
    args = parser.parse_args()
    
    # API key
    api_key = os.getenv("POLYGON_API_KEY") or os.getenv("MASSIVE_API_KEY")
    if not api_key:
        print("ERROR: Configurar POLYGON_API_KEY")
        sys.exit(1)
    
    # Tickers
    if args.indices_only:
        tickers = INDICES_AVAILABLE
    elif args.etfs_only:
        tickers = ETFS
    else:
        tickers = INDICES_AVAILABLE + ETFS
    
    # Timespan
    timespan = "minute" if args.minute else "day"
    
    # Downloader
    downloader = SmartDownloader(api_key, Path(args.output))
    
    try:
        await downloader.init_session()
        await downloader.download_all(tickers, timespan, args.force)
        downloader.save_ranges_cache()
    finally:
        await downloader.close()

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())