#!/usr/bin/env python3
"""
Descarga datos económicos macro de Polygon: Inflation (CPI, PCE), Treasury Yields
"""

import asyncio
import aiohttp
import polars as pl
from pathlib import Path
from datetime import datetime
import argparse
import os
from typing import Dict, List

def log(msg: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {msg}", flush=True)

class EconomicDataDownloader:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io"
        
    async def download_inflation_data(self, output_dir: Path):
        """Descarga datos de inflación (CPI, PCE)"""
        
        log("Descargando datos de inflación...")
        
        inflation_types = [
            {"series_id": "CPIAUCSL", "name": "cpi_all_items"},  # CPI All Items
            {"series_id": "CPILFESL", "name": "cpi_core"},       # Core CPI (ex food & energy)
            {"series_id": "CPIENGSL", "name": "cpi_energy"},     # CPI Energy
            {"series_id": "CPIFABSL", "name": "cpi_food"},       # CPI Food & Beverages
            {"series_id": "PCEPI", "name": "pce_all"},           # PCE All Items
            {"series_id": "PCEPILFE", "name": "pce_core"},       # Core PCE
        ]
        
        async with aiohttp.ClientSession() as session:
            for series in inflation_types:
                url = f"{self.base_url}/v1/reference/economic"
                params = {
                    "series_id": series["series_id"],
                    "apiKey": self.api_key
                }
                
                try:
                    async with session.get(url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            
                            if data.get('results'):
                                df = pl.DataFrame(data['results'])
                                
                                # Guardar
                                output_file = output_dir / f"{series['name']}.parquet"
                                df.write_parquet(output_file)
                                
                                log(f"  {series['name']}: {len(df)} observaciones guardadas")
                        else:
                            log(f"  Error descargando {series['name']}: Status {response.status}", "WARNING")
                            
                except Exception as e:
                    log(f"  Error con {series['name']}: {e}", "ERROR")
    
    async def download_treasury_yields(self, output_dir: Path):
        """Descarga Treasury Yields para diferentes madureces"""
        
        log("Descargando Treasury Yields...")
        
        maturities = [
            {"maturity": "3month", "name": "treasury_3m"},
            {"maturity": "6month", "name": "treasury_6m"},
            {"maturity": "1year", "name": "treasury_1y"},
            {"maturity": "2year", "name": "treasury_2y"},
            {"maturity": "5year", "name": "treasury_5y"},
            {"maturity": "10year", "name": "treasury_10y"},
            {"maturity": "20year", "name": "treasury_20y"},
            {"maturity": "30year", "name": "treasury_30y"}
        ]
        
        async with aiohttp.ClientSession() as session:
            for treasury in maturities:
                url = f"{self.base_url}/v1/marketdata/treasury-yields"
                params = {
                    "maturity": treasury["maturity"],
                    "from": "1990-01-01",  # Máximo histórico disponible
                    "apiKey": self.api_key,
                    "limit": 10000
                }
                
                all_data = []
                next_url = None
                
                while True:
                    try:
                        if next_url:
                            full_url = next_url if next_url.startswith('http') else f"{self.base_url}{next_url}"
                            if 'apiKey=' not in full_url:
                                full_url += f"&apiKey={self.api_key}"
                            async with session.get(full_url) as response:
                                data = await response.json()
                        else:
                            async with session.get(url, params=params) as response:
                                data = await response.json()
                        
                        if response.status == 200:
                            results = data.get('results', [])
                            if results:
                                all_data.extend(results)
                            
                            next_url = data.get('next_url')
                            if not next_url:
                                break
                        else:
                            log(f"  Error con {treasury['name']}: Status {response.status}", "WARNING")
                            break
                            
                    except Exception as e:
                        log(f"  Error con {treasury['name']}: {e}", "ERROR")
                        break
                
                if all_data:
                    df = pl.DataFrame(all_data)
                    output_file = output_dir / f"{treasury['name']}.parquet"
                    df.write_parquet(output_file)
                    log(f"  {treasury['name']}: {len(df)} observaciones guardadas")
    
    async def download_fed_funds_rate(self, output_dir: Path):
        """Descarga Fed Funds Rate histórico"""
        
        log("Descargando Fed Funds Rate...")
        
        url = f"{self.base_url}/v1/reference/economic"
        params = {
            "series_id": "DFF",  # Daily Fed Funds Rate
            "apiKey": self.api_key
        }
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if data.get('results'):
                            df = pl.DataFrame(data['results'])
                            output_file = output_dir / "fed_funds_rate.parquet"
                            df.write_parquet(output_file)
                            log(f"  Fed Funds Rate: {len(df)} observaciones guardadas")
                            
            except Exception as e:
                log(f"  Error con Fed Funds Rate: {e}", "ERROR")
    
    async def download_gdp_data(self, output_dir: Path):
        """Descarga datos de GDP"""
        
        log("Descargando datos de GDP...")
        
        gdp_series = [
            {"series_id": "GDP", "name": "gdp_nominal"},
            {"series_id": "GDPC1", "name": "gdp_real"},
            {"series_id": "A191RL1Q225SBEA", "name": "gdp_growth_rate"}
        ]
        
        async with aiohttp.ClientSession() as session:
            for series in gdp_series:
                url = f"{self.base_url}/v1/reference/economic"
                params = {
                    "series_id": series["series_id"],
                    "apiKey": self.api_key
                }
                
                try:
                    async with session.get(url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            
                            if data.get('results'):
                                df = pl.DataFrame(data['results'])
                                output_file = output_dir / f"{series['name']}.parquet"
                                df.write_parquet(output_file)
                                log(f"  {series['name']}: {len(df)} observaciones guardadas")
                                
                except Exception as e:
                    log(f"  Error con {series['name']}: {e}", "ERROR")
    
    async def download_unemployment_data(self, output_dir: Path):
        """Descarga datos de desempleo"""
        
        log("Descargando datos de desempleo...")
        
        url = f"{self.base_url}/v1/reference/economic"
        params = {
            "series_id": "UNRATE",  # Unemployment Rate
            "apiKey": self.api_key
        }
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if data.get('results'):
                            df = pl.DataFrame(data['results'])
                            output_file = output_dir / "unemployment_rate.parquet"
                            df.write_parquet(output_file)
                            log(f"  Unemployment Rate: {len(df)} observaciones guardadas")
                            
            except Exception as e:
                log(f"  Error con Unemployment Rate: {e}", "ERROR")

async def main_async():
    parser = argparse.ArgumentParser(description='Descarga datos económicos macro de Polygon')
    parser.add_argument('--api-key', help='Polygon API key')
    parser.add_argument('--output-dir', required=True, help='Directorio de salida')
    parser.add_argument('--data-types', nargs='+',
                       choices=['inflation', 'treasury', 'fed', 'gdp', 'unemployment', 'all'],
                       default=['all'],
                       help='Tipos de datos económicos a descargar')
    
    args = parser.parse_args()
    
    # API key
    api_key = args.api_key or os.getenv('POLYGON_API_KEY')
    if not api_key:
        log("ERROR: Necesitas --api-key o POLYGON_API_KEY", "ERROR")
        return
    
    # Crear directorio
    output_dir = Path(args.output_dir) / "economic_data"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    log("="*60)
    log("DESCARGA DE DATOS ECONÓMICOS MACRO")
    log("="*60)
    
    downloader = EconomicDataDownloader(api_key)
    
    download_all = 'all' in args.data_types
    
    # Inflation
    if download_all or 'inflation' in args.data_types:
        inflation_dir = output_dir / "inflation"
        inflation_dir.mkdir(parents=True, exist_ok=True)
        await downloader.download_inflation_data(inflation_dir)
    
    # Treasury Yields
    if download_all or 'treasury' in args.data_types:
        treasury_dir = output_dir / "treasury_yields"
        treasury_dir.mkdir(parents=True, exist_ok=True)
        await downloader.download_treasury_yields(treasury_dir)
    
    # Fed Funds Rate
    if download_all or 'fed' in args.data_types:
        fed_dir = output_dir / "fed"
        fed_dir.mkdir(parents=True, exist_ok=True)
        await downloader.download_fed_funds_rate(fed_dir)
    
    # GDP
    if download_all or 'gdp' in args.data_types:
        gdp_dir = output_dir / "gdp"
        gdp_dir.mkdir(parents=True, exist_ok=True)
        await downloader.download_gdp_data(gdp_dir)
    
    # Unemployment
    if download_all or 'unemployment' in args.data_types:
        unemployment_dir = output_dir / "unemployment"
        unemployment_dir.mkdir(parents=True, exist_ok=True)
        await downloader.download_unemployment_data(unemployment_dir)
    
    log("\n" + "="*60)
    log("DESCARGA COMPLETADA")
    log("="*60)

def main():
    asyncio.run(main_async())

if __name__ == '__main__':
    main()
