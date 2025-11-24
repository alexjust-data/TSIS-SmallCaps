#!/usr/bin/env python3
"""
Genera lista de tickers únicos desde tus datos existentes para usar en descargas
"""

import polars as pl
from pathlib import Path
import argparse

def extract_tickers_from_directory(directory: Path, pattern: str = "*.parquet") -> set:
    """Extrae tickers únicos de un directorio con estructura específica"""
    
    tickers = set()
    
    # Para estructuras tipo: {ticker}/year={year}/...
    for ticker_dir in directory.iterdir():
        if ticker_dir.is_dir() and not ticker_dir.name.startswith('.'):
            # El nombre del directorio es el ticker
            tickers.add(ticker_dir.name)
    
    return tickers

def main():
    parser = argparse.ArgumentParser(description='Genera lista de tickers desde datos existentes')
    parser.add_argument('--daily-dir', help='Directorio de OHLCV daily')
    parser.add_argument('--ping-file', help='Archivo ping_range parquet')
    parser.add_argument('--reference-dir', help='Directorio de reference data')
    parser.add_argument('--output', required=True, help='Archivo de salida con tickers')
    parser.add_argument('--filter-active', action='store_true', help='Solo tickers activos')
    
    args = parser.parse_args()
    
    all_tickers = set()
    
    # Desde directorio daily
    if args.daily_dir:
        daily_path = Path(args.daily_dir)
        if daily_path.exists():
            daily_tickers = extract_tickers_from_directory(daily_path)
            print(f"Tickers desde daily: {len(daily_tickers)}")
            all_tickers.update(daily_tickers)
    
    # Desde ping_range
    if args.ping_file:
        ping_path = Path(args.ping_file)
        if ping_path.exists():
            df = pl.read_parquet(ping_path)
            if args.filter_active:
                df = df.filter(pl.col('has_data') == True)
            ping_tickers = set(df['ticker'].to_list())
            print(f"Tickers desde ping_range: {len(ping_tickers)}")
            all_tickers.update(ping_tickers)
    
    # Desde reference
    if args.reference_dir:
        ref_path = Path(args.reference_dir) / "tickers_snapshot"
        if ref_path.exists():
            # Buscar el archivo más reciente
            files = list(ref_path.glob("*.parquet"))
            if files:
                latest = max(files, key=lambda f: f.stat().st_mtime)
                df = pl.read_parquet(latest)
                
                if args.filter_active:
                    # Filtrar solo tickers activos
                    df = df.filter(
                        (pl.col('market') == 'stocks') &
                        (pl.col('active') == True)
                    )
                
                ref_tickers = set(df['ticker'].to_list())
                print(f"Tickers desde reference: {len(ref_tickers)}")
                all_tickers.update(ref_tickers)
    
    # Guardar lista
    print(f"\nTotal tickers únicos: {len(all_tickers)}")
    
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        for ticker in sorted(all_tickers):
            f.write(f"{ticker}\n")
    
    print(f"Guardado en: {output_path}")
    
    # Mostrar algunos ejemplos
    sample = sorted(all_tickers)[:10]
    print(f"\nPrimeros 10 tickers: {sample}")

if __name__ == '__main__':
    main()
