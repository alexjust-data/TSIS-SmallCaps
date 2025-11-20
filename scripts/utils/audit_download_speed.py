#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
audit_download_speed.py - Audita velocidad de descarga y progreso

Analiza rapidamente usando os.walk() sin glob para maxima velocidad.
Calcula estadisticas de descarga por ticker/año/mes.
"""

import os
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
import polars as pl

def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)

def scan_directory_fast(data_dir: str) -> dict:
    """
    Escanea directorio usando os.walk() para maxima velocidad.
    Retorna estructura: {ticker: {year: {month: file_count}}}
    """
    data_path = Path(data_dir)
    if not data_path.exists():
        raise FileNotFoundError(f"Directorio no existe: {data_dir}")

    structure = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    total_files = 0
    total_size_bytes = 0
    oldest_file = None
    newest_file = None

    log(f"Escaneando: {data_dir}")
    start = time.time()

    # Usar os.walk() es mucho mas rapido que glob
    for root, dirs, files in os.walk(data_dir):
        # Skip _batch_temp
        if '_batch_temp' in root:
            continue

        # Buscar archivos .parquet
        for filename in files:
            if not filename.endswith('.parquet'):
                continue

            total_files += 1
            filepath = os.path.join(root, filename)

            # Obtener size y mtime
            try:
                stat = os.stat(filepath)
                total_size_bytes += stat.st_size
                mtime = datetime.fromtimestamp(stat.st_mtime)

                if oldest_file is None or mtime < oldest_file:
                    oldest_file = mtime
                if newest_file is None or mtime > newest_file:
                    newest_file = mtime
            except:
                pass

            # Parsear estructura: ticker/year=YYYY/month=MM/...
            parts = Path(root).parts
            try:
                # Encontrar ticker (directorio padre antes de year=)
                ticker_idx = None
                for i, part in enumerate(parts):
                    if part.startswith('year='):
                        ticker_idx = i - 1
                        break

                if ticker_idx is None or ticker_idx < 0:
                    continue

                ticker = parts[ticker_idx]

                # Parsear year= y month=
                year = None
                month = None
                for part in parts[ticker_idx+1:]:
                    if part.startswith('year='):
                        year = part.split('=')[1]
                    elif part.startswith('month='):
                        month = part.split('=')[1]

                if ticker and year and month:
                    structure[ticker][year][month] += 1

            except Exception:
                continue

    elapsed = time.time() - start
    log(f"Escaneo completo en {elapsed:.2f}s")

    return {
        'structure': dict(structure),
        'total_files': total_files,
        'total_size_bytes': total_size_bytes,
        'oldest_file': oldest_file,
        'newest_file': newest_file,
        'scan_time': elapsed
    }

def calculate_statistics(scan_result: dict, total_tickers: int = None) -> dict:
    """Calcula estadisticas de descarga."""
    structure = scan_result['structure']

    # Contar tickers, years, months
    tickers_with_data = len(structure)
    total_years = set()
    total_months = set()

    for ticker, years in structure.items():
        for year, months in years.items():
            total_years.add(year)
            for month in months.keys():
                total_months.add(f"{year}-{month}")

    # Calcular velocidad si hay timestamps
    download_rate = None
    if scan_result['oldest_file'] and scan_result['newest_file']:
        time_span = scan_result['newest_file'] - scan_result['oldest_file']
        if time_span.total_seconds() > 0:
            files_per_second = scan_result['total_files'] / time_span.total_seconds()
            download_rate = {
                'files_per_second': files_per_second,
                'files_per_minute': files_per_second * 60,
                'files_per_hour': files_per_second * 3600,
                'files_per_day': files_per_second * 86400,
                'time_span_hours': time_span.total_seconds() / 3600,
                'time_span_days': time_span.total_seconds() / 86400
            }

    # Progreso
    progress = {}
    if total_tickers:
        progress['tickers_completed'] = tickers_with_data
        progress['tickers_pending'] = total_tickers - tickers_with_data
        progress['percent_complete'] = (tickers_with_data / total_tickers) * 100

    return {
        'tickers_with_data': tickers_with_data,
        'unique_years': len(total_years),
        'unique_months': len(total_months),
        'total_files': scan_result['total_files'],
        'total_size_gb': scan_result['total_size_bytes'] / (1024**3),
        'download_rate': download_rate,
        'progress': progress,
        'oldest_file': scan_result['oldest_file'],
        'newest_file': scan_result['newest_file']
    }

def print_report(stats: dict):
    """Imprime reporte formateado."""
    print("\n" + "="*80)
    print("REPORTE DE AUDITORIA DE DESCARGA")
    print("="*80 + "\n")

    print(f"Tickers con datos:      {stats['tickers_with_data']:,}")
    print(f"Anos unicos:            {stats['unique_years']}")
    print(f"Meses unicos:           {stats['unique_months']}")
    print(f"Total archivos:         {stats['total_files']:,}")
    print(f"Tamano total:           {stats['total_size_gb']:.2f} GB")

    if stats['progress']:
        print(f"\n--- PROGRESO ---")
        print(f"Completados:            {stats['progress']['tickers_completed']:,} / {stats['progress']['tickers_completed'] + stats['progress']['tickers_pending']:,}")
        print(f"Pendientes:             {stats['progress']['tickers_pending']:,}")
        print(f"Progreso:               {stats['progress']['percent_complete']:.1f}%")

    if stats['download_rate']:
        dr = stats['download_rate']
        print(f"\n--- VELOCIDAD DE DESCARGA ---")
        print(f"Periodo analizado:      {dr['time_span_days']:.2f} dias ({dr['time_span_hours']:.1f} horas)")
        print(f"Archivos/segundo:       {dr['files_per_second']:.2f}")
        print(f"Archivos/minuto:        {dr['files_per_minute']:.1f}")
        print(f"Archivos/hora:          {dr['files_per_hour']:.0f}")
        print(f"Archivos/dia:           {dr['files_per_day']:.0f}")

        # Estimacion tiempo restante
        if stats['progress'] and stats['progress']['tickers_pending'] > 0:
            # Asumir ~3650 archivos por ticker (15 años x 12 meses x ~20 dias)
            avg_files_per_ticker = stats['total_files'] / stats['tickers_with_data'] if stats['tickers_with_data'] > 0 else 3650
            remaining_files = stats['progress']['tickers_pending'] * avg_files_per_ticker
            days_remaining = remaining_files / dr['files_per_day']

            print(f"\n--- ESTIMACION TIEMPO RESTANTE ---")
            print(f"Archivos promedio/ticker: {avg_files_per_ticker:.0f}")
            print(f"Archivos pendientes:      {remaining_files:,.0f}")
            print(f"Dias estimados restantes: {days_remaining:.1f}")
            print(f"Fecha estimada fin:       {(datetime.now() + timedelta(days=days_remaining)).strftime('%Y-%m-%d')}")

    if stats['oldest_file'] and stats['newest_file']:
        print(f"\n--- TIMESTAMPS ---")
        print(f"Archivo mas antiguo:    {stats['oldest_file'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Archivo mas reciente:   {stats['newest_file'].strftime('%Y-%m-%d %H:%M:%S')}")

    print("\n" + "="*80 + "\n")

def export_ticker_summary(structure: dict, output_path: str):
    """Exporta resumen por ticker a CSV."""
    rows = []
    for ticker, years in structure.items():
        year_count = len(years)
        month_count = sum(len(months) for months in years.values())
        file_count = sum(sum(months.values()) for months in years.values())

        rows.append({
            'ticker': ticker,
            'years': year_count,
            'months': month_count,
            'files': file_count
        })

    df = pl.DataFrame(rows).sort('ticker')
    df.write_csv(output_path)
    log(f"Resumen exportado: {output_path}")

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Auditoria rapida de descarga")
    parser.add_argument('--data-dir', required=True, help='Directorio de datos (ej: C:\\TSIS_Data\\trades_ticks_2004_2018_v2)')
    parser.add_argument('--total-tickers', type=int, help='Total de tickers esperados (para calcular progreso)')
    parser.add_argument('--export-csv', help='Ruta para exportar resumen por ticker')
    args = parser.parse_args()

    # Escanear
    scan_result = scan_directory_fast(args.data_dir)

    # Calcular stats
    stats = calculate_statistics(scan_result, args.total_tickers)

    # Imprimir reporte
    print_report(stats)

    # Exportar CSV si se solicita
    if args.export_csv:
        export_ticker_summary(scan_result['structure'], args.export_csv)

if __name__ == '__main__':
    main()
