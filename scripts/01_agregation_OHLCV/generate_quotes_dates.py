#!/usr/bin/env python3
"""
Genera CSV con todas las fechas de trading para descargar quotes.
Usa el daily OHLCV local como fuente de verdad (ya verificado 100%).
"""

import polars as pl
import sys
import argparse
from pathlib import Path
from datetime import datetime

def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)

def main():
    parser = argparse.ArgumentParser(description='Generar CSV de fechas para descargar quotes')
    parser.add_argument('--daily-root', required=True, help='Path to daily OHLCV root (ej: raw/polygon/ohlcv_daily)')
    parser.add_argument('--ping-range', required=True, help='Path to ping_range parquet')
    parser.add_argument('--output-csv', required=True, help='Output CSV file')
    parser.add_argument('--year-min', type=int, help='Año mínimo (ej: 2004)')
    parser.add_argument('--year-max', type=int, help='Año máximo (ej: 2018)')
    parser.add_argument('--quotes-root', help='Path to existing quotes (para skip existentes)')

    args = parser.parse_args()

    log("=" * 80)
    log("GENERADOR DE FECHAS PARA QUOTES")
    log("=" * 80)

    # Cargar ping_range para obtener lista de tickers
    log(f"Cargando ping desde {args.ping_range}")
    ping_df = pl.read_parquet(args.ping_range)
    tickers_with_data = ping_df.filter(pl.col('has_data') == True).select('ticker').to_series().to_list()
    log(f"  > {len(tickers_with_data):,} tickers con datos")

    if args.year_min or args.year_max:
        log(f"Filtrando años: {args.year_min or 'inicio'} - {args.year_max or 'fin'}")

    # Recolectar todas las fechas
    log("Recolectando fechas de daily...")
    all_dates = []

    for i, ticker in enumerate(tickers_with_data):
        if (i + 1) % 500 == 0:
            log(f"  Progreso: {i+1}/{len(tickers_with_data)} ({(i+1)/len(tickers_with_data)*100:.1f}%)")

        ticker_path = Path(args.daily_root) / ticker
        if not ticker_path.exists():
            continue

        year_dirs = [d for d in ticker_path.iterdir() if d.is_dir() and d.name.startswith('year=')]

        for year_dir in sorted(year_dirs):
            try:
                year_str = year_dir.name.replace('year=', '')
                year_int = int(year_str)

                # Filtrar por rango de años
                if args.year_min is not None and year_int < args.year_min:
                    continue
                if args.year_max is not None and year_int > args.year_max:
                    continue

                daily_file = year_dir / 'daily.parquet'

                # Intentar leer directamente (evita problemas con exists() en archivos corruptos)
                df = pl.read_parquet(daily_file)
                dates = df.select('date').to_series().to_list()

                for date_str in dates:
                    # Verificar si quote ya existe (si se especifica quotes-root)
                    if args.quotes_root:
                        y, m, _ = date_str.split('-')
                        quotes_file = Path(args.quotes_root) / ticker / f'year={y}' / f'month={m}' / f'day={date_str}' / 'quotes.parquet'
                        try:
                            if quotes_file.exists():
                                continue
                        except (PermissionError, OSError):
                            pass  # Si no podemos verificar, lo añadimos

                    all_dates.append({
                        'ticker': ticker,
                        'date': date_str
                    })

            except (PermissionError, OSError, Exception) as e:
                # Archivo corrupto (CRC error) u otro problema - skip
                continue

    log(f"  Progreso: {len(tickers_with_data)}/{len(tickers_with_data)} (100.0%)")

    # Crear DataFrame y guardar
    log("")
    log(f"Total fechas recolectadas: {len(all_dates):,}")

    if all_dates:
        dates_df = pl.DataFrame(all_dates)

        # Estadísticas
        unique_tickers = dates_df.select('ticker').unique().height
        log(f"  Tickers únicos: {unique_tickers:,}")

        # Guardar
        dates_df.write_csv(args.output_csv)
        log(f"  Guardado en: {args.output_csv}")
    else:
        log("  No hay fechas para descargar")

    log("")
    log("=" * 80)
    log("COMPLETADO")
    log("=" * 80)

if __name__ == '__main__':
    main()
