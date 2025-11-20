#!/usr/bin/env python3
"""
Verifica ticks vs daily para todos los tickers en un periodo.
Usa el daily local como fuente de verdad (ya verificado 100% vs Polygon).
"""

import polars as pl
import sys
import os
import argparse
from pathlib import Path
from datetime import datetime

def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)

def verify_ticker_ticks(ticker, daily_root, ticks_root, year_min=None, year_max=None):
    """
    Verifica ticks para un ticker específico.
    year_min, year_max: Filtrar solo años en este rango (ej: 2004-2018)
    Retorna dict con estadísticas.
    """
    # Encontrar todos los años del ticker en daily
    ticker_path = Path(daily_root) / ticker
    if not ticker_path.exists():
        return None

    year_dirs = [d for d in ticker_path.iterdir() if d.is_dir() and d.name.startswith('year=')]

    if not year_dirs:
        return None

    total_daily_days = 0
    total_ticks_days = 0
    missing_dates = []
    first_date = None
    last_date = None

    # Procesar cada año
    for year_dir in sorted(year_dirs):
        # Extraer año del nombre del directorio
        year_str = year_dir.name.replace('year=', '')
        year_int = int(year_str)

        # Filtrar por rango de años si se especifica
        if year_min is not None and year_int < year_min:
            continue
        if year_max is not None and year_int > year_max:
            continue

        daily_file = year_dir / 'daily.parquet'

        try:
            if not daily_file.exists():
                continue

            # Leer fechas del daily
            df = pl.read_parquet(daily_file)
            dates = df.select('date').to_series().to_list()

            total_daily_days += len(dates)

            if first_date is None:
                first_date = dates[0]
            last_date = dates[-1]

            # Verificar cada fecha
            for date_str in dates:
                # Construir path esperado
                date_parts = date_str.split('-')
                y = date_parts[0]
                m = date_parts[1]

                market_file = Path(ticks_root) / ticker / f'year={y}' / f'month={m}' / f'day={date_str}' / 'market.parquet'

                if market_file.exists():
                    total_ticks_days += 1
                else:
                    missing_dates.append(date_str)

        except (PermissionError, OSError) as e:
            # Archivo corrupto o error de CRC - skip este año
            continue

    # Determinar status
    if total_daily_days == 0:
        return None

    if total_ticks_days == total_daily_days:
        status = "COMPLETE"
        issue = ""
    elif total_ticks_days == 0:
        status = "MISSING_TICKER"
        issue = "No existe carpeta o sin ticks"
    else:
        status = "MISSING_DAYS"
        issue = f"Faltan {len(missing_dates)} días"

    return {
        'ticker': ticker,
        'daily_first': first_date,
        'daily_last': last_date,
        'daily_days': total_daily_days,
        'ticks_days': total_ticks_days,
        'missing_days': len(missing_dates),
        'status': status,
        'issue': issue,
        'missing_dates_list': missing_dates
    }

def main():
    parser = argparse.ArgumentParser(description='Verificar ticks vs daily para todos los tickers')
    parser.add_argument('--daily-root', required=True, help='Path to daily OHLCV root (ej: raw/polygon/ohlcv_daily)')
    parser.add_argument('--ticks-root', required=True, help='Path to ticks root (ej: C:\\TSIS_Data\\trades_ticks_2004_2018_v2)')
    parser.add_argument('--ping-range', required=True, help='Path to ping_range parquet (ej: processed/universe/ping_range_2004_2018.parquet)')
    parser.add_argument('--output-prefix', required=True, help='Prefix for output files (ej: verify_ticks_2004_2018)')
    parser.add_argument('--year-min', type=int, help='Año mínimo a verificar (ej: 2004)')
    parser.add_argument('--year-max', type=int, help='Año máximo a verificar (ej: 2018)')

    args = parser.parse_args()

    log("=" * 80)
    log("VERIFICACIÓN TICKS vs DAILY (usando daily local como fuente de verdad)")
    log("=" * 80)

    # Cargar ping_range para obtener lista de tickers
    log(f"Cargando ping desde {args.ping_range}")
    ping_df = pl.read_parquet(args.ping_range)
    tickers_with_data = ping_df.filter(pl.col('has_data') == True).select('ticker').to_series().to_list()
    log(f"  > {len(tickers_with_data):,} tickers con datos según ping")

    # Mostrar rango de años si se especifica
    if args.year_min or args.year_max:
        year_range = f"{args.year_min or 'inicio'} - {args.year_max or 'fin'}"
        log(f"Filtrando años: {year_range}")

    # Verificar cada ticker
    log("Verificando tickers...")
    results = []
    missing_dates_all = []

    for i, ticker in enumerate(tickers_with_data):
        if (i + 1) % 500 == 0:
            log(f"  Progreso: {i+1}/{len(tickers_with_data)} ({(i+1)/len(tickers_with_data)*100:.1f}%)")

        result = verify_ticker_ticks(ticker, args.daily_root, args.ticks_root, args.year_min, args.year_max)

        if result:
            completeness_pct = (result['ticks_days'] / result['daily_days'] * 100) if result['daily_days'] > 0 else 0

            results.append({
                'ticker': result['ticker'],
                'status': result['status'],
                'completeness_pct': round(completeness_pct, 1),
                'daily_first': result['daily_first'],
                'daily_last': result['daily_last'],
                'daily_days': result['daily_days'],
                'ticks_found': result['ticks_days'],
                'ticks_missing': result['missing_days'],
                'issue': result['issue']
            })

            # Guardar fechas faltantes
            for date in result['missing_dates_list']:
                missing_dates_all.append({
                    'ticker': ticker,
                    'missing_date': date
                })

    log(f"  Progreso: {len(tickers_with_data)}/{len(tickers_with_data)} (100.0%)")
    log("")

    # Crear DataFrame de resultados
    results_df = pl.DataFrame(results)

    # Estadísticas
    log("=" * 80)
    log("RESULTADOS")
    log("=" * 80)

    total = len(results_df)
    complete = len(results_df.filter(pl.col('status') == 'COMPLETE'))
    missing_days = len(results_df.filter(pl.col('status') == 'MISSING_DAYS'))
    missing_ticker = len(results_df.filter(pl.col('status') == 'MISSING_TICKER'))

    log(f"Total tickers verificados:       {total:,}")
    log(f"  ✅ COMPLETOS:                   {complete:,} ({complete/total*100:.1f}%)")
    log(f"  ⚠️  DÍAS FALTANTES:              {missing_days:,} ({missing_days/total*100:.1f}%)")
    log(f"  ❌ TICKER FALTANTE:              {missing_ticker:,} ({missing_ticker/total*100:.1f}%)")
    log("")

    # Guardar archivos
    log("Guardando archivos...")

    # Full results
    full_csv = f"{args.output_prefix}_full.csv"
    results_df.write_csv(full_csv)
    log(f"  ✅ {full_csv}")

    # Problems only
    problems_df = results_df.filter(pl.col('status') != 'COMPLETE')
    if len(problems_df) > 0:
        problems_csv = f"{args.output_prefix}_problems.csv"
        problems_df.write_csv(problems_csv)
        log(f"  ✅ {problems_csv}")

    # Missing dates
    if missing_dates_all:
        missing_dates_df = pl.DataFrame(missing_dates_all)
        missing_csv = f"{args.output_prefix}_missing_dates.csv"
        missing_dates_df.write_csv(missing_csv)
        log(f"  ✅ {missing_csv}")
        log(f"     Total fechas faltantes: {len(missing_dates_all):,}")

    log("")
    log("=" * 80)
    log("VERIFICACIÓN COMPLETADA")
    log("=" * 80)

if __name__ == '__main__':
    main()
