#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Verificación MASIVA y PARALELA de descargas intraday 1m.
Procesa todos los tickers del universe file y genera reporte de completitud.

Uso:
    python verify_all_intraday_1m.py --year-min 2019 --year-max 2025
    python verify_all_intraday_1m.py --year-min 2019 --year-max 2025 --workers 16
    python verify_all_intraday_1m.py --year-min 2019 --year-max 2025 --output-csv verification_report.csv
"""

import argparse
import polars as pl
from pathlib import Path
from datetime import datetime
from typing import Dict, Set, Tuple
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

# Fix Windows console encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')


def get_ticker_months_downloaded(data_dir: Path, ticker: str, year_min: int, year_max: int) -> Set[Tuple[int, int]]:
    """
    Obtiene qué meses tienen datos descargados para un ticker.

    Returns:
        Set de tuplas (year, month) con datos descargados
    """
    downloaded_months = set()
    ticker_path = data_dir / ticker

    if not ticker_path.exists():
        return downloaded_months

    for year in range(year_min, year_max + 1):
        year_path = ticker_path / f"year={year}"
        if not year_path.exists():
            continue

        for month in range(1, 13):
            month_dir = year_path / f"month={month:02d}"
            minute_file = month_dir / "minute.parquet"

            if minute_file.exists():
                try:
                    # Verificar que el archivo no esté vacío y tenga fechas válidas
                    df = pl.read_parquet(minute_file)

                    if len(df) > 0:
                        # Verificar que tenga fechas válidas (no 1970)
                        if 'date' in df.columns:
                            dates = df['date'].unique().to_list()
                            valid_dates = [d for d in dates if str(d).startswith(str(year))]
                            if valid_dates:
                                downloaded_months.add((year, month))
                        elif 'timestamp' in df.columns:
                            dates = df['timestamp'].dt.date().unique().to_list()
                            valid_dates = [d for d in dates if str(d).startswith(str(year))]
                            if valid_dates:
                                downloaded_months.add((year, month))
                except:
                    pass

    return downloaded_months


def get_ticker_expected_months(daily_root: Path, ticker: str, year_min: int, year_max: int) -> Set[Tuple[int, int]]:
    """
    Obtiene qué meses deberían tener datos según daily.

    Returns:
        Set de tuplas (year, month) que deberían existir
    """
    expected_months = set()

    for year in range(year_min, year_max + 1):
        daily_file = daily_root / ticker / f"year={year}" / "daily.parquet"

        if not daily_file.exists():
            continue

        try:
            df = pl.read_parquet(daily_file)

            # Extraer año-mes de cada fecha
            for date_str in df['date'].unique().to_list():
                try:
                    if isinstance(date_str, str):
                        year_month = date_str[:7]  # 'YYYY-MM'
                        y, m = int(year_month[:4]), int(year_month[5:7])
                    else:
                        y, m = date_str.year, date_str.month

                    if year_min <= y <= year_max:
                        expected_months.add((y, m))
                except:
                    continue
        except:
            pass

    return expected_months


def verify_single_ticker(args_tuple):
    """
    Verifica un ticker individual.

    Args:
        args_tuple: (ticker, data_dir, daily_root, year_min, year_max)

    Returns:
        Dict con resultados de verificación
    """
    ticker, data_dir, daily_root, year_min, year_max = args_tuple

    try:
        # Obtener meses descargados
        downloaded = get_ticker_months_downloaded(data_dir, ticker, year_min, year_max)

        # Obtener meses esperados
        expected = get_ticker_expected_months(daily_root, ticker, year_min, year_max)

        # Calcular estadísticas
        total_expected = len(expected)
        total_downloaded = len(downloaded)
        missing = expected - downloaded
        extra = downloaded - expected

        if total_expected == 0:
            status = 'NO_DAILY_DATA'
            completeness = 0.0
        elif len(missing) == 0:
            status = 'COMPLETE'
            completeness = 100.0
        else:
            status = 'INCOMPLETE'
            completeness = (total_downloaded / total_expected * 100) if total_expected > 0 else 0.0

        return {
            'ticker': ticker,
            'status': status,
            'expected_months': total_expected,
            'downloaded_months': total_downloaded,
            'missing_months': len(missing),
            'extra_months': len(extra),
            'completeness_pct': completeness,
            'missing_list': sorted(list(missing)),
            'extra_list': sorted(list(extra))
        }
    except Exception as e:
        return {
            'ticker': ticker,
            'status': 'ERROR',
            'expected_months': 0,
            'downloaded_months': 0,
            'missing_months': 0,
            'extra_months': 0,
            'completeness_pct': 0.0,
            'missing_list': [],
            'extra_list': [],
            'error': str(e)
        }


def main():
    parser = argparse.ArgumentParser(
        description='Verificación masiva de descargas intraday 1m',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  # Verificar 2019-2025 con 16 workers
  python verify_all_intraday_1m.py --year-min 2019 --year-max 2025 --workers 16

  # Verificar 2004-2018 y guardar reporte
  python verify_all_intraday_1m.py --year-min 2004 --year-max 2018 \\
    --data-dir "C:\\TSIS_Data\\ohlcv_intraday_1m\\2004_2018" \\
    --output-csv verification_2004_2018.csv

  # Solo verificar tickers incompletos
  python verify_all_intraday_1m.py --year-min 2019 --year-max 2025 \\
    --show-only incomplete
        """
    )

    parser.add_argument('--universe-file',
                       default='processed/universe/ping_range_2019_2025.parquet',
                       help='Archivo parquet con lista de tickers')
    parser.add_argument('--data-dir',
                       default=r"C:\TSIS_Data\ohlcv_intraday_1m\2019_2025",
                       help='Directorio base de datos intraday')
    parser.add_argument('--daily-root',
                       default='raw/polygon/ohlcv_daily',
                       help='Directorio de datos daily para comparar')
    parser.add_argument('--year-min', type=int, required=True,
                       help='Año inicial')
    parser.add_argument('--year-max', type=int, required=True,
                       help='Año final')
    parser.add_argument('--workers', type=int, default=8,
                       help='Número de workers paralelos (default: 8)')
    parser.add_argument('--output-csv',
                       help='Guardar reporte en CSV')
    parser.add_argument('--show-only', choices=['all', 'incomplete', 'complete', 'missing'],
                       default='all',
                       help='Filtrar resultados mostrados')
    parser.add_argument('--limit', type=int,
                       help='Limitar número de tickers a verificar (para testing)')

    args = parser.parse_args()

    # Cargar tickers
    print(f"Cargando tickers desde {args.universe_file}...")
    universe_df = pl.read_parquet(args.universe_file)

    # Filtrar por has_data si existe la columna
    if 'has_data' in universe_df.columns:
        tickers = universe_df.filter(pl.col('has_data') == True)['ticker'].to_list()
    else:
        tickers = universe_df['ticker'].unique().to_list()

    if args.limit:
        tickers = tickers[:args.limit]

    print(f"Total tickers a verificar: {len(tickers):,}")
    print(f"Período: {args.year_min}-{args.year_max}")
    print(f"Workers: {args.workers}")
    print()

    data_dir = Path(args.data_dir)
    daily_root = Path(args.daily_root)

    # Preparar argumentos para workers
    worker_args = [
        (ticker, data_dir, daily_root, args.year_min, args.year_max)
        for ticker in tickers
    ]

    # Procesar en paralelo con barra de progreso
    results = []
    print("Verificando tickers en paralelo...")

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        # Submit all tasks
        futures = {executor.submit(verify_single_ticker, arg): arg[0] for arg in worker_args}

        # Process with progress bar
        with tqdm(total=len(tickers), desc="Verificando", unit="ticker") as pbar:
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    ticker = futures[future]
                    results.append({
                        'ticker': ticker,
                        'status': 'ERROR',
                        'expected_months': 0,
                        'downloaded_months': 0,
                        'missing_months': 0,
                        'extra_months': 0,
                        'completeness_pct': 0.0,
                        'error': str(e)
                    })
                pbar.update(1)

    # Convertir a DataFrame
    df_results = pl.DataFrame(results)

    # Estadísticas generales
    print("\n" + "=" * 70)
    print("RESUMEN DE VERIFICACIÓN")
    print("=" * 70)

    complete = len(df_results.filter(pl.col('status') == 'COMPLETE'))
    incomplete = len(df_results.filter(pl.col('status') == 'INCOMPLETE'))
    no_data = len(df_results.filter(pl.col('status') == 'NO_DAILY_DATA'))
    errors = len(df_results.filter(pl.col('status') == 'ERROR'))

    total = len(df_results)

    print(f"Total tickers: {total:,}")
    print(f"  ✅ Completos:      {complete:,} ({complete/total*100:.1f}%)")
    print(f"  ⚠️  Incompletos:    {incomplete:,} ({incomplete/total*100:.1f}%)")
    print(f"  ℹ️  Sin daily data: {no_data:,} ({no_data/total*100:.1f}%)")
    print(f"  ❌ Errores:        {errors:,} ({errors/total*100:.1f}%)")

    # Completitud promedio
    avg_completeness = df_results.filter(pl.col('expected_months') > 0)['completeness_pct'].mean()
    if avg_completeness is not None:
        print(f"\nCompletitud promedio: {avg_completeness:.1f}%")

    # Meses totales
    total_expected = df_results['expected_months'].sum()
    total_downloaded = df_results['downloaded_months'].sum()
    total_missing = df_results['missing_months'].sum()

    print(f"\nMeses esperados:   {total_expected:,}")
    print(f"Meses descargados: {total_downloaded:,}")
    print(f"Meses faltantes:   {total_missing:,}")

    # Guardar CSV si se especifica
    if args.output_csv:
        # Para CSV, convertir listas a strings
        df_export = df_results.with_columns([
            pl.col('missing_list').map_elements(lambda x: str(x) if len(x) > 0 else '', return_dtype=pl.Utf8).alias('missing_months_detail'),
            pl.col('extra_list').map_elements(lambda x: str(x) if len(x) > 0 else '', return_dtype=pl.Utf8).alias('extra_months_detail')
        ]).drop(['missing_list', 'extra_list'])

        df_export.write_csv(args.output_csv)
        print(f"\n✅ Reporte guardado en: {args.output_csv}")

    # Mostrar detalles según filtro
    print("\n" + "=" * 70)

    if args.show_only == 'incomplete' or args.show_only == 'all':
        incomplete_df = df_results.filter(pl.col('status') == 'INCOMPLETE').sort('completeness_pct')

        if len(incomplete_df) > 0:
            print(f"TICKERS INCOMPLETOS ({len(incomplete_df)}):")
            print("-" * 70)

            for row in incomplete_df.head(20).iter_rows(named=True):
                missing_count = row['missing_months']
                completeness = row['completeness_pct']
                print(f"  {row['ticker']:6s}  {completeness:5.1f}%  "
                      f"({row['downloaded_months']}/{row['expected_months']} meses)  "
                      f"Faltan: {missing_count}")

            if len(incomplete_df) > 20:
                print(f"  ... y {len(incomplete_df) - 20} más")

    if args.show_only == 'missing' or args.show_only == 'all':
        missing_df = df_results.filter(pl.col('downloaded_months') == 0).filter(pl.col('expected_months') > 0)

        if len(missing_df) > 0:
            print(f"\nTICKERS SIN DATOS DESCARGADOS ({len(missing_df)}):")
            print("-" * 70)

            for row in missing_df.head(20).iter_rows(named=True):
                print(f"  {row['ticker']:6s}  Esperados: {row['expected_months']} meses")

            if len(missing_df) > 20:
                print(f"  ... y {len(missing_df) - 20} más")

    print("=" * 70)

    return 0


if __name__ == '__main__':
    exit(main())
