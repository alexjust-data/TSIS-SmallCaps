#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Verifica qué días de intraday 1m se han descargado para un ticker específico.
Permite especificar año y opcionalmente mes para ver el progreso de descarga.

Uso:
    python show_intraday_1m_days.py BMGL --year 2025
    python show_intraday_1m_days.py BMGL --year 2025 --month 11
    python show_intraday_1m_days.py BMGL --year 2025 --data-dir "C:\\TSIS_Data\\ohlcv_intraday_1m\\2019_2025"
"""

import argparse
import polars as pl
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from typing import Set, Dict, List
import sys

# Fix Windows console encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')


def get_downloaded_days(data_dir: Path, ticker: str, year: int, month: int = None) -> Set[str]:
    """
    Obtiene las fechas que tienen datos descargados para un ticker/año/mes.

    Returns:
        Set de fechas en formato 'YYYY-MM-DD'
    """
    downloaded = set()
    ticker_path = data_dir / ticker

    if not ticker_path.exists():
        return downloaded

    # Determinar qué meses revisar
    if month:
        months_to_check = [month]
    else:
        months_to_check = range(1, 13)

    for m in months_to_check:
        month_dir = ticker_path / f"year={year}" / f"month={m:02d}"
        minute_file = month_dir / "minute.parquet"

        if minute_file.exists():
            try:
                df = pl.read_parquet(minute_file)

                # La columna puede ser 'date' o 'timestamp'
                if 'date' in df.columns:
                    # Filtrar valores nulos y convertir
                    dates = df.filter(pl.col('date').is_not_null())['date'].unique().to_list()
                elif 'timestamp' in df.columns:
                    # Filtrar timestamps nulos/inválidos y extraer fecha
                    dates = (df
                            .filter(pl.col('timestamp').is_not_null())
                            .select(pl.col('timestamp').dt.date().alias('date'))
                            .unique()['date']
                            .to_list())
                else:
                    continue

                # Convertir a strings y filtrar fechas inválidas (epoch)
                for d in dates:
                    if d is None:
                        continue

                    date_str = str(d) if not isinstance(d, str) else d

                    # Skip fechas inválidas (epoch = 1970-01-01)
                    if date_str.startswith('1970'):
                        continue

                    # Solo incluir fechas del año correcto
                    if date_str.startswith(str(year)):
                        downloaded.add(date_str)

            except Exception as e:
                print(f"  [WARNING] Error leyendo {minute_file}: {e}")
                continue

    return downloaded


def get_expected_trading_days(daily_root: Path, ticker: str, year: int, month: int = None) -> Set[str]:
    """
    Obtiene las fechas esperadas desde los datos daily.

    Returns:
        Set de fechas en formato 'YYYY-MM-DD'
    """
    expected = set()
    daily_file = daily_root / ticker / f"year={year}" / "daily.parquet"

    if not daily_file.exists():
        return expected

    try:
        df = pl.read_parquet(daily_file)

        # Filtrar por mes si se especifica
        if month:
            month_str = f"{year:04d}-{month:02d}"
            df = df.filter(pl.col('date').str.starts_with(month_str))

        dates = df['date'].unique().to_list()
        expected = {str(d) if not isinstance(d, str) else d for d in dates}

    except Exception as e:
        print(f"  [WARNING] No se pudo leer daily data: {e}")

    return expected


def format_days_by_month(dates: List[str]) -> Dict[str, List[int]]:
    """
    Agrupa fechas por mes y extrae solo los días.

    Args:
        dates: Lista de strings 'YYYY-MM-DD'

    Returns:
        Dict {month_str: [days]}
    """
    by_month = defaultdict(list)

    for date_str in sorted(dates):
        try:
            # date_str puede ser 'YYYY-MM-DD' o un objeto date
            if isinstance(date_str, str):
                month = date_str[:7]  # 'YYYY-MM'
                day = int(date_str[8:10])
            else:
                month = f"{date_str.year:04d}-{date_str.month:02d}"
                day = date_str.day

            by_month[month].append(day)
        except:
            continue

    return by_month


def print_report(ticker: str, year: int, month: int,
                downloaded: Set[str], expected: Set[str],
                data_dir: Path):
    """
    Imprime reporte de descarga.
    """
    print()
    print("=" * 70)
    print(f"VERIFICACIÓN DE DESCARGA INTRADAY 1M")
    print("=" * 70)
    print(f"Ticker      : {ticker}")
    print(f"Año         : {year}")
    if month:
        print(f"Mes         : {month:02d}")
    print(f"Directorio  : {data_dir / ticker}")
    print()

    # Estadísticas
    total_expected = len(expected)
    total_downloaded = len(downloaded)
    missing = expected - downloaded
    extra = downloaded - expected

    if total_expected > 0:
        pct_complete = (total_downloaded / total_expected) * 100
        print(f"Progreso    : {total_downloaded}/{total_expected} días ({pct_complete:.1f}%)")
    else:
        print(f"Descargados : {total_downloaded} días")
        print(f"Esperados   : {total_expected} días (no hay datos daily para comparar)")

    print()

    # Días descargados por mes
    if downloaded:
        print("-" * 70)
        print("DÍAS DESCARGADOS POR MES:")
        print("-" * 70)

        by_month = format_days_by_month(list(downloaded))
        for month_str in sorted(by_month.keys()):
            days = by_month[month_str]
            days_str = ','.join(str(d) for d in sorted(days))
            print(f"{month_str}    {days_str}")
        print()

    # Días faltantes
    if missing:
        print("-" * 70)
        print(f"DÍAS FALTANTES ({len(missing)}):")
        print("-" * 70)

        by_month = format_days_by_month(list(missing))
        for month_str in sorted(by_month.keys()):
            days = by_month[month_str]
            days_str = ','.join(str(d) for d in sorted(days))
            print(f"{month_str}    {days_str}")
        print()

    # Días extra (descargados pero no esperados)
    if extra:
        print("-" * 70)
        print(f"DÍAS EXTRA/INESPERADOS ({len(extra)}):")
        print("-" * 70)

        by_month = format_days_by_month(list(extra))
        for month_str in sorted(by_month.keys()):
            days = by_month[month_str]
            days_str = ','.join(str(d) for d in sorted(days))
            print(f"{month_str}    {days_str}")
        print()

    # Resumen final
    print("=" * 70)
    if total_expected > 0:
        if len(missing) == 0:
            print("✓ DESCARGA COMPLETA")
        else:
            print(f"⚠ INCOMPLETO - Faltan {len(missing)} días")
    else:
        print(f"ℹ {total_downloaded} días descargados (sin referencia daily para validar)")
    print("=" * 70)
    print()


def main():
    parser = argparse.ArgumentParser(
        description='Verifica descarga de datos intraday 1m',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  # Ver todos los meses de 2025 para BMGL
  python show_intraday_1m_days.py BMGL --year 2025

  # Ver solo noviembre 2025
  python show_intraday_1m_days.py BMGL --year 2025 --month 11

  # Especificar directorio personalizado
  python show_intraday_1m_days.py BMGL --year 2025 --data-dir "C:\\TSIS_Data\\ohlcv_intraday_1m\\2019_2025"
        """
    )

    parser.add_argument('ticker', help='Ticker a verificar (ej: BMGL)')
    parser.add_argument('--year', type=int, required=True, help='Año a verificar')
    parser.add_argument('--month', type=int, help='Mes específico (1-12, opcional)')
    parser.add_argument('--data-dir',
                       default=r"C:\TSIS_Data\ohlcv_intraday_1m\2019_2025",
                       help='Directorio base de datos intraday')
    parser.add_argument('--daily-root',
                       default='raw/polygon/ohlcv_daily',
                       help='Directorio de datos daily para comparar')

    args = parser.parse_args()

    # Validaciones
    if args.month and (args.month < 1 or args.month > 12):
        print("ERROR: --month debe estar entre 1 y 12")
        return 1

    data_dir = Path(args.data_dir)
    daily_root = Path(args.daily_root)

    if not data_dir.exists():
        print(f"ERROR: Directorio no existe: {data_dir}")
        return 1

    # Obtener días descargados
    print(f"Escaneando {data_dir / args.ticker}...")
    downloaded = get_downloaded_days(data_dir, args.ticker, args.year, args.month)

    # Obtener días esperados (si daily data existe)
    expected = set()
    if daily_root.exists():
        expected = get_expected_trading_days(daily_root, args.ticker, args.year, args.month)

    # Imprimir reporte
    print_report(args.ticker, args.year, args.month, downloaded, expected, data_dir)

    return 0


if __name__ == '__main__':
    exit(main())
