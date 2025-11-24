#!/usr/bin/env python3
"""
Verificación mejorada de intraday 1m vs daily con paralelización y validación de integridad.
Mejoras:
- Procesamiento paralelo con concurrent.futures
- Validación de integridad de archivos parquet
- Detección de gaps intraday (minutos faltantes)
- Estadísticas de calidad de datos
- Cache de resultados para re-verificaciones rápidas
"""

import polars as pl
import sys
import os
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
import json
import hashlib

def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)

def get_months_from_daily(ticker, daily_root, year_min=None, year_max=None):
    """
    Obtiene lista de año-mes únicos desde los datos daily con validación.
    Retorna dict con metadata adicional: {(year, month): {'trading_days': count, 'volume_total': sum}}
    """
    ticker_path = Path(daily_root) / ticker
    if not ticker_path.exists():
        return {}

    year_dirs = [d for d in ticker_path.iterdir() if d.is_dir() and d.name.startswith('year=')]
    if not year_dirs:
        return {}

    months_data = {}

    for year_dir in sorted(year_dirs):
        year_str = year_dir.name.replace('year=', '')
        year_int = int(year_str)

        # Filtrar por rango de años
        if year_min is not None and year_int < year_min:
            continue
        if year_max is not None and year_int > year_max:
            continue

        daily_file = year_dir / 'daily.parquet'

        try:
            if not daily_file.exists():
                continue

            # Leer y validar datos daily
            df = pl.read_parquet(daily_file)
            
            # Validación de integridad
            if df.is_empty() or 'date' not in df.columns:
                continue
            
            # Agrupar por mes para obtener estadísticas
            df_with_month = df.with_columns([
                pl.col('date').str.slice(0, 7).alias('year_month')
            ])
            
            monthly_stats = df_with_month.group_by('year_month').agg([
                pl.len().alias('trading_days'),
                pl.col('volume').sum().alias('volume_total') if 'volume' in df.columns else pl.lit(0).alias('volume_total')
            ])
            
            for row in monthly_stats.iter_rows(named=True):
                year_month = row['year_month']
                y, m = year_month.split('-')
                months_data[(y, m)] = {
                    'trading_days': row['trading_days'],
                    'volume_total': row['volume_total']
                }

        except Exception as e:
            log(f"  WARNING: Error leyendo {ticker}/{year_str}: {e}")
            continue

    return months_data

def verify_intraday_quality(minute_file, expected_days):
    """
    Verifica la calidad de los datos intraday.
    Retorna dict con métricas de calidad.
    """
    try:
        df = pl.read_parquet(minute_file)
        
        if df.is_empty():
            return {'status': 'EMPTY', 'rows': 0}
        
        # Contar días únicos
        unique_days = df.select('date').unique().height if 'date' in df.columns else 0
        
        # Detectar gaps (minutos faltantes durante horario de trading)
        if 'minute' in df.columns:
            df_time = df.with_columns([
                pl.col('minute').str.slice(11, 5).alias('time')
            ])
            
            # Filtrar solo horario regular (9:30-16:00 ET)
            df_regular = df_time.filter(
                (pl.col('time') >= '09:30') & 
                (pl.col('time') <= '16:00')
            )
            
            # Minutos esperados por día de trading: 390 (6.5 horas * 60)
            expected_minutes = unique_days * 390
            actual_minutes = df_regular.height
            completeness = (actual_minutes / expected_minutes * 100) if expected_minutes > 0 else 0
        else:
            completeness = 0
        
        return {
            'status': 'OK',
            'rows': df.height,
            'unique_days': unique_days,
            'expected_days': expected_days,
            'completeness_pct': round(completeness, 1),
            'file_size_mb': minute_file.stat().st_size / (1024 * 1024)
        }
        
    except Exception as e:
        return {'status': 'CORRUPT', 'error': str(e)}

def verify_ticker_parallel(args):
    """
    Función para verificación paralela de un ticker.
    """
    ticker, daily_root, intraday_root, year_min, year_max = args
    
    # Obtener meses esperados con metadata
    months_data = get_months_from_daily(ticker, daily_root, year_min, year_max)
    
    if not months_data:
        return None
    
    # Verificar cada mes
    found_months = {}
    missing_months = []
    quality_issues = []
    
    for (year, month), metadata in months_data.items():
        minute_file = Path(intraday_root) / ticker / f'year={year}' / f'month={month}' / 'minute.parquet'
        
        if minute_file.exists():
            # Verificar calidad
            quality = verify_intraday_quality(minute_file, metadata['trading_days'])
            found_months[(year, month)] = quality
            
            # Detectar problemas de calidad
            if quality['status'] != 'OK':
                quality_issues.append(f"{year}-{month}: {quality['status']}")
            elif 'completeness_pct' in quality and quality['completeness_pct'] < 80:
                quality_issues.append(f"{year}-{month}: {quality['completeness_pct']}% complete")
        else:
            missing_months.append((year, month))
    
    # Calcular estadísticas
    total_months = len(months_data)
    found_count = len(found_months)
    
    # Determinar status
    if found_count == total_months and not quality_issues:
        status = "COMPLETE"
        issue = ""
    elif found_count == 0:
        status = "MISSING_TICKER"
        issue = "No intraday data"
    elif quality_issues:
        status = "QUALITY_ISSUES"
        issue = f"{len(quality_issues)} quality issues"
    else:
        status = "MISSING_MONTHS"
        issue = f"Missing {len(missing_months)} months"
    
    # Calcular completeness promedio ponderada por volumen
    total_volume = sum(m['volume_total'] for m in months_data.values())
    weighted_completeness = 0
    
    if total_volume > 0:
        for (y, m), metadata in months_data.items():
            weight = metadata['volume_total'] / total_volume
            if (y, m) in found_months and 'completeness_pct' in found_months[(y, m)]:
                weighted_completeness += found_months[(y, m)]['completeness_pct'] * weight
    
    return {
        'ticker': ticker,
        'expected_months': total_months,
        'found_months': found_count,
        'missing_months_count': len(missing_months),
        'status': status,
        'issue': issue,
        'missing_months_list': missing_months,
        'quality_issues': quality_issues,
        'weighted_completeness': round(weighted_completeness, 1),
        'total_size_mb': sum(q.get('file_size_mb', 0) for q in found_months.values())
    }

def load_cache(cache_file):
    """Carga cache de verificaciones previas."""
    if cache_file.exists():
        with open(cache_file, 'r') as f:
            return json.load(f)
    return {}

def save_cache(cache_file, cache_data):
    """Guarda cache de verificaciones."""
    with open(cache_file, 'w') as f:
        json.dump(cache_data, f)

def main():
    parser = argparse.ArgumentParser(description='Verificación mejorada de intraday vs daily')
    parser.add_argument('--daily-root', required=True, help='Path to daily OHLCV root')
    parser.add_argument('--intraday-root', required=True, help='Path to intraday 1m root')
    parser.add_argument('--ping-range', required=True, help='Path to ping_range parquet')
    parser.add_argument('--output-prefix', required=True, help='Prefix for output files')
    parser.add_argument('--year-min', type=int, help='Año mínimo a verificar')
    parser.add_argument('--year-max', type=int, help='Año máximo a verificar')
    parser.add_argument('--workers', type=int, default=4, help='Número de workers paralelos (default: 4)')
    parser.add_argument('--use-cache', action='store_true', help='Usar cache para acelerar re-verificaciones')
    parser.add_argument('--quality-threshold', type=float, default=80.0, help='Umbral de calidad mínima % (default: 80)')

    args = parser.parse_args()

    log("=" * 80)
    log("VERIFICACIÓN MEJORADA INTRADAY vs DAILY")
    log("=" * 80)

    # Cache handling
    cache_file = Path(f"{args.output_prefix}_cache.json")
    cache = load_cache(cache_file) if args.use_cache else {}

    # Cargar tickers
    log(f"Cargando ping desde {args.ping_range}")
    ping_df = pl.read_parquet(args.ping_range)
    tickers_with_data = ping_df.filter(pl.col('has_data') == True).select('ticker').to_series().to_list()
    log(f"  > {len(tickers_with_data):,} tickers con datos")

    # Filtrar por cache si está habilitado
    cached_tickers = set()  # Inicializar siempre
    if args.use_cache and cache:
        cached_tickers = set(cache.keys())
        tickers_to_verify = [t for t in tickers_with_data if t not in cached_tickers]
        log(f"  > {len(cached_tickers):,} tickers en cache (skip)")
        log(f"  > {len(tickers_to_verify):,} tickers para verificar")
    else:
        tickers_to_verify = tickers_with_data

    if args.year_min or args.year_max:
        log(f"Filtrando años: {args.year_min or 'inicio'} - {args.year_max or 'fin'}")

    # Preparar argumentos para procesamiento paralelo
    verify_args = [
        (ticker, args.daily_root, args.intraday_root, args.year_min, args.year_max)
        for ticker in tickers_to_verify
    ]

    # Procesamiento paralelo
    log(f"Verificando tickers con {args.workers} workers...")
    results = []
    missing_months_all = []
    quality_issues_all = []

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(verify_ticker_parallel, arg): arg[0] for arg in verify_args}
        
        completed = 0
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                result = future.result()
                if result:
                    completed += 1
                    
                    # Actualizar cache
                    if args.use_cache:
                        cache[ticker] = {
                            'status': result['status'],
                            'weighted_completeness': result['weighted_completeness'],
                            'timestamp': datetime.now().isoformat()
                        }
                    
                    # Preparar resultado para DataFrame
                    results.append({
                        'ticker': result['ticker'],
                        'status': result['status'],
                        'completeness_pct': result['weighted_completeness'],
                        'expected_months': result['expected_months'],
                        'found_months': result['found_months'],
                        'missing_months': result['missing_months_count'],
                        'issue': result['issue'],
                        'size_mb': result['total_size_mb']
                    })
                    
                    # Guardar detalles de meses faltantes
                    for year, month in result['missing_months_list']:
                        missing_months_all.append({
                            'ticker': ticker,
                            'year': year,
                            'month': month
                        })
                    
                    # Guardar problemas de calidad
                    for issue in result['quality_issues']:
                        quality_issues_all.append({
                            'ticker': ticker,
                            'issue': issue
                        })
                    
                    if completed % 100 == 0:
                        log(f"  Progreso: {completed}/{len(tickers_to_verify)} "
                            f"({completed/len(tickers_to_verify)*100:.1f}%)")
                        
            except Exception as e:
                log(f"  ERROR procesando {ticker}: {e}")

    # Agregar resultados del cache
    if args.use_cache:
        for ticker in cached_tickers:
            if ticker in tickers_with_data:
                cache_data = cache[ticker]
                results.append({
                    'ticker': ticker,
                    'status': cache_data['status'],
                    'completeness_pct': cache_data['weighted_completeness'],
                    'from_cache': True
                })
        save_cache(cache_file, cache)

    # Crear DataFrames
    results_df = pl.DataFrame(results)

    # Estadísticas
    log("")
    log("=" * 80)
    log("RESULTADOS")
    log("=" * 80)

    total = len(results_df)
    complete = len(results_df.filter(pl.col('status') == 'COMPLETE'))
    missing_months = len(results_df.filter(pl.col('status') == 'MISSING_MONTHS'))
    missing_ticker = len(results_df.filter(pl.col('status') == 'MISSING_TICKER'))
    quality_issues = len(results_df.filter(pl.col('status') == 'QUALITY_ISSUES'))

    log(f"Total tickers verificados:       {total:,}")
    log(f"  ✅ COMPLETOS:                  {complete:,} ({complete/total*100:.1f}%)")
    log(f"  ⚠️ MESES FALTANTES:            {missing_months:,} ({missing_months/total*100:.1f}%)")
    log(f"  ⚠️ PROBLEMAS DE CALIDAD:       {quality_issues:,} ({quality_issues/total*100:.1f}%)")
    log(f"  ❌ TICKER FALTANTE:            {missing_ticker:,} ({missing_ticker/total*100:.1f}%)")
    
    # Estadísticas de calidad
    if 'completeness_pct' in results_df.columns:
        avg_completeness = results_df.filter(pl.col('completeness_pct') > 0)['completeness_pct'].mean()
        log(f"\nCalidad promedio: {avg_completeness:.1f}%")
        
        high_quality = len(results_df.filter(pl.col('completeness_pct') >= args.quality_threshold))
        log(f"Alta calidad (>={args.quality_threshold}%): {high_quality:,} tickers")

    # Guardar archivos
    log("\nGuardando archivos...")

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

    # Missing months
    if missing_months_all:
        missing_months_df = pl.DataFrame(missing_months_all)
        missing_csv = f"{args.output_prefix}_missing_months.csv"
        missing_months_df.write_csv(missing_csv)
        log(f"  ✅ {missing_csv}")
        log(f"     Total meses faltantes: {len(missing_months_all):,}")

    # Quality issues
    if quality_issues_all:
        quality_df = pl.DataFrame(quality_issues_all)
        quality_csv = f"{args.output_prefix}_quality_issues.csv"
        quality_df.write_csv(quality_csv)
        log(f"  ✅ {quality_csv}")
        log(f"     Total problemas de calidad: {len(quality_issues_all):,}")

    # Summary report
    report_file = f"{args.output_prefix}_report.txt"
    with open(report_file, 'w') as f:
        f.write(f"Verificación Intraday vs Daily - {datetime.now()}\n")
        f.write("=" * 60 + "\n")
        f.write(f"Total tickers: {total:,}\n")
        f.write(f"Completos: {complete:,} ({complete/total*100:.1f}%)\n")
        f.write(f"Con problemas: {total - complete:,} ({(total-complete)/total*100:.1f}%)\n")
        f.write(f"Tamaño total: {results_df['size_mb'].sum():.1f} MB\n")
        if 'completeness_pct' in results_df.columns:
            f.write(f"Calidad promedio: {avg_completeness:.1f}%\n")
    log(f"  ✅ {report_file}")

    log("")
    log("=" * 80)
    log("VERIFICACIÓN COMPLETADA")
    log("=" * 80)

if __name__ == '__main__':
    main()
