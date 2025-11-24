#!/usr/bin/env python3
"""
Calcula percentiles de volumen para crear estrategia de descarga escalonada.
Permite descargar quotes en fases según importancia.
"""

import polars as pl
from pathlib import Path
import argparse
from typing import Dict, Tuple

def analyze_volume_distribution(daily_root: Path, tickers: list,
                               year_min: int, year_max: int) -> Dict:
    """
    Analiza distribución de volumen para todos los tickers.
    """
    all_volumes = []
    ticker_stats = {}

    for ticker in tickers:
        ticker_volumes = []

        for year in range(year_min, year_max + 1):
            daily_file = daily_root / ticker / f"year={year}" / "daily.parquet"

            try:
                # Verificar existencia puede lanzar PermissionError o FileNotFoundError en Windows
                try:
                    if not daily_file.exists():
                        continue
                except (PermissionError, OSError, FileNotFoundError):
                    continue

                df = pl.read_parquet(daily_file)
                # La columna puede llamarse 'volume' o 'v'
                vol_col = 'volume' if 'volume' in df.columns else 'v'
                volumes = df[vol_col].to_list()
                ticker_volumes.extend(volumes)
                all_volumes.extend(volumes)
            except (PermissionError, OSError, Exception):
                # Archivo corrupto (CRC error) u otro problema - skip
                continue
        
        if ticker_volumes:
            ticker_stats[ticker] = {
                'mean': sum(ticker_volumes) / len(ticker_volumes),
                'p50': sorted(ticker_volumes)[len(ticker_volumes)//2],
                'p90': sorted(ticker_volumes)[int(len(ticker_volumes)*0.9)],
                'p95': sorted(ticker_volumes)[int(len(ticker_volumes)*0.95)],
                'p99': sorted(ticker_volumes)[int(len(ticker_volumes)*0.99)],
                'max': max(ticker_volumes)
            }
    
    # Calcular percentiles globales
    if not all_volumes:
        print("ERROR: No se encontraron datos de volumen")
        return {}, {}

    all_volumes.sort()
    n = len(all_volumes)

    global_stats = {
        'total_days': n,
        'p50': all_volumes[n//2] if n > 0 else 0,
        'p75': all_volumes[int(n*0.75)] if n > 0 else 0,
        'p90': all_volumes[int(n*0.90)] if n > 0 else 0,
        'p95': all_volumes[int(n*0.95)] if n > 0 else 0,
        'p99': all_volumes[int(n*0.99)] if n > 0 else 0,
        'p99_9': all_volumes[int(n*0.999)] if n > 0 else 0
    }

    return global_stats, ticker_stats

def create_phased_download_strategy(daily_root: Path, ping_file: Path,
                                   year_min: int, year_max: int) -> Dict:
    """
    Crea estrategia de descarga en fases.
    """
    # Cargar tickers
    ping_df = pl.read_parquet(ping_file)
    tickers = ping_df.filter(pl.col('has_data') == True)['ticker'].to_list()
    
    print(f"Analizando {len(tickers)} tickers...")
    
    # Analizar distribución de volumen
    global_stats, ticker_stats = analyze_volume_distribution(
        daily_root, tickers, year_min, year_max  # Todos los tickers
    )
    
    print("\n" + "="*60)
    print("DISTRIBUCIÓN GLOBAL DE VOLUMEN")
    print("="*60)
    print(f"Total días analizados: {global_stats['total_days']:,}")
    print(f"Percentil 50 (mediana): {global_stats['p50']:,.0f}")
    print(f"Percentil 75: {global_stats['p75']:,.0f}")
    print(f"Percentil 90: {global_stats['p90']:,.0f}")
    print(f"Percentil 95: {global_stats['p95']:,.0f}")
    print(f"Percentil 99: {global_stats['p99']:,.0f}")
    print(f"Percentil 99.9: {global_stats['p99_9']:,.0f}")
    
    # Definir fases de descarga
    phases = {
        'FASE_1_CRITICAL': {
            'description': 'Días ultra-críticos (>P99)',
            'threshold': global_stats['p99'],
            'estimated_pct': 1,
            'estimated_size_gb': 55  # 1% de 5.5TB
        },
        'FASE_2_HIGH': {
            'description': 'Días de alto volumen (P95-P99)',
            'threshold': global_stats['p95'],
            'estimated_pct': 4,
            'estimated_size_gb': 220  # 4% de 5.5TB
        },
        'FASE_3_MEDIUM': {
            'description': 'Días importantes (P90-P95)',
            'threshold': global_stats['p90'],
            'estimated_pct': 5,
            'estimated_size_gb': 275  # 5% de 5.5TB
        },
        'FASE_4_OPTIONAL': {
            'description': 'Días normales altos (P75-P90)',
            'threshold': global_stats['p75'],
            'estimated_pct': 15,
            'estimated_size_gb': 825  # 15% de 5.5TB
        },
        'FASE_5_SKIP': {
            'description': 'Días de bajo volumen (<P75)',
            'threshold': 0,
            'estimated_pct': 75,
            'estimated_size_gb': 4125  # 75% de 5.5TB - NO DESCARGAR
        }
    }
    
    print("\n" + "="*60)
    print("ESTRATEGIA DE DESCARGA POR FASES")
    print("="*60)
    
    cumulative_size = 0
    for phase_name, phase_info in phases.items():
        if 'SKIP' not in phase_name:
            cumulative_size += phase_info['estimated_size_gb']
            print(f"\n{phase_name}:")
            print(f"  Descripción: {phase_info['description']}")
            print(f"  Umbral volumen: {phase_info['threshold']:,.0f}")
            print(f"  % de días: {phase_info['estimated_pct']}%")
            print(f"  Tamaño estimado: {phase_info['estimated_size_gb']} GB")
            print(f"  Tamaño acumulado: {cumulative_size} GB")
    
    return phases, global_stats, ticker_stats

def generate_phase_csv(daily_root: Path, output_prefix: str,
                      phase_threshold: float, tickers: list,
                      year_min: int, year_max: int, phase_name: str):
    """
    Genera CSV para una fase específica.
    """
    selected_days = []

    for ticker in tickers:
        for year in range(year_min, year_max + 1):
            daily_file = daily_root / ticker / f"year={year}" / "daily.parquet"

            try:
                # Verificar existencia puede lanzar PermissionError o FileNotFoundError en Windows
                try:
                    if not daily_file.exists():
                        continue
                except (PermissionError, OSError, FileNotFoundError):
                    continue

                df = pl.read_parquet(daily_file)
                # La columna puede llamarse 'volume' o 'v'
                vol_col = 'volume' if 'volume' in df.columns else 'v'
                # Filtrar por umbral de volumen
                high_vol_days = df.filter(pl.col(vol_col) >= phase_threshold)

                for row in high_vol_days.iter_rows(named=True):
                    selected_days.append({
                        'ticker': ticker,
                        'date': row['date'],
                        'volume': row[vol_col]
                    })
            except (PermissionError, OSError, Exception):
                # Archivo corrupto (CRC error) u otro problema - skip
                continue
    
    if selected_days:
        df_phase = pl.DataFrame(selected_days)
        output_file = f"{output_prefix}_{phase_name}.csv"
        df_phase.select(['ticker', 'date']).write_csv(output_file)
        print(f"Generado: {output_file} ({len(df_phase):,} días)")
        return len(df_phase)
    
    return 0

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Análisis de distribución de volumen para estrategia de quotes')
    parser.add_argument('--daily-root', default='raw/polygon/ohlcv_daily', help='Path to daily OHLCV')
    parser.add_argument('--ping-file', default='processed/universe/ping_range_2019_2025.parquet', help='Path to ping_range parquet')
    parser.add_argument('--year-min', type=int, default=2019, help='Start year')
    parser.add_argument('--year-max', type=int, default=2025, help='End year')
    parser.add_argument('--generate-csv', choices=['P75', 'P90', 'P95', 'P99'], help='Generar CSV para percentil específico')

    args = parser.parse_args()

    daily_root = Path(args.daily_root)
    ping_file = Path(args.ping_file)

    phases, global_stats, ticker_stats = create_phased_download_strategy(
        daily_root, ping_file, args.year_min, args.year_max
    )

    print("\n" + "="*60)
    print("RECOMENDACIÓN")
    print("="*60)
    print("\n1. EMPEZAR con FASE_1_CRITICAL:")
    print("   - Solo ~55 GB")
    print("   - Días más importantes (crashes, squeezes, halts)")
    print("   - 1-2 días de descarga\n")

    print("2. CONTINUAR con FASE_2_HIGH si necesitas más:")
    print("   - +220 GB adicionales")
    print("   - Días de earnings, news, eventos\n")

    print("3. PARAR en FASE_3 para la mayoría de casos:")
    print("   - Total ~550 GB (10% del total)")
    print("   - Cubre 90% de los días importantes\n")

    print("4. NUNCA descargar FASE_5:")
    print("   - 4.1 TB de días con volumen bajo")
    print("   - Poco valor para análisis")

    # Generar CSV si se especifica
    if args.generate_csv:
        print("\n" + "="*60)
        print(f"GENERANDO CSV PARA {args.generate_csv}")
        print("="*60)

        # Cargar tickers
        ping_df = pl.read_parquet(ping_file)
        tickers = ping_df.filter(pl.col('has_data') == True)['ticker'].to_list()

        # Seleccionar umbral según percentil
        percentil_map = {
            'P75': ('p75', 'FASE_4'),
            'P90': ('p90', 'FASE_3'),
            'P95': ('p95', 'FASE_2'),
            'P99': ('p99', 'FASE_1')
        }

        percentil_key, fase_name = percentil_map[args.generate_csv]
        threshold = global_stats[percentil_key]

        print(f"\nUmbral {args.generate_csv}: {threshold:,.0f}")
        print(f"Filtrando días con volumen >= {threshold:,.0f}...")

        output_prefix = f"quotes_{args.generate_csv}_{args.year_min}_{args.year_max}"

        count = generate_phase_csv(
            daily_root, output_prefix, threshold,
            tickers, args.year_min, args.year_max, args.generate_csv
        )

        if count > 0:
            csv_file = f"{output_prefix}_{args.generate_csv}.csv"
            print("\n" + "="*60)
            print("CSV GENERADO - LISTO PARA DESCARGA")
            print("="*60)
            print(f"\nArchivo: {csv_file}")
            print(f"Total días: {count:,}")
            print(f"\nComando para descargar:")
            print(f"python scripts\\02_final\\download_quotes_fase3.py \\")
            print(f"  --csv {csv_file} \\")
            print(f"  --output \"C:\\TSIS_Data\\quotes_{args.generate_csv.lower()}_{args.year_min}_{args.year_max}\" \\")
            print(f"  --concurrent 50")
