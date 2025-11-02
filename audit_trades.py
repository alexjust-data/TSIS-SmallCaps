#!/usr/bin/env python3
"""Script temporal para auditar progreso de descarga de trades"""

from pathlib import Path
from datetime import datetime

trades_dir = Path('raw/polygon/trades_ticks')
ticker_dirs = [d for d in trades_dir.iterdir() if d.is_dir() and d.name != '_batch_temp']

print(f'=== AUDIT TRADES TICK-LEVEL - {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} ===')
print()
print(f'Tickers completados: {len(ticker_dirs):,} / 6,405')
print()

# Calcular tamaño total y archivos
total_size = 0
total_files = 0
ticker_stats = []

for ticker_dir in ticker_dirs:
    ticker_files = 0
    ticker_size = 0
    for pfile in ticker_dir.rglob('*.parquet'):
        ticker_files += 1
        ticker_size += pfile.stat().st_size
        total_files += 1
        total_size += ticker_size

    if ticker_files > 0:
        latest_time = max((f.stat().st_mtime for f in ticker_dir.rglob('*.parquet')), default=0)
        ticker_stats.append({
            'name': ticker_dir.name,
            'files': ticker_files,
            'size': ticker_size,
            'mtime': latest_time
        })

print(f'Total archivos: {total_files:,}')
print(f'Total tamaño: {total_size / (1024**2):.2f} MB ({total_size / (1024**3):.2f} GB)')
print()

# Cálculo de velocidad
if len(ticker_stats) > 0:
    # Ordenar por tiempo de modificación
    ticker_stats.sort(key=lambda x: x['mtime'], reverse=True)

    # Calcular tiempo desde el primer ticker
    oldest_time = min(t['mtime'] for t in ticker_stats)
    newest_time = max(t['mtime'] for t in ticker_stats)
    elapsed_mins = (newest_time - oldest_time) / 60

    if elapsed_mins > 0:
        velocity_per_hour = (len(ticker_stats) / elapsed_mins) * 60
        mins_per_ticker = elapsed_mins / len(ticker_stats)

        print(f'Velocidad:')
        print(f'  Tiempo transcurrido: {elapsed_mins:.1f} minutos')
        print(f'  Velocidad: {velocity_per_hour:.1f} tickers/hora')
        print(f'  Tiempo por ticker: {mins_per_ticker:.1f} minutos')
        print()

        # Estimar tiempo restante
        remaining_tickers = 6405 - len(ticker_stats)
        remaining_hours = remaining_tickers / velocity_per_hour
        remaining_days = remaining_hours / 24

        eta = datetime.fromtimestamp(newest_time + (remaining_hours * 3600))

        print(f'Estimación:')
        print(f'  Tickers pendientes: {remaining_tickers:,}')
        print(f'  Tiempo restante: {remaining_hours:.1f} horas ({remaining_days:.1f} días)')
        print(f'  ETA: {eta.strftime("%Y-%m-%d %H:%M")}')
        print()

        # Estimación de espacio
        avg_size_mb = (total_size / len(ticker_stats)) / (1024**2)
        estimated_total_gb = (avg_size_mb * 6405) / 1024
        print(f'  Tamaño promedio: {avg_size_mb:.2f} MB/ticker')
        print(f'  Espacio estimado total: {estimated_total_gb:.1f} GB')
        print()

# Últimos 15 tickers completados
print('Últimos 15 tickers completados:')
for stat in ticker_stats[:15]:
    time_str = datetime.fromtimestamp(stat['mtime']).strftime('%H:%M:%S')
    size_mb = stat['size'] / (1024**2)
    print(f"  {stat['name']:8s} | {stat['files']:4d} archivos | {size_mb:8.2f} MB | {time_str}")
