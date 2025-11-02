#!/usr/bin/env python3
"""Quick count of completed tickers"""

from pathlib import Path

trades_dir = Path('raw/polygon/trades_ticks')
ticker_dirs = [d for d in trades_dir.iterdir() if d.is_dir() and d.name != '_batch_temp']

print(f'Tickers completados: {len(ticker_dirs)}')
print('\nPrimeros 20:')
for td in sorted(ticker_dirs)[:20]:
    print(f'  {td.name}')
