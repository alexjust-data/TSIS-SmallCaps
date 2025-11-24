#!/usr/bin/env python3
"""
Descarga Ticker Events/Changes (cambios de símbolo, rebranding, etc.)
NOTA: Splits y Dividends ya están descargados en processed/corporate_actions/
"""

import asyncio
import aiohttp
import polars as pl
import os
from pathlib import Path
from datetime import datetime

api_key = os.environ.get('POLYGON_API_KEY')
base_url = 'https://api.polygon.io'

# Cargar tickers
with open(r'D:\TSIS_SmallCaps\tickers_confirmed.txt') as f:
    tickers = [line.strip() for line in f if line.strip()]
print(f'Cargados {len(tickers):,} tickers')

output_dir = Path(r'C:\TSIS_Data\additional\corporate_actions')
output_dir.mkdir(parents=True, exist_ok=True)

async def download_all():
    async with aiohttp.ClientSession() as session:

        # TICKER EVENTS (por cada ticker)
        print('\n=== Descargando Ticker Events ===')
        semaphore = asyncio.Semaphore(30)
        all_events = []
        tickers_with_events = 0

        async def fetch_events(ticker):
            nonlocal tickers_with_events
            async with semaphore:
                try:
                    url = f'{base_url}/vX/reference/tickers/{ticker}/events?apiKey={api_key}'
                    async with session.get(url) as r:
                        if r.status != 200:
                            return []
                        data = await r.json()

                    events = data.get('results', {}).get('events', [])
                    if events:
                        tickers_with_events += 1
                        return [{'ticker': ticker, **e} for e in events]
                except:
                    pass
                return []

        batch_size = 100
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i+batch_size]
            tasks = [fetch_events(t) for t in batch]
            results = await asyncio.gather(*tasks)

            for r in results:
                all_events.extend(r)

            batch_num = i // batch_size + 1
            total_batches = (len(tickers) + batch_size - 1) // batch_size
            print(f'  Batch {batch_num}/{total_batches} | Events: {len(all_events):,} | Tickers: {tickers_with_events}')

        if all_events:
            df = pl.DataFrame(all_events)
            df.write_parquet(output_dir / 'ticker_events.parquet')
            print(f'\n  Guardado: {len(df):,} eventos de {tickers_with_events} tickers')
        else:
            print('\n  No se encontraron eventos')

        print('\n=== COMPLETADO ===')

if __name__ == '__main__':
    asyncio.run(download_all())
