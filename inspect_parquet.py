import polars as pl
import sys

file_path = r'C:\TSIS_Data\trades_ticks_2004_2018\NKTR\year=2004\month=01\day=2004-01-02\market.parquet'

df = pl.read_parquet(file_path)

print('='*80)
print(f'File: NKTR 2004-01-02 market.parquet')
print('='*80)
print(f'Total rows (trades): {len(df):,}')
print(f'Columns: {list(df.columns)}')

print(f'\nData types:')
for col in df.columns:
    print(f'  {col}: {df[col].dtype}')

print(f'\n--- FIRST 15 TRADES ---')
print(df.head(15).to_pandas().to_string())

print(f'\n--- LAST 15 TRADES ---')
print(df.tail(15).to_pandas().to_string())

print(f'\n--- PRICE & SIZE STATISTICS ---')
stats = df.select(['price', 'size']).describe()
print(f'Price - Min: ${stats["price"][3]:.4f}, Max: ${stats["price"][7]:.4f}, Mean: ${stats["price"][1]:.4f}')
print(f'Size  - Min: {int(stats["size"][3])}, Max: {int(stats["size"][7])}, Mean: {int(stats["size"][1])}')

print(f'\n--- EXCHANGE DISTRIBUTION ---')
exchanges = df.group_by('exchange').agg(pl.count().alias('count')).sort('count', descending=True)
for row in exchanges.iter_rows(named=True):
    pct = row['count'] / len(df) * 100
    print(f'  Exchange {row["exchange"]}: {row["count"]:,} trades ({pct:.1f}%)')

# Time range
print(f'\n--- TIME RANGE ---')
print(f'First trade: {df["timestamp"].min()}')
print(f'Last trade: {df["timestamp"].max()}')
