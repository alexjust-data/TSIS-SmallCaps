import polars as pl
import sys

sys.stdout.reconfigure(encoding='utf-8')

# Check daily data
df = pl.read_parquet('raw/polygon/ohlcv_daily/BCAC/year=2022/daily.parquet')
vol_col = 'volume' if 'volume' in df.columns else 'v'
feb = df.filter(pl.col('date').str.starts_with('2022-02'))

print("Días en daily para BCAC Feb 2022:")
print(feb.select(['date', vol_col]).sort('date'))
print(f"\nTotal días: {len(feb)}")
print(f"\nVolumen promedio: {feb[vol_col].mean():,.0f}")
print(f"Volumen mínimo: {feb[vol_col].min():,.0f}")
