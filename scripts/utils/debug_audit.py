#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import polars as pl
from pathlib import Path

# Fix encoding for Windows console
sys.stdout.reconfigure(encoding='utf-8')

# Load ping data
ping_file = "D:/TSIS_SmallCaps/processed/universe/ping_binary_2004_2018.parquet"
outdir = Path("C:/TSIS_Data/trades_ticks_2004_2018_v2")

print(f"Loading {ping_file}...")
ping_df = pl.read_parquet(ping_file)

print(f"\nColumns: {ping_df.columns}")
print(f"Shape: {ping_df.shape}")
print(f"\nFirst 5 rows:")
print(ping_df.head(5))

# Count tickers with history
print(f"\nColumn types:")
for col in ping_df.columns:
    print(f"  {col}: {ping_df[col].dtype}")

# Get tickers with history
has_history_col = ping_df.columns[1]  # Segunda columna
print(f"\nUsing column '{has_history_col}' for has_history check")

tickers_with_history = ping_df.filter(pl.col(has_history_col) == True)["ticker"].to_list()
print(f"\nTickers with history: {len(tickers_with_history)}")

# Check which exist in outdir
existing_dirs = {d.name for d in outdir.iterdir() if d.is_dir() and d.name != "_batch_temp"}
print(f"Existing ticker directories: {len(existing_dirs)}")

# Find missing
missing = [t for t in tickers_with_history if t not in existing_dirs]
print(f"Missing tickers: {len(missing)}")

if len(missing) > 0:
    print(f"\nFirst 20 missing tickers:")
    for t in missing[:20]:
        print(f"  {t}")

# Find extra (downloaded but not in ping)
extra = existing_dirs - set(tickers_with_history)
print(f"\nExtra tickers (in dir but not in ping): {len(extra)}")
if len(extra) > 0:
    print(f"First 20 extra:")
    for t in list(extra)[:20]:
        print(f"  {t}")
