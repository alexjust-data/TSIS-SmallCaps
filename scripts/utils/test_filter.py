#!/usr/bin/env python3
import polars as pl
from pathlib import Path

ping = pl.read_parquet("D:/TSIS_SmallCaps/processed/universe/ping_binary_2004_2018.parquet")
print(f"Total rows: {ping.height}")
print(f"Columns: {ping.columns}")
print(f"\nFirst 5 rows:")
print(ping.head(5))

# Test filter
col_name = ping.columns[1]
print(f"\nFiltering by column '{col_name}' == True")
filtered = ping.filter(pl.col(col_name) == True)
print(f"Rows with history: {filtered.height}")

# Check data type
print(f"\nColumn '{col_name}' values (first 10):")
print(ping[col_name].head(10))
