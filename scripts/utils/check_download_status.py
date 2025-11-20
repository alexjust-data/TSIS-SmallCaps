#!/usr/bin/env python3
from pathlib import Path
from datetime import datetime

data_dir = Path("C:/TSIS_Data/trades_ticks_2004_2018_v2")

# Count ticker directories
dirs = [d for d in data_dir.iterdir() if d.is_dir() and d.name != "_batch_temp"]
print(f"Total ticker directories: {len(dirs)}")

# Find most recent parquet file
parquet_files = list(data_dir.rglob("*.parquet"))
if parquet_files:
    most_recent = max(parquet_files, key=lambda x: x.stat().st_mtime)
    mtime = datetime.fromtimestamp(most_recent.stat().st_mtime)
    print(f"\nMost recent parquet file:")
    print(f"  Path: {most_recent}")
    print(f"  Modified: {mtime}")
else:
    print("\nNo parquet files found")

# Find recently created directories (last 5 minutes)
now = datetime.now()
recent_dirs = []
for d in dirs:
    mtime = datetime.fromtimestamp(d.stat().st_mtime)
    age_mins = (now - mtime).total_seconds() / 60
    if age_mins < 5:
        recent_dirs.append((d.name, mtime))

if recent_dirs:
    print(f"\nTicker directories created/modified in last 5 minutes: {len(recent_dirs)}")
    for name, mtime in sorted(recent_dirs, key=lambda x: x[1], reverse=True)[:10]:
        print(f"  {name}: {mtime}")
else:
    print("\nNo directories created/modified in last 5 minutes")
