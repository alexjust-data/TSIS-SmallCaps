#!/usr/bin/env python3
"""
Create filtered universe parquet with only incomplete tickers for 2004-2018.
"""
import polars as pl
from pathlib import Path
import sys

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

def main():
    # Paths for 2004-2018
    incomplete_csv = Path('01_daily/01_agregation_OHLCV/files_csv/verification_2004_2018_incomplete.csv')
    original_universe = Path('processed/universe/ping_range_2004_2018.parquet')
    output_parquet = Path('processed/universe/ping_range_2004_2018_incomplete.parquet')

    print("=" * 70)
    print("CREANDO UNIVERSE FILTRADO PARA REDOWNLOAD 2004-2018")
    print("=" * 70)

    # Load incomplete tickers
    print(f"\nCargando tickers incompletos desde: {incomplete_csv}")
    df_incomplete = pl.read_csv(incomplete_csv)
    # Filter to only INCOMPLETE status
    df_incomplete = df_incomplete.filter(pl.col('status') == 'INCOMPLETE')
    incomplete_tickers = set(df_incomplete['ticker'].to_list())
    print(f"  ✓ {len(incomplete_tickers)} tickers incompletos encontrados")

    # Load original universe
    print(f"\nCargando universe original: {original_universe}")
    df_universe = pl.read_parquet(original_universe)
    print(f"  ✓ {len(df_universe)} tickers totales en universe")

    # Filter to only incomplete tickers
    print(f"\nFiltrando universe...")
    df_filtered = df_universe.filter(pl.col('ticker').is_in(incomplete_tickers))
    print(f"  ✓ {len(df_filtered)} tickers en universe filtrado")

    # Verify match
    if len(df_filtered) != len(incomplete_tickers):
        print(f"\n⚠️  ADVERTENCIA: {len(incomplete_tickers)} en CSV pero {len(df_filtered)} en universe filtrado")
        missing = incomplete_tickers - set(df_filtered['ticker'].to_list())
        if missing:
            print(f"  Tickers no encontrados en universe: {sorted(list(missing))[:10]}")

    # Save filtered parquet
    print(f"\nGuardando universe filtrado: {output_parquet}")
    output_parquet.parent.mkdir(parents=True, exist_ok=True)
    df_filtered.write_parquet(output_parquet)
    print(f"  ✓ Guardado exitosamente")

    # Show sample
    print(f"\n" + "-" * 70)
    print("MUESTRA DE TICKERS A REDESCARGAR:")
    print("-" * 70)
    sample = df_filtered.head(20)
    for row in sample.iter_rows(named=True):
        print(f"  {row['ticker']:8s}  {row.get('first_day', 'N/A')} → {row.get('last_day', 'N/A')}")

    if len(df_filtered) > 20:
        print(f"  ... y {len(df_filtered) - 20} más")

    print("\n" + "=" * 70)
    print("COMANDO PARA REDOWNLOAD:")
    print("=" * 70)
    print(f"""
python scripts\\01_agregation_OHLCV\\ingest_intraday_ultra_fast.py `
  --tickers-csv {output_parquet} `
  --outdir C:\\TSIS_Data\\ohlcv_intraday_1m\\2004_2018 `
  --daily-dir raw/polygon/ohlcv_daily `
  --start-year 2004 `
  --end-year 2018 `
  --concurrent 60
""")
    print("=" * 70)

if __name__ == '__main__':
    main()
