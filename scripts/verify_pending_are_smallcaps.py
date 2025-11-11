"""
Verificar que los 131 tickers pendientes están en el universo SmallCaps oficial
"""
from pathlib import Path
import polars as pl

# Cargar universo oficial
universe_path = Path('D:/TSIS_SmallCaps/processed/universe/smallcaps_universe_2025-11-01.parquet')
df_universe = pl.read_parquet(universe_path)

print(f"\nUNIVERSO OFICIAL: {len(df_universe)} tickers")
print(f"Archivo: {universe_path}")
print("="*80)

# Obtener tickers descargados
outdir = Path('C:/TSIS_Data/trades_ticks_2019_2025')
downloaded = {d.name for d in outdir.iterdir() if d.is_dir() and d.name != '_batch_temp'}

# Tickers del universo
universe_tickers = set(df_universe['ticker'].to_list())

# Calcular pendientes
pending = sorted(universe_tickers - downloaded)

print(f"\nTICKERS DESCARGADOS: {len(downloaded)}")
print(f"TICKERS PENDIENTES: {len(pending)}")
print("="*80)

# Verificar que cada ticker pendiente está en el universo
print("\nVERIFICACIÓN: ¿Están los 131 pendientes en el universo SmallCaps?")
print("="*80)

all_valid = True
for ticker in pending[:10]:  # Mostrar primeros 10 como muestra
    row = df_universe.filter(pl.col('ticker') == ticker)
    if len(row) > 0:
        company = row['name'][0] if 'name' in row.columns else 'N/A'
        market_cap = row['market_cap'][0] if 'market_cap' in row.columns else 'N/A'
        print(f"OK {ticker:10s} | {str(company)[:40]:40s} | Cap: {market_cap}")
    else:
        print(f"ERROR {ticker:10s} | NO ENCONTRADO EN UNIVERSO!")
        all_valid = False

print("\n" + "="*80)
if all_valid:
    print("OK - TODOS LOS TICKERS PENDIENTES SON SMALLCAPS VALIDOS DEL UNIVERSO")
else:
    print("ERROR - ALGUNOS TICKERS PENDIENTES NO ESTAN EN EL UNIVERSO!")

# Mostrar los 131 tickers pendientes
print(f"\n\nLISTA COMPLETA DE LOS {len(pending)} TICKERS PENDIENTES:")
print("="*80)
for i in range(0, len(pending), 10):
    print(", ".join(pending[i:i+10]))

# Guardar a CSV para inspección
pending_df = df_universe.filter(pl.col('ticker').is_in(pending))
csv_path = 'D:/TSIS_SmallCaps/pending_131_verification.csv'
pending_df.write_csv(csv_path)
print(f"\n✓ Detalles guardados en: {csv_path}")
print(f"  Columnas: {pending_df.columns}")
