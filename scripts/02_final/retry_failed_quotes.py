#!/usr/bin/env python3
"""
Reintenta solo los días que fallaron en la descarga de quotes.
Compara el CSV original con los archivos descargados para identificar faltantes.
"""

import polars as pl
from pathlib import Path
import argparse

def find_failed_downloads(csv_file: Path, output_dir: Path) -> pl.DataFrame:
    """
    Identifica qué ticker-date combinaciones NO tienen archivo descargado.

    Returns:
        DataFrame con ticker, date de tareas faltantes
    """
    print(f"Cargando CSV original: {csv_file}")
    df_all = pl.read_csv(csv_file)
    total = len(df_all)
    print(f"  Total tareas en CSV: {total:,}")

    # Verificar cuáles existen
    missing = []
    for row in df_all.iter_rows(named=True):
        ticker = row['ticker']
        date = row['date']

        # Construir path esperado
        year, month, day = date.split('-')
        quotes_file = output_dir / ticker / f"year={year}" / f"month={month}" / f"day={day}" / "quotes.parquet"

        if not quotes_file.exists():
            missing.append({'ticker': ticker, 'date': date})

    print(f"\n  Completados: {total - len(missing):,} ({(total - len(missing))/total*100:.1f}%)")
    print(f"  Faltantes: {len(missing):,} ({len(missing)/total*100:.1f}%)")

    if missing:
        return pl.DataFrame(missing)
    else:
        return pl.DataFrame({'ticker': [], 'date': []})

def main():
    parser = argparse.ArgumentParser(description='Encuentra y reintenta descargas fallidas de quotes')
    parser.add_argument('--original-csv', required=True, help='CSV original con todas las tareas')
    parser.add_argument('--output-dir', required=True, help='Directorio donde se guardaron los quotes')
    parser.add_argument('--retry-csv', default='quotes_retry.csv', help='CSV de salida con tareas a reintentar')

    args = parser.parse_args()

    print("=" * 70)
    print("IDENTIFICANDO DESCARGAS FALLIDAS")
    print("=" * 70)

    csv_file = Path(args.original_csv)
    output_dir = Path(args.output_dir)
    retry_csv = Path(args.retry_csv)

    # Encontrar faltantes
    df_missing = find_failed_downloads(csv_file, output_dir)

    if len(df_missing) == 0:
        print("\n✅ ¡TODO COMPLETO! No hay tareas faltantes.")
        return 0

    # Guardar CSV de retry
    df_missing.write_csv(retry_csv)
    print(f"\n📝 CSV de retry guardado: {retry_csv}")
    print(f"   Total tareas a reintentar: {len(df_missing):,}")

    # Mostrar muestra
    print("\n" + "-" * 70)
    print("MUESTRA DE TAREAS A REINTENTAR:")
    print("-" * 70)
    for row in df_missing.head(20).iter_rows(named=True):
        print(f"  {row['ticker']:6s}  {row['date']}")

    if len(df_missing) > 20:
        print(f"  ... y {len(df_missing) - 20:,} más")

    print("\n" + "=" * 70)
    print("COMANDO PARA REINTENTAR:")
    print("=" * 70)
    print(f"""
python scripts\\02_final\\download_quotes_ultra_fast.py \\
  --csv {retry_csv} \\
  --output "{args.output_dir}" \\
  --concurrent 50 \\
  --resume
""")
    print("=" * 70)

    return 0

if __name__ == '__main__':
    exit(main())
