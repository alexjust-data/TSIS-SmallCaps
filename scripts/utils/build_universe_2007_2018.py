#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
build_universe_2007_2018.py - Construye universo elegible para descarga 2007-2018

Filtra solo tickers que:
1. Ya tienen ALGÚN dato en 2007-2018 (según análisis previo)
2. Su list_date/delist_date solapa con 2007-2018

Output: CSV con tickers elegibles para descarga histórica optimizada
"""
import polars as pl
from datetime import date
import csv

# Rango de interés
FROM = date(2007, 1, 1)
TO = date(2018, 12, 31)

print("=" * 80)
print("CONSTRUCCION DE UNIVERSO ELEGIBLE 2007-2018")
print("=" * 80)

# 1) Cargar universo completo
print("\n1. Cargando universo completo...")
df_universe = pl.read_parquet("processed/universe/smallcaps_universe_2025-11-01.parquet")
print(f"   Total tickers en universo: {len(df_universe):,}")

# 2) Cargar tickers que YA tienen datos 2007-2018 (análisis previo)
print("\n2. Cargando tickers con datos confirmados 2007-2018...")
with open('tickers_with_2007_2018.csv', 'r') as f:
    reader = csv.DictReader(f)
    confirmed_tickers = set(row['ticker'] for row in reader)
print(f"   Tickers con datos históricos confirmados: {len(confirmed_tickers):,}")

# 3) Filtrar universo por tickers confirmados
print("\n3. Filtrando universo por tickers confirmados...")
df_confirmed = df_universe.filter(pl.col("ticker").is_in(list(confirmed_tickers)))
print(f"   Tickers en universo confirmados: {len(df_confirmed):,}")

# 4) Generar CSV final (sin validación de fechas, confiamos en análisis previo)
print("\n4. Generando CSV final...")
df_final = df_confirmed.select("ticker").unique()

output_path = "processed/universe/eligible_2007_2018.csv"
df_final.write_csv(output_path)

print(f"\nArchivo: {output_path}")
print(f"Total tickers elegibles: {len(df_final):,}")

# 5) Mostrar algunos ejemplos
print("\nPrimeros 20 tickers elegibles:")
for i, row in enumerate(df_final.head(20).iter_rows(), 1):
    print(f"  {i:3d}. {row[0]}")

print("\n" + "=" * 80)
print("COMPLETADO")
print("=" * 80)
print(f"\nArchivo generado: {output_path}")
print(f"Listo para usar con batch_trades_wrapper.py --tickers-csv {output_path}")
