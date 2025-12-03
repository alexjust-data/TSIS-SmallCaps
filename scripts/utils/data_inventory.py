#!/usr/bin/env python3
"""
Inventario completo de data descargada de Polygon
"""

import os
from pathlib import Path
from datetime import datetime

def get_folder_stats(path: Path) -> tuple:
    """Obtiene estadísticas de una carpeta"""
    if not path.exists():
        return 0, 0, 0

    total_size = 0
    file_count = 0
    ticker_count = 0

    # Contar tickers (subdirectorios de primer nivel)
    try:
        tickers = [d for d in path.iterdir() if d.is_dir() and not d.name.startswith('.')]
        ticker_count = len(tickers)
    except:
        pass

    # Contar archivos y tamaño
    try:
        for f in path.rglob('*.parquet'):
            file_count += 1
            total_size += f.stat().st_size
    except:
        pass

    return file_count, total_size / (1024**3), ticker_count

def main():
    print("=" * 90)
    print("INVENTARIO COMPLETO DE DATA POLYGON")
    print(f"Fecha: {datetime.now():%Y-%m-%d %H:%M}")
    print("=" * 90)

    # ===== C:\TSIS_Data =====
    print("\n" + "=" * 90)
    print("C:\\TSIS_Data")
    print("=" * 90)

    c_data = [
        ("ohlcv_intraday_1m", "OHLCV Intraday 1 minuto"),
        ("trades_ticks_2004_2018", "Trades Tick-Level 2004-2018"),
        ("trades_ticks_2019_2025", "Trades Tick-Level 2019-2025"),
        ("quotes_p95_2004_2018", "Quotes P95 2004-2018"),
        ("quotes_p95_2019_2025", "Quotes P95 2019-2025"),
        ("fundamentals", "Fundamentals"),
        ("short_data", "Short Interest"),
        ("additional", "Additional Data"),
        ("reference", "Reference Data"),
    ]

    total_c_files = 0
    total_c_size = 0

    print(f"\n{'Tipo':<35} {'Archivos':>12} {'Tamaño':>12} {'Tickers':>10}")
    print("-" * 90)

    for dirname, desc in c_data:
        path = Path(f"C:/TSIS_Data/{dirname}")
        files, size, tickers = get_folder_stats(path)
        if files > 0 or path.exists():
            print(f"{desc:<35} {files:>12,} {size:>10.2f} GB {tickers:>10,}")
            total_c_files += files
            total_c_size += size

    print("-" * 90)
    print(f"{'SUBTOTAL C:\\TSIS_Data':<35} {total_c_files:>12,} {total_c_size:>10.2f} GB")

    # ===== D:\TSIS_SmallCaps\raw\polygon =====
    print("\n" + "=" * 90)
    print("D:\\TSIS_SmallCaps\\raw\\polygon")
    print("=" * 90)

    d_data = [
        ("ohlcv_daily", "OHLCV Daily"),
        ("ohlcv_intraday_1m", "OHLCV Intraday 1m (backup)"),
        ("reference/dividends", "Dividends"),
        ("reference/splits", "Splits"),
        ("reference/ticker_details", "Ticker Details"),
        ("trades_ticks", "Trades Ticks (backup)"),
    ]

    total_d_files = 0
    total_d_size = 0

    print(f"\n{'Tipo':<35} {'Archivos':>12} {'Tamaño':>12} {'Tickers':>10}")
    print("-" * 90)

    for dirname, desc in d_data:
        path = Path(f"D:/TSIS_SmallCaps/raw/polygon/{dirname}")
        files, size, tickers = get_folder_stats(path)
        if files > 0 or path.exists():
            print(f"{desc:<35} {files:>12,} {size:>10.2f} GB {tickers:>10,}")
            total_d_files += files
            total_d_size += size

    print("-" * 90)
    print(f"{'SUBTOTAL D:\\raw\\polygon':<35} {total_d_files:>12,} {total_d_size:>10.2f} GB")

    # ===== RESUMEN FINAL =====
    print("\n" + "=" * 90)
    print("RESUMEN FINAL")
    print("=" * 90)
    print(f"{'Total Archivos Parquet:':<35} {total_c_files + total_d_files:>12,}")
    print(f"{'Total Almacenamiento:':<35} {total_c_size + total_d_size:>10.2f} GB")
    print("=" * 90)

    # ===== DETALLE POR TIPO DE DATA =====
    print("\n" + "=" * 90)
    print("RESUMEN POR TIPO DE DATA")
    print("=" * 90)

    summary = {
        "OHLCV Daily": "2004-2025, ~7K tickers",
        "OHLCV Intraday 1m": "2004-2025, ~9K tickers, ~920M+ rows",
        "Trades Tick-Level": "2004-2025, ~9K tickers, tick por tick",
        "Quotes P95": "2004-2025, top 5% volumen, ~16B quotes",
        "Reference": "Dividends, Splits, Ticker Details",
        "Additional": "News, Corporate Actions, Economic",
    }

    for tipo, desc in summary.items():
        print(f"  {tipo}: {desc}")

if __name__ == "__main__":
    main()
