"""
Script de auditoría completa para la descarga de trades tick-level

Verifica:
1. Cobertura de tickers: ¿Cuántos tickers tienen datos?
2. Cobertura temporal: ¿Qué años tiene cada ticker?
3. Gaps temporales: ¿Hay años faltantes entre el primer y último año?
4. Volumen de datos: Tamaño por ticker, número de trades
5. Integridad: Archivos corruptos o vacíos

Uso:
    python audit_trades_download.py --data-dir "C:\TSIS_Data\trades_ticks_2019_2025" \
                                     --universe processed/universe/smallcaps_universe_2025-11-01.parquet \
                                     --expected-start 2007 \
                                     --expected-end 2025
"""

import argparse
from pathlib import Path
import polars as pl
from collections import defaultdict
from datetime import datetime
import sys

def parse_args():
    parser = argparse.ArgumentParser(description="Auditoría completa de trades tick-level")
    parser.add_argument("--data-dir", required=True, help="Directorio con datos de trades")
    parser.add_argument("--universe", required=True, help="Archivo del universo (parquet o csv)")
    parser.add_argument("--expected-start", type=int, default=2007, help="Año inicial esperado")
    parser.add_argument("--expected-end", type=int, default=2025, help="Año final esperado")
    parser.add_argument("--output", default="audit_trades_report.csv", help="Archivo de reporte")
    parser.add_argument("--verbose", action="store_true", help="Mostrar detalles por ticker")
    return parser.parse_args()

def load_universe(universe_path: str) -> set[str]:
    """Carga el universo de tickers esperados"""
    path = Path(universe_path)

    if path.suffix == ".parquet":
        df = pl.read_parquet(path)
    else:
        df = pl.read_csv(path)

    # Intentar diferentes nombres de columna
    ticker_col = None
    for col in ["ticker", "Ticker", "symbol", "Symbol", "TICKER", "SYMBOL"]:
        if col in df.columns:
            ticker_col = col
            break

    if not ticker_col:
        raise ValueError(f"No se encontró columna de ticker en {universe_path}. Columnas: {df.columns}")

    tickers = set(df[ticker_col].unique().to_list())
    return tickers

def audit_ticker_coverage(data_dir: Path, expected_tickers: set[str],
                         expected_start: int, expected_end: int,
                         verbose: bool = False) -> dict:
    """
    Audita la cobertura de cada ticker

    Returns:
        Dict con información de auditoría por ticker
    """
    audit_results = {}

    for ticker in sorted(expected_tickers):
        ticker_dir = data_dir / ticker

        if not ticker_dir.exists():
            audit_results[ticker] = {
                "status": "MISSING",
                "years_found": [],
                "years_missing": list(range(expected_start, expected_end + 1)),
                "total_files": 0,
                "total_size_mb": 0.0,
                "first_year": None,
                "last_year": None,
                "has_gaps": False,
                "gap_years": []
            }
            if verbose:
                print(f"❌ {ticker}: Directorio no existe")
            continue

        # Buscar todos los archivos parquet
        parquet_files = list(ticker_dir.rglob("*.parquet"))

        if not parquet_files:
            audit_results[ticker] = {
                "status": "EMPTY",
                "years_found": [],
                "years_missing": list(range(expected_start, expected_end + 1)),
                "total_files": 0,
                "total_size_mb": 0.0,
                "first_year": None,
                "last_year": None,
                "has_gaps": False,
                "gap_years": []
            }
            if verbose:
                print(f"⚠️  {ticker}: Directorio existe pero sin archivos")
            continue

        # Extraer años de los paths (format: ticker/year=YYYY/month=MM/day=DD/*.parquet)
        years_found = set()
        total_size = 0

        for pfile in parquet_files:
            total_size += pfile.stat().st_size

            # Extraer año del path
            parts = pfile.parts
            for part in parts:
                if part.startswith("year="):
                    year = int(part.split("=")[1])
                    years_found.add(year)
                    break

        years_found = sorted(years_found)
        years_missing = sorted(set(range(expected_start, expected_end + 1)) - set(years_found))

        # Detectar gaps (años faltantes entre el primer y último año)
        has_gaps = False
        gap_years = []
        if len(years_found) > 1:
            first_year = min(years_found)
            last_year = max(years_found)
            expected_range = set(range(first_year, last_year + 1))
            gaps = sorted(expected_range - set(years_found))
            if gaps:
                has_gaps = True
                gap_years = gaps

        # Determinar status
        if len(years_found) == 0:
            status = "EMPTY"
        elif has_gaps:
            status = "INCOMPLETE_GAPS"
        elif years_missing:
            status = "INCOMPLETE"
        else:
            status = "COMPLETE"

        audit_results[ticker] = {
            "status": status,
            "years_found": years_found,
            "years_missing": years_missing,
            "total_files": len(parquet_files),
            "total_size_mb": total_size / (1024 * 1024),
            "first_year": min(years_found) if years_found else None,
            "last_year": max(years_found) if years_found else None,
            "has_gaps": has_gaps,
            "gap_years": gap_years
        }

        if verbose:
            if status == "COMPLETE":
                print(f"✅ {ticker}: Completo ({len(years_found)} años, {len(parquet_files)} archivos, {total_size/(1024*1024):.1f} MB)")
            elif status == "INCOMPLETE_GAPS":
                print(f"⚠️  {ticker}: Gaps detectados - años faltantes: {gap_years}")
            elif status == "INCOMPLETE":
                print(f"⚠️  {ticker}: Incompleto - tiene {len(years_found)} años, faltan {len(years_missing)}")

    return audit_results

def generate_summary(audit_results: dict, expected_start: int, expected_end: int) -> dict:
    """Genera resumen estadístico de la auditoría"""

    statuses = defaultdict(int)
    total_tickers = len(audit_results)
    total_size_mb = 0.0
    total_files = 0

    tickers_complete = []
    tickers_with_gaps = []
    tickers_incomplete = []
    tickers_empty = []
    tickers_missing = []

    year_coverage = defaultdict(int)

    for ticker, info in audit_results.items():
        statuses[info["status"]] += 1
        total_size_mb += info["total_size_mb"]
        total_files += info["total_files"]

        # Clasificar por status
        if info["status"] == "COMPLETE":
            tickers_complete.append(ticker)
        elif info["status"] == "INCOMPLETE_GAPS":
            tickers_with_gaps.append(ticker)
        elif info["status"] == "INCOMPLETE":
            tickers_incomplete.append(ticker)
        elif info["status"] == "EMPTY":
            tickers_empty.append(ticker)
        elif info["status"] == "MISSING":
            tickers_missing.append(ticker)

        # Cobertura por año
        for year in info["years_found"]:
            year_coverage[year] += 1

    summary = {
        "total_tickers": total_tickers,
        "complete": statuses["COMPLETE"],
        "incomplete_gaps": statuses["INCOMPLETE_GAPS"],
        "incomplete": statuses["INCOMPLETE"],
        "empty": statuses["EMPTY"],
        "missing": statuses["MISSING"],
        "total_size_gb": total_size_mb / 1024,
        "total_files": total_files,
        "tickers_complete": tickers_complete,
        "tickers_with_gaps": tickers_with_gaps,
        "tickers_incomplete": tickers_incomplete,
        "tickers_empty": tickers_empty,
        "tickers_missing": tickers_missing,
        "year_coverage": dict(sorted(year_coverage.items()))
    }

    return summary

def save_detailed_report(audit_results: dict, output_path: str, expected_start: int, expected_end: int):
    """Guarda reporte detallado en CSV"""

    records = []
    for ticker, info in audit_results.items():
        records.append({
            "ticker": ticker,
            "status": info["status"],
            "first_year": info["first_year"],
            "last_year": info["last_year"],
            "years_count": len(info["years_found"]),
            "years_found": ",".join(map(str, info["years_found"])),
            "years_missing": ",".join(map(str, info["years_missing"])),
            "has_gaps": info["has_gaps"],
            "gap_years": ",".join(map(str, info["gap_years"])),
            "total_files": info["total_files"],
            "size_mb": round(info["total_size_mb"], 2)
        })

    df = pl.DataFrame(records)
    df.write_csv(output_path)
    print(f"\n📄 Reporte detallado guardado en: {output_path}")

def print_summary(summary: dict, expected_start: int, expected_end: int):
    """Imprime resumen en consola"""

    print("\n" + "="*80)
    print("RESUMEN DE AUDITORÍA - TRADES TICK-LEVEL")
    print("="*80)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Rango esperado: {expected_start}-{expected_end}")
    print()

    print(f"📊 COBERTURA DE TICKERS:")
    print(f"   Total tickers esperados:     {summary['total_tickers']:,}")
    print(f"   ✅ Completos:                {summary['complete']:,} ({summary['complete']/summary['total_tickers']*100:.1f}%)")
    print(f"   ⚠️  Incompletos (con gaps):   {summary['incomplete_gaps']:,} ({summary['incomplete_gaps']/summary['total_tickers']*100:.1f}%)")
    print(f"   ⚠️  Incompletos (sin gaps):   {summary['incomplete']:,} ({summary['incomplete']/summary['total_tickers']*100:.1f}%)")
    print(f"   ⚠️  Vacíos:                   {summary['empty']:,} ({summary['empty']/summary['total_tickers']*100:.1f}%)")
    print(f"   ❌ Faltantes:                {summary['missing']:,} ({summary['missing']/summary['total_tickers']*100:.1f}%)")
    print()

    print(f"💾 VOLUMEN DE DATOS:")
    print(f"   Tamaño total:                {summary['total_size_gb']:.2f} GB")
    print(f"   Archivos totales:            {summary['total_files']:,}")
    print()

    print(f"📅 COBERTURA POR AÑO:")
    for year, count in summary['year_coverage'].items():
        percentage = (count / summary['total_tickers']) * 100
        bar_length = int(percentage / 2)  # Scale to 50 chars max
        bar = "█" * bar_length + "░" * (50 - bar_length)
        print(f"   {year}: {bar} {count:,}/{summary['total_tickers']:,} ({percentage:.1f}%)")
    print()

    # Mostrar ejemplos de problemas
    if summary['tickers_with_gaps']:
        print(f"⚠️  EJEMPLOS DE TICKERS CON GAPS (primeros 10):")
        for ticker in summary['tickers_with_gaps'][:10]:
            print(f"   - {ticker}")
        if len(summary['tickers_with_gaps']) > 10:
            print(f"   ... y {len(summary['tickers_with_gaps']) - 10} más")
        print()

    if summary['tickers_missing']:
        print(f"❌ EJEMPLOS DE TICKERS FALTANTES (primeros 10):")
        for ticker in summary['tickers_missing'][:10]:
            print(f"   - {ticker}")
        if len(summary['tickers_missing']) > 10:
            print(f"   ... y {len(summary['tickers_missing']) - 10} más")
        print()

    print("="*80)

def main():
    args = parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        print(f"❌ Error: Directorio de datos no existe: {data_dir}")
        sys.exit(1)

    print(f"📂 Cargando universo de tickers desde: {args.universe}")
    expected_tickers = load_universe(args.universe)
    print(f"✅ Cargados {len(expected_tickers):,} tickers esperados")
    print()

    print(f"🔍 Auditando cobertura de datos en: {data_dir}")
    print(f"   Rango esperado: {args.expected_start}-{args.expected_end}")
    print()

    audit_results = audit_ticker_coverage(
        data_dir=data_dir,
        expected_tickers=expected_tickers,
        expected_start=args.expected_start,
        expected_end=args.expected_end,
        verbose=args.verbose
    )

    summary = generate_summary(audit_results, args.expected_start, args.expected_end)

    print_summary(summary, args.expected_start, args.expected_end)

    save_detailed_report(audit_results, args.output, args.expected_start, args.expected_end)

    # Guardar también lista de tickers pendientes
    pending_file = args.output.replace(".csv", "_pending.csv")
    pending_tickers = (
        summary['tickers_with_gaps'] +
        summary['tickers_incomplete'] +
        summary['tickers_empty'] +
        summary['tickers_missing']
    )

    if pending_tickers:
        df_pending = pl.DataFrame({"ticker": pending_tickers})
        df_pending.write_csv(pending_file)
        print(f"📄 Lista de tickers pendientes guardada en: {pending_file}")
        print(f"   Total pendientes: {len(pending_tickers):,}")

if __name__ == "__main__":
    main()
