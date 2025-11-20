#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Auditoría de integridad del dataset descargado de trades.
Detecta:
  - Días con parquet pero sin _SUCCESS
  - Días con _SUCCESS pero sin parquet
  - Días faltantes dentro de la vida real del ticker
Genera:
  - CSV de días sospechosos
  - Resumen global
"""

import os
from pathlib import Path
import polars as pl
import argparse
from datetime import date, timedelta

def daterange(start, end):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--outdir", required=True)
    p.add_argument("--ping", required=True, help="ping_binary_XXXX.parquet con first_day")
    p.add_argument("--period-start", required=True)
    p.add_argument("--period-end", required=True)
    p.add_argument("--out", default="audit_result.csv")
    args = p.parse_args()

    OUTDIR = Path(args.outdir.replace("\\", "/"))
    PING = pl.read_parquet(args.ping.replace("\\", "/"))

    START = date.fromisoformat(args.period_start)
    END   = date.fromisoformat(args.period_end)

    rows = []
    checked_tickers = 0
    found_tickers = 0

    for rec in PING.iter_rows(named=True):
        ticker = rec["ticker"]
        has_history = rec[list(rec.keys())[1]]  # columna TRUE/FALSE
        first_day_str = rec[list(rec.keys())[2]]
        if not has_history:
            continue

        checked_tickers += 1
        first_day = date.fromisoformat(first_day_str)

        ticker_dir = OUTDIR / ticker
        if not ticker_dir.exists():
            rows.append({
                "ticker": ticker,
                "date": None,
                "issue": "TICKER_DIR_MISSING"
            })
            continue

        found_tickers += 1

        # Determinar el rango real a auditar
        audit_start = max(first_day, START)
        audit_end   = END

        for d in daterange(audit_start, audit_end):
            y = f"{d.year:04d}"
            m = f"{d.month:02d}"
            ds = d.isoformat()

            day_dir = ticker_dir / f"year={y}" / f"month={m}" / f"day={ds}"
            success = (day_dir / "_SUCCESS").exists()
            pm = (day_dir / "premarket.parquet").exists()
            mk = (day_dir / "market.parquet").exists()

            # A: parquet pero NO _SUCCESS → día parcial
            if (pm or mk) and not success:
                rows.append({
                    "ticker": ticker,
                    "date": ds,
                    "issue": "PARTIAL_DAY_NO_SUCCESS"
                })

            # B: _SUCCESS pero NO parquet → corrupto
            if success and not (pm or mk):
                rows.append({
                    "ticker": ticker,
                    "date": ds,
                    "issue": "SUCCESS_NO_PARQUET"
                })

    df = pl.DataFrame(rows) if rows else pl.DataFrame({"ticker": [], "date": [], "issue": []})
    df.write_csv(args.out)

    # Summary statistics
    total_tickers_with_history = PING.filter(pl.col(PING.columns[1])).height

    if len(df) > 0:
        ticker_dir_missing = len([r for r in rows if r["issue"] == "TICKER_DIR_MISSING"])
        partial_days = len([r for r in rows if r["issue"] == "PARTIAL_DAY_NO_SUCCESS"])
        corrupt_days = len([r for r in rows if r["issue"] == "SUCCESS_NO_PARQUET"])
    else:
        ticker_dir_missing = 0
        partial_days = 0
        corrupt_days = 0

    tickers_completed = total_tickers_with_history - ticker_dir_missing
    completion_pct = (tickers_completed / total_tickers_with_history * 100) if total_tickers_with_history > 0 else 0

    print(f"\n{'='*60}")
    print(f"AUDIT SUMMARY")
    print(f"{'='*60}")
    print(f"Output directory: {OUTDIR}")
    print(f"Checked tickers: {checked_tickers:,}")
    print(f"Found tickers: {found_tickers:,}")
    print(f"Total tickers with history: {total_tickers_with_history:,}")
    print(f"Tickers completed: {tickers_completed:,} ({completion_pct:.1f}%)")
    print(f"Tickers missing: {ticker_dir_missing:,} ({100-completion_pct:.1f}%)")
    print(f"\nIssues found:")
    print(f"  - Tickers not downloaded: {ticker_dir_missing:,}")
    print(f"  - Partial days (no _SUCCESS): {partial_days:,}")
    print(f"  - Corrupt days (_SUCCESS but no parquet): {corrupt_days:,}")
    print(f"\nTotal issues: {len(rows):,}")
    print(f"Audit saved to {args.out}")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
