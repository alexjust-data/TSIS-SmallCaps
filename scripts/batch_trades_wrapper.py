#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
batch_trades_wrapper.py - Micro-batches para descarga de trades tick-level

Adaptado para TSIS_SmallCaps:
- Período: 2019-2025 (7 años)
- Universo: 6,405 tickers Small Caps
- Descarga DIARIA para evitar JSONs gigantes en tickers líquidos

Uso:
  export POLYGON_API_KEY=xxx

  python scripts/batch_trades_wrapper.py \
    --tickers-csv processed/universe/smallcaps_universe_2025-11-01.parquet \
    --outdir raw/polygon/trades_ticks \
    --from 2019-01-01 --to 2025-11-01 \
    --batch-size 15 \
    --max-concurrent 6 \
    --rate-limit 0.18 \
    --ingest-script scripts/ingest_trades_ticks.py \
    --resume
"""
from __future__ import annotations
import os, sys, time, argparse, subprocess
from pathlib import Path
from typing import List, Set, Tuple
from datetime import datetime
import polars as pl
from concurrent.futures import ThreadPoolExecutor, as_completed

def log(msg: str) -> None:
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)

def load_tickers(path: str) -> List[str]:
    """Carga lista de tickers desde CSV o Parquet (auto-detecta)"""
    path_obj = Path(path)

    if not path_obj.exists():
        raise FileNotFoundError(f"No existe el archivo: {path}")

    if path.endswith('.parquet'):
        log(f"Detectado formato Parquet: {path}")
        df = pl.read_parquet(path)
    elif path.endswith('.csv'):
        log(f"Detectado formato CSV: {path}")
        df = pl.read_csv(path)
    else:
        log(f"Extensión desconocida, intentando como CSV: {path}")
        df = pl.read_csv(path)

    if "ticker" not in df.columns:
        raise ValueError(f"El archivo debe tener columna 'ticker'. Columnas encontradas: {df.columns}")

    tickers = df["ticker"].drop_nulls().unique().to_list()
    log(f"Cargados {len(tickers):,} tickers únicos")

    return tickers

def get_completed_tickers(outdir: Path) -> Set[str]:
    if not outdir.exists():
        return set()
    done = set()
    for tdir in outdir.iterdir():
        if not tdir.is_dir(): continue
        if tdir.name == '_batch_temp': continue
        # Si existe cualquier year=*/month=*/day=*/premarket.parquet o market.parquet
        any_parquet = any(
            (y.is_dir() and
             any(m.is_dir() and
                 any(d.is_dir() and
                     (any(f.name in ["premarket.parquet", "market.parquet"]
                          for f in d.glob("*.parquet")))
                     for d in m.glob("day=*"))
                 for m in y.glob("month=*")))
            for y in tdir.glob("year=*"))
        if any_parquet:
            done.add(tdir.name)
    return done

def chunk_list(lst: List[str], size: int) -> List[List[str]]:
    return [lst[i:i+size] for i in range(0, len(lst), size)]

def run_batch(batch_id: int, tickers: List[str], args, script_path: Path, temp_dir: Path, tries: int = 2) -> Tuple[int, str, float]:
    """Lanza un subproceso del ingestor procesando este batch"""
    start = time.time()
    csv_path = temp_dir / f"batch_{batch_id:04d}.csv"
    pl.DataFrame({"ticker": tickers}).write_csv(csv_path)

    log_path = temp_dir / f"batch_{batch_id:04d}.log"
    cmd = [
        sys.executable, str(script_path),
        "--tickers-csv", str(csv_path),
        "--outdir", args.outdir,
        "--from", args.date_from,
        "--to", args.date_to,
        "--rate-limit", str(args.rate_limit),
        "--max-tickers-per-process", str(len(tickers)),
        "--max-workers", "1",
    ]

    env = os.environ.copy()

    attempt = 0
    rc = 1
    while attempt < tries:
        attempt += 1
        with open(log_path, "a", encoding="utf-8") as lf:
            lf.write(f"== BATCH {batch_id:04d} attempt {attempt}/{tries} ==\n")
            lf.flush()
            proc = subprocess.run(cmd, stdout=lf, stderr=subprocess.STDOUT, text=True, env=env)
            rc = proc.returncode
        if rc == 0:
            break
        time.sleep(3)

    elapsed = time.time() - start
    try:
        csv_path.unlink(missing_ok=True)
    except Exception:
        pass

    status = "success" if rc == 0 else f"failed(rc={rc})"
    return (batch_id, status, elapsed)

def main():
    ap = argparse.ArgumentParser(description="Wrapper de micro-batches para trades tick-level")
    ap.add_argument("--tickers-csv", required=True, help="CSV o Parquet con columna 'ticker'")
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--from", dest="date_from", required=True)
    ap.add_argument("--to", dest="date_to", required=True)
    ap.add_argument("--batch-size", type=int, default=15)
    ap.add_argument("--max-concurrent", type=int, default=6)
    ap.add_argument("--rate-limit", type=float, default=0.18)
    ap.add_argument("--ingest-script", required=True, help="Ruta al ingest_trades_ticks.py")
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()

    if not os.getenv("POLYGON_API_KEY"):
        sys.exit("ERROR: POLYGON_API_KEY no está definida")

    script_path = Path(args.ingest_script)
    if not script_path.exists():
        sys.exit(f"ERROR: no encuentro el ingestor en {script_path}")

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    temp_dir = outdir / "_batch_temp"; temp_dir.mkdir(exist_ok=True)

    all_tickers = load_tickers(args.tickers_csv)

    if args.resume:
        completed = get_completed_tickers(outdir)
        tickers = [t for t in all_tickers if t not in completed]
        log(f"--resume: {len(completed):,} tickers ya con datos | pendientes: {len(tickers):,}")
    else:
        tickers = all_tickers
        log(f"Pendientes: {len(tickers):,}")

    if not tickers:
        log("Nada que hacer - todos los tickers ya tienen datos")
        return

    batches = chunk_list(tickers, args.batch_size)
    log("== Config ==")
    log(f"  Universo pendiente: {len(tickers):,} tickers")
    log(f"  Batches: {len(batches)} x {args.batch_size} tickers")
    log(f"  Concurrencia: {args.max_concurrent} batches a la vez")
    log(f"  Ventana: {args.date_from} -> {args.date_to}")
    log(f"  Ingestor: {script_path}")

    start = time.time()
    results = []

    with ThreadPoolExecutor(max_workers=args.max_concurrent) as ex:
        futs = {ex.submit(run_batch, i, b, args, script_path, temp_dir): i for i, b in enumerate(batches)}
        for fut in as_completed(futs):
            bid, status, elapsed = fut.result()
            results.append((bid, status, elapsed))
            done = len(results); pct = done / len(batches) * 100
            log(f"Batch {bid:04d}: {status} ({elapsed:.1f}s) | Progreso {done}/{len(batches)} = {pct:.1f}%")

    ok = sum(1 for _, s, _ in results if s == "success")
    fail = len(results) - ok
    elapsed_all = time.time() - start
    log("\n" + "="*60)
    log(f"COMPLETADO: {ok}/{len(results)} batches OK | {fail} fallidos")
    log(f"Tiempo total: {elapsed_all/3600:.2f} h")
    log(f"Logs por batch: {temp_dir}/")

if __name__ == "__main__":
    main()
