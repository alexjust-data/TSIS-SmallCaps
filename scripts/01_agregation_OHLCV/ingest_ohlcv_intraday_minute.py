#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ingest_ohlcv_intraday_minute.py  (version STREAMING + MENSUAL + ZSTD + RL ADAPTATIVO)

Adaptado para TSIS_SmallCaps:
- Acepta CSV o Parquet como input (auto-detecta)
- Período: 2019-2025 (7 años)
- Universo: 6,405 tickers Small Caps

Características:
- Descarga OHLCV 1-min de Polygon paginando con cursor.
- Escribe cada pagina directamente a disco por año/mes (sin acumular en RAM).
- Usa requests.Session() con pool=3 (TLS estable) y SIN hilos internos.
- Idempotente: mergea y deduplica por 'minute'.
- Descarga MENSUAL para reducir JSON gigante y pico de RAM.
- Compresion ZSTD con level=2 para archivos mas pequeños.
- Rate-limit ADAPTATIVO que se ajusta segun errores/exitos.

Uso tipico con launcher externo (paralelismo fuera):
  export POLYGON_API_KEY="tu_api_key"
  python scripts/ingest_ohlcv_intraday_minute.py \
    --tickers-csv processed/universe/smallcaps_universe_2025-11-01.parquet \
    --outdir raw/polygon/ohlcv_intraday_1m \
    --from 2019-01-01 --to 2025-11-01 \
    --rate-limit 0.20 \
    --max-tickers-per-process 40
"""
import os, sys, io, gc, time, argparse, datetime as dt
from pathlib import Path
from typing import Dict, Any, Optional, List
import urllib.parse as urlparse

import requests
import polars as pl
from dotenv import load_dotenv
import certifi

# stdout/stderr UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

load_dotenv()

BASE_URL   = "https://api.polygon.io"
TIMEOUT    = 40
RETRY_MAX  = 8
BACKOFF    = 1.6
# Trabajando por MESES podemos subir el limite sin reventar memoria
PAGE_LIMIT = 50000
ADJUSTED   = True

def log(m: str) -> None:
    print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] {m}", flush=True)

def load_tickers(path: str) -> List[str]:
    """
    Carga lista de tickers desde CSV o Parquet (auto-detecta)

    Args:
        path: Ruta al archivo CSV o Parquet con columna 'ticker'

    Returns:
        Lista de tickers únicos
    """
    path_obj = Path(path)

    if not path_obj.exists():
        raise FileNotFoundError(f"No existe el archivo: {path}")

    # Auto-detectar formato por extensión
    if path.endswith('.parquet'):
        log(f"Detectado formato Parquet: {path}")
        df = pl.read_parquet(path)
    elif path.endswith('.csv'):
        log(f"Detectado formato CSV: {path}")
        df = pl.read_csv(path)
    else:
        # Intentar leer como CSV por defecto
        log(f"Extensión desconocida, intentando como CSV: {path}")
        df = pl.read_csv(path)

    if "ticker" not in df.columns:
        raise ValueError(f"El archivo debe tener columna 'ticker'. Columnas encontradas: {df.columns}")

    tickers = df["ticker"].drop_nulls().unique().to_list()
    log(f"Cargados {len(tickers):,} tickers únicos")

    return tickers

def parse_next_cursor(next_url: Optional[str]) -> Optional[str]:
    if not next_url:
        return None
    try:
        q = urlparse.urlparse(next_url).query
        qs = urlparse.parse_qs(q)
        cur = qs.get("cursor")
        return cur[0] if cur else None
    except Exception:
        return None

def build_session() -> requests.Session:
    s = requests.Session()
    # Pool mejorado para reutilizar conexiones SSL (evita overhead de handshake)
    adapter = requests.adapters.HTTPAdapter(pool_connections=3, pool_maxsize=4, max_retries=0)
    s.mount("https://", adapter)
    # Desactivar gzip para evitar "Unable to allocate output buffer"
    s.headers.update({"Accept-Encoding": "identity"})
    return s

def http_get_json(session: requests.Session, url: str, params: Dict[str, Any], headers: Dict[str, str]) -> Dict[str, Any]:
    last_error = None
    for k in range(1, RETRY_MAX + 1):
        try:
            r = session.get(url, params=params, headers=headers, timeout=TIMEOUT)
            if r.status_code == 429:
                sl = int(r.headers.get("Retry-After", "2"))
                log(f"429 Rate Limit -> sleep {sl}s")
                time.sleep(sl); continue
            if 500 <= r.status_code < 600:
                sl = min(30, BACKOFF ** k)
                log(f"{r.status_code} Server Error -> backoff {sl:.1f}s")
                time.sleep(sl); continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_error = e
            msg = str(e).lower()
            if "certificate" in msg or "ssl" in msg:
                sl = 2  # retry rapido para SSL/TLS
            elif "allocate" in msg or "buffer" in msg or "decompress" in msg:
                sl = min(60, BACKOFF ** (k + 2))  # mas lento si es memoria/compresion
            else:
                sl = min(30, BACKOFF ** k)
            log(f"GET error {e} -> backoff {sl:.1f}s")
            time.sleep(sl)
    raise RuntimeError(f"Failed after {RETRY_MAX} attempts: {last_error}")

def normalize_page(results: list, ticker: str) -> pl.DataFrame:
    if not results:
        return pl.DataFrame({"ticker":[], "date":[], "minute":[], "t":[], "o":[], "h":[], "l":[], "c":[], "v":[], "n":[], "vw":[]})
    df = pl.from_dicts(results)
    # columnas esperadas de /v2/aggs: t, o, h, l, c, v, n, vw
    picks = {}
    for col, typ in [("t", pl.Int64), ("o", pl.Float64), ("h", pl.Float64),
                     ("l", pl.Float64), ("c", pl.Float64), ("v", pl.Float64),
                     ("n", pl.Int64), ("vw", pl.Float64)]:
        picks[col] = (df[col].cast(typ) if col in df.columns else pl.Series(name=col, values=[], dtype=typ))
    out = pl.DataFrame(picks)
    if out.height == 0:
        return pl.DataFrame({"ticker":[], "date":[], "minute":[], "t":[], "o":[], "h":[], "l":[], "c":[], "v":[], "n":[], "vw":[]})
    ts = pl.from_epoch(pl.col("t")/1000, time_unit="s")
    out = out.with_columns([
        ts.dt.strftime("%Y-%m-%d").alias("date"),
        ts.dt.strftime("%Y-%m-%d %H:%M").alias("minute"),
        pl.lit(ticker).alias("ticker")
    ]).select(["ticker","date","minute","t","o","h","l","c","v","n","vw"])
    return out

def write_page_by_month(df: pl.DataFrame, outdir: Path, ticker: str) -> int:
    """Escribe una pagina normalizada, particionando por año/mes, merge idempotente."""
    if df.is_empty():
        return 0
    df = df.with_columns(pl.col("date").str.slice(0,7).alias("ym"))
    files = 0
    for ym, part in df.group_by("ym"):
        year, month = ym[0].split("-")
        pdir = outdir / ticker / f"year={year}" / f"month={month}"
        pdir.mkdir(parents=True, exist_ok=True)
        outp = pdir / "minute.parquet"
        part = part.drop("ym").sort(["date","minute"])
        if outp.exists():
            old = pl.read_parquet(outp)
            merged = pl.concat([old, part], how="vertical_relaxed")\
                       .unique(subset=["minute"], keep="last")\
                       .sort(["date","minute"])
            merged.write_parquet(outp, compression="zstd", compression_level=2, statistics=False)
            del old, merged
        else:
            part.write_parquet(outp, compression="zstd", compression_level=2, statistics=False)
        files += 1
        del part
    return files

def fetch_and_stream_write(session: requests.Session, api_key: str, ticker: str, from_date: str, to_date: str,
                           rate_limit_s_ref: Optional[float], outdir: Path) -> str:
    url = f"{BASE_URL}/v2/aggs/ticker/{ticker}/range/1/minute/{from_date}/{to_date}"
    headers = {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}
    params = {"adjusted": str(ADJUSTED).lower(), "sort": "asc", "limit": PAGE_LIMIT}

    cursor = None
    pages = 0
    rows_total = 0
    files_total = 0
    # rate-limit adaptativo (compartido por llamada)
    cur_rl = rate_limit_s_ref if rate_limit_s_ref and rate_limit_s_ref > 0 else None
    ok_streak, err_streak = 0, 0
    MIN_RL, MAX_RL = 0.12, 0.35

    while True:
        p = params.copy()
        if cursor: p["cursor"] = cursor
        try:
            data = http_get_json(session, url, p, headers) or {}
            ok_streak += 1; err_streak = 0
            # si varias paginas ok seguidas, aflojamos un poco
            if cur_rl and ok_streak >= 5:
                cur_rl = max(MIN_RL, cur_rl - 0.02); ok_streak = 0
        except Exception as e:
            err_streak += 1; ok_streak = 0
            if cur_rl:
                cur_rl = min(MAX_RL, cur_rl + 0.04)
            # re-propaga para que quede registrado en results
            raise

        results = data.get("results") or []
        pages += 1
        rows_total += len(results)

        # normaliza y escribe esta pagina directamente
        df_page = normalize_page(results, ticker)
        files_total += write_page_by_month(df_page, outdir, ticker)

        # Extraer cursor ANTES de deletear data
        cursor = parse_next_cursor(data.get("next_url")) if data else None

        del df_page, results, data
        gc.collect()

        if not cursor:
            break
        if cur_rl and cur_rl > 0:
            time.sleep(cur_rl)

    return f"{ticker}: {rows_total:,} rows, {files_total} files ({pages} pages) [1m]"

def main():
    ap = argparse.ArgumentParser(description="Descarga OHLCV 1-min (streaming por pagina, sin acumulacion en RAM)")
    ap.add_argument("--tickers-csv", required=True, help="CSV o Parquet con columna 'ticker'")
    ap.add_argument("--outdir", required=True, help="raw/polygon/ohlcv_intraday_1m")
    ap.add_argument("--from", dest="date_from", required=True, help="YYYY-MM-DD")
    ap.add_argument("--to", dest="date_to", required=True, help="YYYY-MM-DD")
    ap.add_argument("--rate-limit", type=float, default=0.125, help="segundos entre paginas (por proceso)")
    ap.add_argument("--max-tickers-per-process", type=int, default=30,
                    help="Max. tickers que procesara este proceso antes de salir (libera RAM). 0=sin limite")
    # (Compatibilidad) Aceptamos --max-workers pero lo ignoramos adrede:
    ap.add_argument("--max-workers", type=int, default=1, help="(IGNORADO) Paralelismo lo maneja el launcher.")
    args = ap.parse_args()

    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        sys.exit("ERROR: variable POLYGON_API_KEY no establecida")

    # TLS en Windows: configurar certificados SSL
    os.environ.setdefault("SSL_CERT_FILE", certifi.where())
    log(f"Running INGESTOR: {__file__}")
    log(f"SSL_CERT_FILE={os.getenv('SSL_CERT_FILE')}")

    # Cargar tickers (CSV o Parquet)
    tickers = load_tickers(args.tickers_csv)
    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    rate_limit = args.rate_limit if args.rate_limit and args.rate_limit > 0 else None

    session = build_session()

    log(f"Tickers: {len(tickers):,} | {args.date_from} -> {args.date_to} | rate={rate_limit}s/page (adaptativo)")
    processed = 0
    results = []

    # === Iteracion MENSUAL ===
    # Divide [date_from, date_to] en meses y descarga por mes: menos JSON gigante, menos RAM pico.
    def month_range(dfrom: dt.date, dto: dt.date):
        y, m = dfrom.year, dfrom.month
        while (y < dto.year) or (y == dto.year and m <= dto.month):
            yield y, m
            if m == 12: y, m = y + 1, 1
            else: m += 1

    df = dt.datetime.strptime(args.date_from, "%Y-%m-%d").date()
    dt0 = dt.datetime.strptime(args.date_to,   "%Y-%m-%d").date()

    for t in tickers:
        try:
            pages_sum = 0
            rows_sum  = 0
            files_sum = 0
            for (y, m) in month_range(df, dt0):
                # primer y ultimo dia del mes
                start = dt.date(y, m, 1)
                if m == 12:
                    end = dt.date(y+1, 1, 1) - dt.timedelta(days=1)
                else:
                    end = dt.date(y, m+1, 1) - dt.timedelta(days=1)
                res = fetch_and_stream_write(
                    session, api_key, t,
                    start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"),
                    rate_limit, outdir
                )
                # recolecta contadores para el resumen por ticker
                # "TICKER: X rows, Y files (Z pages) [1m]"
                try:
                    parts = res.split(":")[1].strip().split(",")
                    rows_sum  += int(parts[0].split()[0].replace(",", ""))
                    files_sum += int(parts[1].split()[0])
                    pages_sum += int(parts[2].split()[0].replace("(", ""))
                except Exception:
                    pass
            results.append(f"{t}: {rows_sum:,} rows, {files_sum} files ({pages_sum} pages) [1m]")
        except Exception as e:
            results.append(f"{t}: ERROR {e}")
        processed += 1

        # checkpoint de progreso
        if processed % 25 == 0:
            log(f"Progreso {processed:,}/{len(tickers):,}")
            # flush a archivo de log incremental
            (outdir / "minute_download.partial.log").write_text("\n".join(results), encoding="utf-8")

        # kill-restart para liberar memoria a tope
        if args.max_tickers_per_process and processed >= args.max_tickers_per_process:
            log(f"Alcanzado --max-tickers-per-process={args.max_tickers_per_process}. Saliendo limpio para liberar RAM.")
            break

        gc.collect()

    # Log final (de lo procesado en este proceso)
    ok = sum("ERROR" not in r for r in results)
    err = len(results) - ok
    log_file = outdir / "minute_download.log"
    # append en vez de overwrite
    with open(log_file, "a", encoding="utf-8") as f:
        f.write("\n".join(results) + "\n")

    log(f"OK: {ok:,} | ERRORES: {err:,} | Log: {log_file}")

if __name__ == "__main__":
    main()
