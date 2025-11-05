#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ingest_trades_ticks.py - Descarga trades tick-level (premarket + market)

Adaptado para TSIS_SmallCaps:
- Período: 2019-2025 (7 años)
- Universo: 6,405 tickers Small Caps
- Descarga DIARIA (evita JSONs gigantes en tickers líquidos)
- Separación premarket/market por timestamp

Horarios de mercado (ET):
  PREMARKET:  04:00 - 09:30 ET  → premarket.parquet
  MARKET:     09:30 - 16:00 ET  → market.parquet

Uso:
  export POLYGON_API_KEY="tu_api_key"
  python scripts/ingest_trades_ticks.py \
    --tickers-csv processed/universe/smallcaps_universe_2025-11-01.parquet \
    --outdir raw/polygon/trades_ticks \
    --from 2019-01-01 --to 2025-11-01 \
    --rate-limit 0.18 \
    --max-tickers-per-process 15
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
TIMEOUT    = 45
RETRY_MAX  = 8
BACKOFF    = 1.6
PAGE_LIMIT = 50000
ADJUSTED   = True

# Horarios de mercado en timestamps Unix (milisegundos)
# Usaremos hora UTC equivalente (ET - 4/5 horas según DST)
# Simplificado: usar rangos de hora del día
PREMARKET_START_HOUR = 4   # 04:00 ET
PREMARKET_END_HOUR = 9     # 09:30 ET (hasta las 09:29:59)
MARKET_START_HOUR = 9      # 09:30 ET
MARKET_END_HOUR = 16       # 16:00 ET

def log(m: str) -> None:
    print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] {m}", flush=True)

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
    # Pool más grande para concurrencia real
    adapter = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=64, max_retries=0)
    s.mount("https://", adapter)
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
                sl = 2
            elif "allocate" in msg or "buffer" in msg or "decompress" in msg:
                sl = min(60, BACKOFF ** (k + 2))
            else:
                sl = min(30, BACKOFF ** k)
            log(f"GET error {e} -> backoff {sl:.1f}s")
            time.sleep(sl)
    raise RuntimeError(f"Failed after {RETRY_MAX} attempts: {last_error}")

def normalize_trades(results: list, ticker: str, trade_date: str, args=None) -> tuple:
    """
    Normaliza trades y separa en premarket/market según timestamp

    Returns:
        (df_premarket, df_market)
    """
    if not results:
        empty_schema = {"ticker":[], "date":[], "timestamp":[], "price":[], "size":[], "exchange":[], "conditions":[]}
        return pl.DataFrame(empty_schema), pl.DataFrame(empty_schema)

    df = pl.from_dicts(results)

    # Columnas de /v3/trades API (formato completo):
    # participant_timestamp, sip_timestamp, price, size, exchange, conditions
    # Usamos sip_timestamp (SIP=Securities Information Processor, oficial)
    picks = {}
    # Mapeo: nombre_esperado -> (nombre_real, tipo)
    col_map = {
        "t": ("sip_timestamp", pl.Int64),
        "p": ("price", pl.Float64),
        "s": ("size", pl.Int64),
        "x": ("exchange", pl.Int64)
    }

    for dest_col, (src_col, typ) in col_map.items():
        if src_col in df.columns:
            picks[dest_col] = df[src_col].cast(typ)
        else:
            picks[dest_col] = pl.Series(name=dest_col, values=[], dtype=typ)

    # Conditions
    if "conditions" in df.columns:
        picks["c"] = df["conditions"]
    else:
        picks["c"] = pl.Series(name="c", values=[], dtype=pl.List(pl.Int64))

    out = pl.DataFrame(picks)

    if out.height == 0:
        empty_schema = {"ticker":[], "date":[], "timestamp":[], "price":[], "size":[], "exchange":[], "conditions":[]}
        return pl.DataFrame(empty_schema), pl.DataFrame(empty_schema)

    # 1) Construir timestamp UTC a partir de nanos
    out = out.with_columns([
        pl.from_epoch((pl.col("t") / 1_000_000_000), time_unit="s").alias("timestamp_utc"),
        pl.lit(ticker).alias("ticker"),
        pl.lit(trade_date).alias("date"),
        pl.col("p").alias("price"),
        pl.col("s").alias("size"),
        pl.col("x").alias("exchange"),
        pl.col("c").alias("conditions")
    ])

    # 2) Hacer timezone-aware y convertir a America/New_York para el split logico
    out = out.with_columns([
        pl.col("timestamp_utc")
          .dt.replace_time_zone("UTC")
          .dt.convert_time_zone("America/New_York")
          .alias("timestamp_et")
    ]).select([
        "ticker","date","timestamp_utc","timestamp_et","price","size","exchange","conditions"
    ]).rename({"timestamp_utc": "timestamp"})

    # Separar premarket (04:00-09:30 ET) vs market (09:30-20:00 ET) usando hora local ET
    # EXTENDIDO: capturamos after-hours hasta 20:00 ET
    out = out.with_columns([
        pl.col("timestamp_et").dt.hour().alias("hour_et"),
        pl.col("timestamp_et").dt.minute().alias("minute_et")
    ])

    # Premarket: hour_et in [4,5,6,7,8] OR (hour_et=9 AND minute_et<30)
    df_premarket = out.filter(
        (pl.col("hour_et") >= PREMARKET_START_HOUR) &
        ((pl.col("hour_et") < MARKET_START_HOUR) |
         ((pl.col("hour_et") == MARKET_START_HOUR) & (pl.col("minute_et") < 30)))
    ).drop(["hour_et","minute_et","timestamp_et"])

    # Market regular y, opcionalmente, after-hours
    if args and getattr(args, "no_afterhours", False):
        df_market = out.filter(
            ((pl.col("hour_et") == MARKET_START_HOUR) & (pl.col("minute_et") >= 30)) |
            ((pl.col("hour_et") > MARKET_START_HOUR) & (pl.col("hour_et") < 16))
        ).drop(["hour_et","minute_et","timestamp_et"])
    else:
        # Regular + after-hours hasta 20:00 ET
        df_market = out.filter(
            ((pl.col("hour_et") == MARKET_START_HOUR) & (pl.col("minute_et") >= 30)) |
            ((pl.col("hour_et") > MARKET_START_HOUR) & (pl.col("hour_et") < 20))
        ).drop(["hour_et","minute_et","timestamp_et"])

    return df_premarket, df_market

def write_trades_by_day(df_premarket: pl.DataFrame, df_market: pl.DataFrame,
                        outdir: Path, ticker: str, trade_date: str) -> int:
    """Escribe trades separados en premarket.parquet y market.parquet"""
    if df_premarket.height == 0 and df_market.height == 0:
        return 0

    # Extraer año/mes de la fecha
    year, month = trade_date.split("-")[0], trade_date.split("-")[1]

    pdir = outdir / ticker / f"year={year}" / f"month={month}" / f"day={trade_date}"
    pdir.mkdir(parents=True, exist_ok=True)

    files_written = 0

    # Escribir premarket
    if not df_premarket.is_empty():
        outp_pre = pdir / "premarket.parquet"
        df_premarket = df_premarket.sort("timestamp")

        if outp_pre.exists():
            old = pl.read_parquet(outp_pre)
            merged = pl.concat([old, df_premarket], how="vertical_relaxed")\
                       .unique(subset=["timestamp"], keep="last")\
                       .sort("timestamp")
            merged.write_parquet(outp_pre, compression="zstd", compression_level=1, statistics=False)
            del old, merged
        else:
            df_premarket.write_parquet(outp_pre, compression="zstd", compression_level=1, statistics=False)

        files_written += 1

    # Escribir market
    if not df_market.is_empty():
        outp_mkt = pdir / "market.parquet"
        df_market = df_market.sort("timestamp")

        if outp_mkt.exists():
            old = pl.read_parquet(outp_mkt)
            merged = pl.concat([old, df_market], how="vertical_relaxed")\
                       .unique(subset=["timestamp"], keep="last")\
                       .sort("timestamp")
            merged.write_parquet(outp_mkt, compression="zstd", compression_level=1, statistics=False)
            del old, merged
        else:
            df_market.write_parquet(outp_mkt, compression="zstd", compression_level=1, statistics=False)

        files_written += 1

    return files_written

def fetch_and_stream_write_trades(session: requests.Session, api_key: str, ticker: str,
                                   trade_date: str, rate_limit_s: Optional[float], outdir: Path, args=None) -> str:
    """Descarga trades para un día específico y escribe directamente"""
    # Convertir fecha a timestamp Unix (nanosegundos)
    date_obj = dt.datetime.strptime(trade_date, "%Y-%m-%d")
    start_ns = int(date_obj.timestamp() * 1_000_000_000)
    end_ns = start_ns + (24 * 3600 * 1_000_000_000) - 1  # hasta 23:59:59.999999999

    url = f"{BASE_URL}/v3/trades/{ticker}"
    headers = {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}
    params = {
        "timestamp.gte": start_ns,
        "timestamp.lte": end_ns,
        "limit": PAGE_LIMIT,
        "sort": "timestamp"
    }

    cursor = None
    pages = 0
    rows_total = 0
    files_total = 0

    cur_rl = rate_limit_s if rate_limit_s and rate_limit_s > 0 else None
    ok_streak, err_streak = 0, 0
    # Límites de rate adaptativo (bajamos el mínimo)
    MIN_RL, MAX_RL = 0.06, 0.40

    while True:
        p = params.copy()
        if cursor: p["cursor"] = cursor

        try:
            data = http_get_json(session, url, p, headers) or {}
            ok_streak += 1; err_streak = 0
            if cur_rl and ok_streak >= 5:
                cur_rl = max(MIN_RL, cur_rl - 0.02); ok_streak = 0
        except Exception as e:
            err_streak += 1; ok_streak = 0
            if cur_rl:
                cur_rl = min(MAX_RL, cur_rl + 0.04)
            raise

        results = data.get("results") or []
        pages += 1
        rows_total += len(results)

        # Normalizar y separar premarket/market
        df_premarket, df_market = normalize_trades(results, ticker, trade_date, args)
        files_total += write_trades_by_day(df_premarket, df_market, outdir, ticker, trade_date)

        cursor = parse_next_cursor(data.get("next_url")) if data else None

        del df_premarket, df_market, results, data
        gc.collect()

        if not cursor:
            break
        if cur_rl and cur_rl > 0:
            time.sleep(cur_rl)

    return f"{ticker} {trade_date}: {rows_total:,} trades, {files_total} files ({pages} pages)"

def main():
    ap = argparse.ArgumentParser(description="Descarga trades tick-level (streaming por día)")
    ap.add_argument("--tickers-csv", required=True, help="CSV o Parquet con columna 'ticker'")
    ap.add_argument("--outdir", required=True, help="raw/polygon/trades_ticks")
    ap.add_argument("--from", dest="date_from", required=True, help="YYYY-MM-DD")
    ap.add_argument("--to", dest="date_to", required=True, help="YYYY-MM-DD")
    ap.add_argument("--rate-limit", type=float, default=0.18, help="segundos entre páginas")
    ap.add_argument("--max-tickers-per-process", type=int, default=15,
                    help="Max. tickers que procesará este proceso antes de salir")
    ap.add_argument("--max-workers", type=int, default=1, help="(IGNORADO) Paralelismo lo maneja el launcher")
    # Opciones nuevas:
    ap.add_argument("--skip-weekends", action="store_true", help="Saltar sábados y domingos")
    ap.add_argument("--skip-us-holidays", action="store_true", help="Saltar festivos USA (lista simple embebida)")
    ap.add_argument("--no-afterhours", action="store_true", help="Excluir after-hours (solo premarket+regular)")
    args = ap.parse_args()

    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        sys.exit("ERROR: variable POLYGON_API_KEY no establecida")

    # TLS en Windows
    os.environ.setdefault("SSL_CERT_FILE", certifi.where())
    log(f"Running TRADES INGESTOR: {__file__}")
    log(f"SSL_CERT_FILE={os.getenv('SSL_CERT_FILE')}")

    tickers = load_tickers(args.tickers_csv)
    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    rate_limit = args.rate_limit if args.rate_limit and args.rate_limit > 0 else None

    session = build_session()

    log(f"Tickers: {len(tickers):,} | {args.date_from} -> {args.date_to} | rate={rate_limit}s/page (adaptativo)")
    processed = 0
    results = []

    # Lista mínima de festivos USA (amplíala si quieres)
    US_HOLIDAYS = {
        "2019-01-01","2019-07-04","2019-12-25",
        "2020-01-01","2020-07-03","2020-12-25",
        "2021-01-01","2021-07-05","2021-12-24",
        "2022-01-17","2022-07-04","2022-12-26",
        "2023-01-02","2023-07-04","2023-12-25",
        "2024-01-01","2024-07-04","2024-12-25",
        "2025-01-01","2025-07-04"
    }

    # Iterar por DÍAS (descarga diaria)
    def date_range(start: dt.date, end: dt.date):
        current = start
        while current <= end:
            # Opcionalmente saltar fines de semana y festivos
            if args.skip_weekends and current.weekday() >= 5:
                current += dt.timedelta(days=1)
                continue
            if args.skip_us_holidays and current.strftime("%Y-%m-%d") in US_HOLIDAYS:
                current += dt.timedelta(days=1)
                continue
            yield current
            current += dt.timedelta(days=1)

    df = dt.datetime.strptime(args.date_from, "%Y-%m-%d").date()
    dt0 = dt.datetime.strptime(args.date_to, "%Y-%m-%d").date()

    for t in tickers:
        try:
            days_sum = 0
            rows_sum = 0
            files_sum = 0

            log(f"Processing {t} (ticker {processed+1}/{len(tickers)})...")

            for day in date_range(df, dt0):
                day_str = day.strftime("%Y-%m-%d")

                res = fetch_and_stream_write_trades(
                    session, api_key, t, day_str, rate_limit, outdir, args
                )

                # Parsear resultado
                try:
                    parts = res.split(":")
                    trades_part = parts[1].split(",")[0].strip().split()[0].replace(",", "")
                    rows_sum += int(trades_part) if trades_part.isdigit() else 0
                    days_sum += 1
                except Exception:
                    pass

                # Log progreso cada 200 días
                if days_sum > 0 and days_sum % 200 == 0:
                    log(f"  {t}: {days_sum}/~2555 días | {rows_sum:,} trades")

            results.append(f"{t}: {rows_sum:,} trades, {days_sum} days")
            log(f"Completed {t}: {rows_sum:,} trades total")
        except Exception as e:
            results.append(f"{t}: ERROR {e}")

        processed += 1

        if processed % 10 == 0:
            log(f"Progreso {processed:,}/{len(tickers):,}")
            (outdir / "trades_download.partial.log").write_text("\n".join(results), encoding="utf-8")

        if args.max_tickers_per_process and processed >= args.max_tickers_per_process:
            log(f"Alcanzado --max-tickers-per-process={args.max_tickers_per_process}. Saliendo limpio.")
            break

        gc.collect()

    # Log final
    ok = sum("ERROR" not in r for r in results)
    err = len(results) - ok
    log_file = outdir / "trades_download.log"

    with open(log_file, "a", encoding="utf-8") as f:
        f.write("\n".join(results) + "\n")

    log(f"OK: {ok:,} | ERRORES: {err:,} | Log: {log_file}")

if __name__ == "__main__":
    main()
