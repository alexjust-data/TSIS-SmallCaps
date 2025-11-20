# scripts/00_universe_ingest/filter_universe_2007_2018.py
import polars as pl
import argparse

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input",  required=True, help="raw/.../tickers_all.parquet")
    p.add_argument("--output", required=True, help="processed/universe/tickers_2007_2018.parquet (o .csv)")
    p.add_argument("--start-year", type=int, default=2007)
    p.add_argument("--end-year",   type=int, default=2018)
    p.add_argument("--details", help="(opcional) raw/.../ticker_details/.../details.parquet con list_date")
    args = p.parse_args()

    FROM = pl.date(args.start_year, 1, 1)
    TO   = pl.date(args.end_year, 12, 31)

    df = pl.read_parquet(args.input)

    # Normaliza delisted_date si existe; si solo hay delisted_utc, derivamos
    if "delisted_date" not in df.columns and "delisted_utc" in df.columns:
        df = df.with_columns(
            pl.col("delisted_utc")
              .str.strptime(pl.Datetime("us", "UTC"), "%Y-%m-%dT%H:%M:%SZ", strict=False)
              .dt.date()
              .alias("delisted_date")
        )

    # (OPCIONAL) si nos das details.parquet (ticker_details), usamos list_date para NO incluir
    # activos listados después de 2018 (reduce falsos positivos).
    # Si no lo tienes, incluimos todos los activos (como hiciste en 2019–2025).
    if args.details:
        det = pl.read_parquet(args.details)
        # algunos details traen list_date como string ISO
        if "list_date" in det.columns:
            det = det.select(
                pl.col("ticker"),
                pl.col("list_date").str.strptime(pl.Date, strict=False)
            )
            df = df.join(det, on="ticker", how="left")
        else:
            df = df.with_columns(pl.lit(None, dtype=pl.Date).alias("list_date"))
    else:
        df = df.with_columns(pl.lit(None, dtype=pl.Date).alias("list_date"))

    # LÓGICA DE FILTRADO (análogo a 2019–2025):
    # - INACTIVOS: incluir si delisted_date ∈ [2007-01-01, 2018-12-31]
    # - ACTIVOS: incluir todos (como en 2019–2025) o, si tenemos list_date, solo si list_date ≤ 2018-12-31
    inactivos = df.filter(
        (pl.col("active") == False) &
        (pl.col("delisted_date").is_not_null()) &
        (pl.col("delisted_date") >= FROM) &
        (pl.col("delisted_date") <= TO)
    )

    # Activos con o sin list_date
    activos = df.filter(pl.col("active") == True)
    if "list_date" in activos.columns:
        activos = activos.filter(
            (pl.col("list_date").is_null()) |  # si no hay list_date, asumimos posible existencia
            (pl.col("list_date") <= TO)
        )

    out = pl.concat([activos, inactivos], how="vertical_relaxed").unique("ticker")

    # Guarda igual que en 2019–2025 (parquet o csv, según extensión)
    if args.output.lower().endswith(".csv"):
        out.write_csv(args.output)
    else:
        out.write_parquet(args.output)

    # Mini resumen en consola
    print(f"Total filtrado 2007–2018: {out.height:,}")
    print(out.select([
        pl.count().alias("tickers"),
        pl.col("active").sum().alias("activos_hoy"),
        (pl.count() - pl.col("active").sum()).alias("inactivos_hoy")
    ]))

if __name__ == "__main__":
    main()
