# scripts/00_universe_ingest/filter_universe_cs_exchanges_2007_2018.py
import polars as pl
import argparse

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input",  required=True, help="processed/universe/tickers_2007_2018.parquet")
    p.add_argument("--output", required=True, help="processed/universe/tickers_2007_2018_cs_exchanges.parquet")
    args = p.parse_args()

    df = pl.read_parquet(args.input)
    out = df.filter(
        (pl.col("type") == "CS") &
        (pl.col("primary_exchange").is_in(["XNAS", "XNYS"]))
    )
    if args.output.lower().endswith(".csv"):
        out.write_csv(args.output)
    else:
        out.write_parquet(args.output)

    print(f"CS+XNAS/XNYS 2007–2018: {out.height:,}")

if __name__ == "__main__":
    main()
