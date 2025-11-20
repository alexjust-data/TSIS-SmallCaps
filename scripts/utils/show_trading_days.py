import polars as pl
import sys
from pathlib import Path

if len(sys.argv) < 3:
    print("Uso: python show_trading_days.py <ticker> <year>")
    print("Ejemplo: python show_trading_days.py RNVA 2016")
    sys.exit(1)

ticker = sys.argv[1]
year = int(sys.argv[2])

daily_path_year = f'D:/TSIS_SmallCaps/raw/polygon/ohlcv_daily/{ticker}/year={year}/daily.parquet'
daily_root = Path(f'D:/TSIS_SmallCaps/raw/polygon/ohlcv_daily/{ticker}')

# Determinar path de ticks según año
if year <= 2018:
    ticks_path = f'C:\\TSIS_Data\\trades_ticks_2004_2018_v2\\{ticker}\\'
else:
    ticks_path = f'C:\\TSIS_Data\\trades_ticks_2019_2025\\{ticker}\\'

try:
    # 1. Leer TODOS los años para obtener rango completo
    all_dfs = []
    year_dirs = [d for d in daily_root.iterdir() if d.is_dir() and d.name.startswith('year=')]

    for year_dir in sorted(year_dirs):
        daily_file = year_dir / 'daily.parquet'
        if daily_file.exists():
            all_dfs.append(pl.read_parquet(daily_file))

    if not all_dfs:
        print(f'ERROR: No se encontraron datos para {ticker}', file=sys.stderr)
        sys.exit(1)

    # Concatenar todos los años para estadísticas generales
    df_all = pl.concat(all_dfs)
    first_date_all = df_all['date'].min()
    last_date_all = df_all['date'].max()
    total_days_all = len(df_all)

    # Extraer años únicos del total
    years_all = df_all.select(pl.col('date').str.slice(0, 4).cast(pl.Int32).alias('year')).unique().sort('year')
    year_min = years_all['year'].min()
    year_max = years_all['year'].max()

    # 2. Leer el año específico para la tabla de meses
    df_year = pl.read_parquet(daily_path_year)
    total_days_year = len(df_year)

    # Extraer mes y día del año específico
    df_year = df_year.with_columns([
        pl.col('date').str.slice(0, 7).alias('month'),
        pl.col('date').str.slice(8, 2).cast(pl.Int32).alias('day')
    ])

    # Agrupar por mes
    result = df_year.group_by('month').agg(
        pl.col('day').sort().alias('days')
    ).sort('month')

    # Imprimir con formato solicitado
    print()
    print(f'Ticker: {ticker}')
    print(f'Path  : {ticks_path}')
    print(f'Years : {year_min} - {year_max}')
    print(f'Dates : {first_date_all} to {last_date_all}')
    print(f'Total : {total_days_all} trading days (all years), {total_days_year} trading days ({year})')
    print()
    print(f'Month, days (year {year} only)')
    for row in result.iter_rows():
        month = row[0]
        days = ','.join(map(str, row[1]))
        print(f'{month}    {days}')

except Exception as e:
    print(f'ERROR: {e}', file=sys.stderr)
    sys.exit(1)
