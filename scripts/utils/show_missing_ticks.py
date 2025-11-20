import polars as pl
import sys
import os

if len(sys.argv) < 3:
    print("Uso: python show_missing_ticks.py <ticker> <year>")
    print("Ejemplo: python show_missing_ticks.py RNVA 2016")
    sys.exit(1)

ticker = sys.argv[1]
year = int(sys.argv[2])

daily_path = f'D:/TSIS_SmallCaps/raw/polygon/ohlcv_daily/{ticker}/year={year}/daily.parquet'

# Determinar path de ticks según año
if year <= 2018:
    ticks_root = f'C:\\TSIS_Data\\trades_ticks_2004_2018_v2\\{ticker}\\'
else:
    ticks_root = f'C:\\TSIS_Data\\trades_ticks_2019_2025\\{ticker}\\'

try:
    # Leer fechas del daily
    df = pl.read_parquet(daily_path)

    # Extraer mes y día
    df = df.with_columns([
        pl.col('date').str.slice(0, 7).alias('month'),
        pl.col('date').str.slice(8, 2).cast(pl.Int32).alias('day'),
        pl.col('date').alias('full_date')
    ])

    # Verificar qué días tienen ticks
    missing_by_month = {}

    for row in df.iter_rows(named=True):
        date_str = row['full_date']
        month = row['month']
        day = row['day']

        # Construir path esperado
        date_obj = date_str.split('-')
        y = date_obj[0]
        m = date_obj[1]
        d = date_str

        market_file = f'{ticks_root}year={y}\\month={m}\\day={d}\\market.parquet'

        # Verificar si existe
        if not os.path.exists(market_file):
            if month not in missing_by_month:
                missing_by_month[month] = []
            missing_by_month[month].append(day)

    # Imprimir resultados
    if not missing_by_month:
        print()
        print(f'✅ COMPLETO: Todos los días de {ticker} año {year} tienen ticks')
        print(f'Path: {ticks_root}')
    else:
        total_missing = sum(len(days) for days in missing_by_month.values())
        total_days = len(df)

        print()
        print(f'❌ INCOMPLETO: {ticker} año {year}')
        print(f'Path: {ticks_root}')
        print(f'Días totales: {total_days}')
        print(f'Días faltantes: {total_missing} ({(total_missing/total_days*100):.1f}%)')
        print()
        print('Month, days (missing)')

        for month in sorted(missing_by_month.keys()):
            days = ','.join(map(str, sorted(missing_by_month[month])))
            print(f'{month}    {days}')

except Exception as e:
    print(f'ERROR: {e}', file=sys.stderr)
    import traceback
    traceback.print_exc()
    sys.exit(1)
