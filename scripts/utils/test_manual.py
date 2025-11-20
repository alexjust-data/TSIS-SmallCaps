import polars as pl
import os

# Test manual de descarga
api_key = os.environ.get("POLYGON_API_KEY")
if not api_key:
    print("ERROR: Configura POLYGON_API_KEY")
    exit(1)

import requests

# Test con BRT para un día
ticker = "BRT"
date = "2017-01-03"
url = f"https://api.polygon.io/v3/trades/{ticker}?timestamp.gte=2017-01-03T09:30:00Z&timestamp.lte=2017-01-03T16:00:00Z&limit=100&apiKey={api_key}"

response = requests.get(url)
data = response.json()

print(f"Status: {data.get('status')}")
print(f"Results count: {data.get('resultsCount', 0)}")

if 'results' in data and data['results']:
    trades = data['results']
    print(f"Primeros 3 trades:")
    for i, trade in enumerate(trades[:3]):
        print(f"  Trade {i}: {trade}")
    
    # Intentar crear DataFrame
    try:
        df = pl.DataFrame(trades)
        print(f"\nDataFrame creado: {len(df)} filas")
        print(f"Columnas: {df.columns}")
        print(f"Tipos: {df.dtypes}")
    except Exception as e:
        print(f"\nError creando DataFrame: {e}")
        
        # Analizar tipos de datos
        print("\nAnalizando tipos de datos:")
        sample_trade = trades[0]
        for key, value in sample_trade.items():
            print(f"  {key}: {type(value).__name__} = {value}")