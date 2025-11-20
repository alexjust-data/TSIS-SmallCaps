import requests
import os
import time
from datetime import datetime

api_key = "G4fe7xkLNVEWmWeVHcuXx73P2gfRE6mN"

# Tickers de la lista de missing
test_tickers = ['MED', 'MGN', 'MIW', 'MJI', 'MLR', 'MPB', 'MPX', 'MTS', 'NAT', 'NBG']

print('=== TEST TICKERS MISSING - DISPONIBILIDAD 2004 vs 2011 ===\n')

for ticker in test_tickers:
    # Test 2004
    url_2004 = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/2004-01-02/2004-01-10'
    r_2004 = requests.get(url_2004, params={'apiKey': api_key}, timeout=10)
    data_2004 = r_2004.json()
    count_2004 = data_2004.get('resultsCount', 0)

    time.sleep(0.08)

    # Test 2011
    url_2011 = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/2011-10-17/2011-10-25'
    r_2011 = requests.get(url_2011, params={'apiKey': api_key}, timeout=10)
    data_2011 = r_2011.json()
    count_2011 = data_2011.get('resultsCount', 0)

    print(f'{ticker:6} | 2004: {count_2004:2} días | 2011: {count_2011:2} días | Status: {data_2004.get("status", "ERROR")}')

    # Si hay datos en 2004, mostrar el primer precio
    if count_2004 > 0 and 'results' in data_2004:
        result = data_2004['results'][0]
        t_date = result.get('t', 0)
        date_str = datetime.fromtimestamp(t_date/1000).strftime('%Y-%m-%d')
        print(f'       2004: {date_str} C={result.get("c")} V={result.get("v")}')

    # Si hay datos en 2011, mostrar el primer precio
    if count_2011 > 0 and 'results' in data_2011:
        result = data_2011['results'][0]
        t_date = result.get('t', 0)
        date_str = datetime.fromtimestamp(t_date/1000).strftime('%Y-%m-%d')
        print(f'       2011: {date_str} C={result.get("c")} V={result.get("v")}')

    time.sleep(0.08)
    print()
