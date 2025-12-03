

Daily local [`raw/polygon/ohlcv_daily`] se usa como referencia para intraday SOLO porque ya fue certificado al 100% contra Polygon usando verify_daily_vs_ping.py

```sh
La Jerarquía Correcta:

1. FUENTE DE VERDAD ABSOLUTA (Autoridad máxima):
   📡 Polygon API
   └─> ping_polygon_range.py # consulta a Polygon
       └─> Genera: ping_range_2019_2025.parquet
           └─> Contiene: first_day, last_day por ticker según Polygon

2. DATOS LOCALES (Lo que tienes descargado):
   💾 raw/polygon/ohlcv_daily/
   └─> Tu copia local de datos daily

3. VERIFICACIÓN:
   ✅ verify_daily_vs_ping.py compara:
      ping_range (fuente de verdad) vs ohlcv_daily (copia local)
```

**Por Qué `verify_all_intraday_1m.py` Compara Contra Daily Local:

```sh
Para intraday 1m, la jerarquía es:
1. FUENTE DE VERDAD:
   📡 ping_range_2019_2025.parquet (Polygon dice qué debe existir)
   
2. DAILY LOCAL (primera verificación):
   💾 raw/polygon/ohlcv_daily/ 
   └─> Ya verificado como completo (100% match con ping)
   
3. INTRADAY 1M (segunda verificación):
   💾 C:\TSIS_Data\ohlcv_intraday_1m\
   └─> Debe tener los mismos días que daily local

Entonces:
✅ verify_daily_vs_ping.py → Certifica que daily local = Polygon (100% completo)
✅ verify_all_intraday_1m.py → Certifica que intraday 1m = daily local (ya validado)
```

```sh
# 1. Descargar 2004-2018 con su universe
python scripts\01_agregation_OHLCV\ingest_intraday_ultra_fast.py `
  --tickers-csv processed/universe/ping_range_2004_2018.parquet `
  --outdir C:\TSIS_Data\ohlcv_intraday_1m\ `
  --daily-dir raw/polygon/ohlcv_daily\2004_2018 `
  --start-year 2004 `
  --end-year 2018 `
  --concurrent 60

# 2. Descargar 2019-2025 con su universe
# PS D:\TSIS_SmallCaps> Remove-Item -Recurse "C:\TSIS_Data\ohlcv_intraday_1m\2019_2025\*"
PS D:\TSIS_SmallCaps> python scripts\01_agregation_OHLCV\ingest_intraday_ultra_fast.py `
>>   --tickers-csv processed/universe/ping_range_2019_2025.parquet `
>>   --outdir C:\TSIS_Data\ohlcv_intraday_1m\2019_2025 `
>>   --daily-dir raw/polygon/ohlcv_daily `
>>   --start-year 2019 `
>>   --end-year 2025 `
>>   --concurrent 60
Tickers: 6405
============================================================
DESCARGA ULTRA-RÁPIDA DE INTRADAY 1M
============================================================
Tickers: 6,405
Período: 2019-2025
Concurrencia: 60
Daily dir: raw\polygon\ohlcv_daily
[09:41:01] 20/6405 (0.3%) | 0.55 tickers/s | ETA: 191.8m | Rows: 2.6M | Skip: 0
[09:41:42] 40/6405 (0.6%) | 0.52 tickers/s | ETA: 204.3m | Rows: 4.8M | Skip: 0
[09:42:29] 60/6405 (0.9%) | 0.48 tickers/s | ETA: 218.3m | Rows: 7.7M | Skip: 0
...

```
**verify_all_intraday_1m.py**

This script verifies ALL tickers in parallel, comparing downloaded intraday 1m data against expected data from daily files. 

```sh
# Basic verification for 2019-2025 with 16 workers
python scripts\utils\verify_all_intraday_1m.py --year-min 2019 --year-max 2025 --workers 16

# With CSV output for analysis
python scripts\utils\verify_all_intraday_1m.py `
  --year-min 2019 `
  --year-max 2025 `
  --workers 16 `
  --output-csv verification_2019_2025.csv

# Show only incomplete tickers
python scripts\utils\verify_all_intraday_1m.py `
  --year-min 2019 `
  --year-max 2025 `
  --show-only incomplete

# Verify 2004-2018 range
python scripts\utils\verify_all_intraday_1m.py `
  --year-min 2004 `
  --year-max 2018 `
  --data-dir "C:\TSIS_Data\ohlcv_intraday_1m\2004_2018" `
  --daily-root "raw/polygon/ohlcv_daily/2004_2018" `
  --universe-file "processed/universe/ping_range_2004_2018.parquet" `
  --workers 16
```


**show_intraday_1m_days.py**

El script lee los archivos parquet de intraday, extrae las fechas únicas, y las compara con los datos daily para mostrarte exactamente qué días has completado y cuáles faltan. ¡Pruébalo con BMGL y me dices cómo funciona!

```sh
# Ver todo el año 2025 para BMGL
python scripts\utils\show_intraday_1m_days.py BMGL --year 2025
# Ver solo noviembre 2025
python scripts\utils\show_intraday_1m_days.py BMGL --year 2025 --month 11
# Especificar directorio personalizado
python scripts\utils\show_intraday_1m_days.py BMGL --year 2025 --data-dir "C:\TSIS_Data\ohlcv_intraday_1m\2019_2025"
# Para datos 2004-2018
python scripts\utils\show_intraday_1m_days.py AAPL --year 2015 --data-dir "C:\TSIS_Data\ohlcv_intraday_1m\2004_2018"
```

```SH
(.venv-smallcap) PS D:\TSIS_SmallCaps> python scripts\utils\show_intraday_1m_days.py AUTO --year 2022
Escaneando C:\TSIS_Data\ohlcv_intraday_1m\2019_2025\AUTO...

======================================================================
VERIFICACIÓN DE DESCARGA INTRADAY 1M
======================================================================
Ticker      : AUTO
Año         : 2022
Directorio  : C:\TSIS_Data\ohlcv_intraday_1m\2019_2025\AUTO

Progreso    : 167/167 días (100.0%)

----------------------------------------------------------------------
DÍAS DESCARGADOS POR MES:
----------------------------------------------------------------------
2022-01    3,4,5,6,7,10,11,12,13,14,18,19,20,21,24,25,26,27,28,31
2022-02    1,2,3,4,7,8,9,10,11,14,15,16,17,18,22,23,24,25,28
2022-03    1,2,3,4,7,8,9,10,11,14,15,16,17,18,21,22,23,24,25,28,29,30,31
2022-04    1,4,5,6,7,8,11,12,13,14,18,19,20,21,22,25,26,27,28,29
2022-05    2,3,4,5,6,9,10,11,12,13,16,17,18,19,20,23,24,25,26,27,31
2022-06    1,2,3,6,7,8,9,10,13,14,15,16,17,21,22,23,24,27,28,29,30
2022-07    1,5,6,7,8,11,12,13,14,15,18,19,20,21,22,25,26,27,28,29
2022-08    1,2,3,4,5,8,9,10,11,12,15,16,17,18,19,22,23,24,25,26,29,30,31

======================================================================
✓ DESCARGA COMPLETA
======================================================================
```