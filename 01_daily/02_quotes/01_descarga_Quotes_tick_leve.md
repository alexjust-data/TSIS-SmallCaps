
## **Script Optimizado:**

[ingest_quotes_ticks_async.py](01_agregation_OHLCV/ingest_quotes_ticks_async.py)

## **Mejoras Clave del Script Optimizado:**

1. **Descarga Asíncrona con `aiohttp`**
- Hasta 30-50 conexiones concurrentes (vs 10-12 threads)
- Connection pooling para reusar conexiones TCP
- DNS caching para reducir latencia

2. **Paginación Completa**
- Descarga TODAS las páginas de quotes (no solo 50K)
- Sigue `next_url` automáticamente
- Ideal para días con alto volumen

3. **Rate Limiting Inteligente**
- Usa header `X-Polygon-Retry-After` 
- Backoff exponencial con `@backoff`
- Semáforo para controlar concurrencia

4. **Optimización de I/O**
- Compresión `zstd` nivel 3 (mejor ratio velocidad/tamaño)
- Escritura de archivos en threads separados
- No bloquea las descargas

5. **Procesamiento en Batches**
- Procesa 1000 tareas por batch
- Mejor control de memoria
- Estadísticas por batch

## **Comandos de Ejecución Recomendados:**

```bash
# Para máxima velocidad con tu plan Advanced
python ingest_quotes_optimized.py \
  --dates-csv quotes_dates_2004_2018.csv \
  --outdir "C:\TSIS_Data\quotes_ticks_2004_2018" \
  --concurrent 40 \
  --batch-size 2000 \
  --skip-existing

# Para testing inicial
python ingest_quotes_optimized.py \
  --dates-csv quotes_dates_test.csv \
  --outdir "C:\TSIS_Data\quotes_test" \
  --concurrent 20 \
  --limit 100 \
  --skip-existing
```


## Ejecutando descarga

```sh
#Paso 1: Generar CSV de fechas
# 2004-2018
python scripts\01_agregation_OHLCV\generate_quotes_dates.py `
  --daily-root raw/polygon/ohlcv_daily `
  --ping-range processed/universe/ping_range_2004_2018.parquet `
  --output-csv quotes_dates_2004_2018.csv `
  --year-min 2004 --year-max 2018

# 2019-2025
python scripts\01_agregation_OHLCV\generate_quotes_dates.py `
  --daily-root raw/polygon/ohlcv_daily `
  --ping-range processed/universe/ping_range_2019_2025.parquet `
  --output-csv quotes_dates_2019_2025.csv `
  --year-min 2019 --year-max 2025


# Paso 2: Descargar quotes (versión async optimizada)
# 2004-2018 (máxima velocidad)
python scripts\01_agregation_OHLCV\ingest_quotes_ticks_async.py `
  --dates-csv quotes_dates_2004_2018.csv `
  --outdir "C:\TSIS_Data\quotes_ticks_2004_2018" `
  --concurrent 40 `
  --batch-size 2000 `
  --skip-existing

# 2019-2025
python scripts\01_agregation_OHLCV\ingest_quotes_ticks_async.py `
  --dates-csv quotes_dates_2019_2025.csv `
  --outdir "C:\TSIS_Data\quotes_ticks_2019_2025" `
  --concurrent 40 `
  --batch-size 2000 `
  --skip-existing


#Testing primero (recomendado)
python scripts\01_agregation_OHLCV\ingest_quotes_ticks_async.py `
  --dates-csv quotes_dates_2004_2018.csv `
  --outdir "C:\TSIS_Data\quotes_ticks_2004_2018" `
  --concurrent 20 `
  --limit 100

#Rendimiento esperado: ~25-40 req/s (3-5x más rápido que ThreadPoolExecutor)
```


**GENERADOR DE FECHAS PARA QUOTES : quotes_dates_2019_2025.csv** 

```sh
PS D:\TSIS_SmallCaps> # 2019-2025
>> python scripts\01_agregation_OHLCV\generate_quotes_dates.py `
>>   --daily-root raw/polygon/ohlcv_daily `
>>   --ping-range processed/universe/ping_range_2019_2025.parquet `
>>   --output-csv 01_daily\01_agregation_OHLCV\quotes_dates_2019_2025.csv `
>>   --year-min 2019 --year-max 2025
[2025-11-24 10:05:56] ================================================================================
[2025-11-24 10:05:56] GENERADOR DE FECHAS PARA QUOTES
[2025-11-24 10:05:56] ================================================================================
[2025-11-24 10:05:56] Cargando ping desde processed/universe/ping_range_2019_2025.parquet
[2025-11-24 10:05:56]   > 6,297 tickers con datos
[2025-11-24 10:05:56] Filtrando años: 2019 - 2025
[2025-11-24 10:05:56] Recolectando fechas de daily...
[2025-11-24 10:07:53]   Progreso: 500/6297 (7.9%)
[2025-11-24 10:09:55]   Progreso: 1000/6297 (15.9%)
[2025-11-24 10:11:56]   Progreso: 1500/6297 (23.8%)
[2025-11-24 10:14:00]   Progreso: 2000/6297 (31.8%)
[2025-11-24 10:16:18]   Progreso: 2500/6297 (39.7%)
[2025-11-24 10:18:42]   Progreso: 3000/6297 (47.6%)
[2025-11-24 10:21:01]   Progreso: 3500/6297 (55.6%)
[2025-11-24 10:23:19]   Progreso: 4000/6297 (63.5%)
[2025-11-24 10:25:32]   Progreso: 4500/6297 (71.5%)
[2025-11-24 10:28:00]   Progreso: 5000/6297 (79.4%)
[2025-11-24 10:29:58]   Progreso: 5500/6297 (87.3%)
[2025-11-24 10:32:11]   Progreso: 6000/6297 (95.3%)
[2025-11-24 10:33:22]   Progreso: 6297/6297 (100.0%)
[2025-11-24 10:33:22] 
[2025-11-24 10:33:22] Total fechas recolectadas: 5,526,516
[2025-11-24 10:33:23]   Tickers únicos: 6,297
[2025-11-24 10:33:26]   Guardado en: quotes_dates_2019_2025.csv
[2025-11-24 10:33:26] 
[2025-11-24 10:33:26] ================================================================================
[2025-11-24 10:33:26] COMPLETADO
[2025-11-24 10:33:26] ================================================================================
PS D:\TSIS_SmallCaps> 
```


```sh
PS D:\TSIS_SmallCaps> 
python scripts\01_agregation_OHLCV\generate_quotes_dates.py `
>>   --daily-root raw/polygon/ohlcv_daily `
>>   --ping-range processed/universe/ping_range_2004_2018.parquet `
>>   --output-csv 01_daily\01_agregation_OHLCV\files_csvquotes_dates_2004_2018.csv `
>>   --year-min 2004 --year-max 2018

[2025-11-24 10:33:58] ================================================================================
[2025-11-24 10:33:58] GENERADOR DE FECHAS PARA QUOTES
[2025-11-24 10:33:58] ================================================================================
[2025-11-24 10:33:58] Cargando ping desde processed/universe/ping_range_2004_2018.parquet
[2025-11-24 10:33:58]   > 3,477 tickers con datos
[2025-11-24 10:33:58] Filtrando años: 2004 - 2018
[2025-11-24 10:33:58] Recolectando fechas de daily...
[2025-11-24 10:37:05]   Progreso: 500/3477 (14.4%)
[2025-11-24 10:40:05]   Progreso: 1000/3477 (28.8%)
[2025-11-24 10:43:02]   Progreso: 1500/3477 (43.1%)
[2025-11-24 10:46:02]   Progreso: 2000/3477 (57.5%)
[2025-11-24 10:49:25]   Progreso: 2500/3477 (71.9%)
[2025-11-24 10:53:04]   Progreso: 3000/3477 (86.3%)
[2025-11-24 10:57:25]   Progreso: 3477/3477 (100.0%)
[2025-11-24 10:57:25] 
[2025-11-24 10:57:25] Total fechas recolectadas: 6,414,096
[2025-11-24 10:57:26]   Tickers únicos: 3,477
[2025-11-24 10:57:29]   Guardado en: quotes_dates_2004_2018.csv
[2025-11-24 10:57:29] 
[2025-11-24 10:57:29] ================================================================================
[2025-11-24 10:57:29] COMPLETADO
[2025-11-24 10:57:29] ================================================================================
```

**Estrategia por Fases Percentiles de volumen**

**DESCARGA**

**Para 2019-2025**

```sh
# 2019-2025 Estadísticas - GENERANDO CSV P95
python scripts\02_final\quotes_phased_strategy.py `
  --daily-root raw/polygon/ohlcv_daily `
  --ping-file processed/universe/ping_range_2019_2025.parquet `
  --year-min 2019 `
  --year-max 2025 `
  --generate-csv P95 `
  --output-dir 01_daily\01_agregation_OHLCV\files_csv `
  --download-dir  \TSIS_SmallCaps\01_daily\01_agregation_OHLCV\files_csv

Analizando 6297 tickers...
============================================================
DISTRIBUCIÓN GLOBAL DE VOLUMEN
============================================================
Total días analizados: 5,526,516
Percentil 50 (mediana): 100,351
Percentil 75: 414,458
Percentil 90: 1,209,222
Percentil 95: 2,274,650
Percentil 99: 8,349,477
Percentil 99.9: 41,419,593
============================================================
GENERANDO CSV PARA P95
============================================================
Umbral P95: 2,274,650
Filtrando días con volumen >= 2,274,650...
Generando: quotes_P95_2019_2025_P95.csv (276,326 días)
Archivo: quotes_P95_2019_2025_P95.csv
Total días: 276,326

# 2019-2025 INICIANDO DESCARGA QUOTES para  percentil 95 
PS D:\TSIS_SmallCaps> python scripts\02_final\download_quotes_ultra_fast.py `
>>   --csv 01_daily\02_quotes\files_csv\quotes_P95_2019_2025_P95.csv `
>>   --output "C:\TSIS_Data\quotes_p95_2019_2025" `
>>   --concurrent 100 `
>>   --resume
Cargando tareas desde 01_daily\02_quotes\files_csv\quotes_P95_2019_2025_P95.csv...
============================================================
DESCARGA ULTRA-RÁPIDA DE QUOTES
============================================================
Total tareas: 276,326
Por procesar: 276,326
Concurrencia: 100
Output: C:\TSIS_Data\quotes_p95_2019_2025
============================================================
[2025-11-25 20:17:03] Progress: 500/276326 (0.2%) | Rate: 2.2/s | ETA: 1.5d | Success: 500 | Errors: 0 | Skip: 0 | Quotes: 18.9M
[2025-11-26 09:11:49] Progress: 69000/276326 (25.0%) | Rate: 1.5/s | ETA: 1.6d | Success: 67308 | Errors: 1692 | Skip: 0 | Quotes: 3053.2M
...
[2025-11-28 06:02:47] Progress: 276326/276326 (100.0%) | Rate: 3.7/s | ETA: 0.0m | Success: 275431 | Errors: 895 | Skip: 3423 | Quotes: 2797.1M

============================================================
DESCARGA COMPLETADA
============================================================
Tiempo total: 1243.3 minutos (20.72 horas)
Total procesados: 276,326
Exitosos: 275,431
Errores: 895
Saltados (cache): 3,423
Días vacíos: 156
Total quotes: 2,797,122,950
Total páginas: 94,530
Velocidad promedio: 3.7 tareas/segundo
Quotes/segundo: 37496

# 2019-2025 VERIFICANDO
# SE HA ITERADO DOS VECES HASTA 100% Completados
python scripts\02_final\retry_failed_quotes.py 
  --original-csv "01_daily\02_quotes\files_csv\quotes_P95_2019_2025_P95.csv" --output-dir "C:\TSIS_Data\quotes_p95_2019_2025" 
  --retry-csv quotes_retry_2019_2025.csv
======================================================================
IDENTIFICANDO DESCARGAS FALLIDAS
======================================================================
Cargando CSV original: 01_daily\02_quotes\files_csv\quotes_P95_2019_2025_P95.csv
  Total tareas en CSV: 276,326

  Completados: 276,122 (99.9%)
  Faltantes: 204 (0.1%)

📝 CSV de retry guardado: quotes_retry_2019_2025_v2.csv
   Total tareas a reintentar: 204
======================================================================
COMANDO PARA REINTENTAR DESCARGAS FALLIDAS:
======================================================================
python scripts\02_final\download_quotes_ultra_fast.py \
  --csv quotes_retry_2019_2025.csv \
  --output "C:\TSIS_Data\quotes_p95_2019_2025" \
  --concurrent 50 \
  --resume
```

**Para 2004-2018:**

```sh
# 2004-2018 Estadisticas 
# GENERANDO CSV P95
cd D:\TSIS_SmallCaps
python "scripts\02_final\quotes_phased_strategy.py" `
  --daily-root "raw/polygon/ohlcv_daily" `
  --ping-file "processed/universe/ping_range_2004_2018.parquet" `
  --year-min 2004 `
  --year-max 2018 `
  --generate-csv P95

Analizando 3477 tickers...
============================================================
DISTRIBUCIÓN GLOBAL DE VOLUMEN
============================================================
Total días analizados: 6,414,035
Percentil 50 (mediana): 89,300
Percentil 75: 370,000
Percentil 90: 1,142,700
Percentil 95: 2,185,813
Percentil 99: 6,525,473
Percentil 99.9: 22,593,600
============================================================
GENERANDO CSV PARA P95
============================================================
Umbral P95: 2,185,813
Filtrando días con volumen >= 2,185,813...
Generado: quotes_P95_2004_2018_P95.csv (320,702 días)
Archivo: quotes_P95_2004_2018_P95.csv
Total días: 320,702


# 2004-2018 INICIANDO DESCARGA QUOTES para percentil 95 
PS D:\TSIS_SmallCaps> python scripts\02_final\download_quotes_ultra_fast.py `
>>   --csv 01_daily\02_quotes\files_csv\quotes_P95_2004_2018_P95.csv `
>>   --output "C:\TSIS_Data\quotes_p95_2004_2018" `
>>   --concurrent 30 `
>>   --resume
Cargando tareas desde 01_daily\02_quotes\files_csv\quotes_P95_2004_2018_P95.csv...
============================================================
DESCARGA ULTRA-RÁPIDA DE QUOTES
============================================================
Total tareas: 320,702
Por procesar: 320,702
Concurrencia: 30
Output: C:\TSIS_Data\quotes_p95_2004_2018
============================================================
[2025-11-27 13:51:36] Progress: 500/320702 (0.2%) | Rate: 4.6/s | ETA: 19.5h | Success: 500 | Errors: 0 | Skip: 0 | Quotes: 19.3M
...
...
[2025-11-28 22:34:14] Progress: 320702/320702 (100.0%) | Rate: 2.7/s | ETA: 0.0m | Success: 319869 | Errors: 833 | Skip: 0 | Quotes: 13280.7M

============================================================
DESCARGA COMPLETADA
============================================================
Tiempo total: 1964.5 minutos (32.74 horas)
Total procesados: 320,702
Exitosos: 319,869
Errores: 833
Saltados (cache): 0
Días vacíos: 15
Total quotes: 13,280,693,630
Total páginas: 450,068
Velocidad promedio: 2.7 tareas/segundo
Quotes/segundo: 112675

# 2004-2018 VERIFICANDO DESCARGA
# SE HA ITERADO DOS VECES HASTA 100% Completados
python scripts\02_final\retry_failed_quotes.py 
  --original-csv "01_daily\02_quotes\files_csv\quotes_P95_2004_2018_P95.csv" --output-dir "C:\TSIS_Data\quotes_p95_2004_2018" 
  --retry-csv quotes_retry_2004_2018.csv
======================================================================
IDENTIFICANDO DESCARGAS FALLIDAS
======================================================================
Cargando CSV original: 01_daily\02_quotes\files_csv\quotes_P95_2004_2018_P95.csv
  Total tareas en CSV: 320,702

  Completados: 320,627 (100.0%)
  Faltantes: 75 (0.0%)

📝 CSV de retry guardado: quotes_retry_2004_2018_v2.csv
   Total tareas a reintentar: 75
======================================================================
COMANDO PARA REINTENTAR DESCARGAS FALLIDAS:
======================================================================
python scripts\02_final\download_quotes_ultra_fast.py 
  --csv quotes_retry_2004_2018.csv 
  --output "C:\TSIS_Data\quotes_p95_2004_2018" 
  --concurrent 50
```


Para 2019-2025

```sh
# Primera ejecución
python scripts\02_final\download_quotes_fase3.py `
  --csv quotes_P90_2019_2025_P95.csv `
  --output "C:\TSIS_Data\quotes_p90_2019_2025" `
  --concurrent 50

# Si se interrumpe, retomar con:
python scripts\02_final\download_quotes_fase3.py `
  --csv quotes_P90_2019_2025_P95.csv `
  --output "C:\TSIS_Data\quotes_p90_2019_2025" `
  --concurrent 50 `
  --resume
```

Para 2004-2018:

```sh
# Primera ejecución
python scripts\02_final\download_quotes_fase3.py `
  --csv quotes_P90_2004_2018_P95.csv `
  --output "C:\TSIS_Data\quotes_p90_2004_2018" `
  --concurrent 50

# Si se interrumpe, retomar con:
python scripts\02_final\download_quotes_fase3.py `
  --csv quotes_P90_2004_2018_P95.csv `
  --output "C:\TSIS_Data\quotes_p90_2004_2018" `
  --concurrent 50 `
  --resume
```