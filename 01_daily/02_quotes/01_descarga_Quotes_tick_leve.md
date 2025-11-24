
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
>>   --output-csv quotes_dates_2019_2025.csv `
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
PS D:\TSIS_SmallCaps> python scripts\01_agregation_OHLCV\generate_quotes_dates.py `
>>   --daily-root raw/polygon/ohlcv_daily `
>>   --ping-range processed/universe/ping_range_2004_2018.parquet `
>>   --output-csv quotes_dates_2004_2018.csv `
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