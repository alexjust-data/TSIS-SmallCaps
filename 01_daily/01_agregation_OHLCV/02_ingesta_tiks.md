```sh
PS D:\TSIS_SmallCaps> # Mejor configuración para 169 tickers
PS D:\TSIS_SmallCaps> python scripts\01_agregation_OHLCV\batch_trades_wrapper.py `
>>     --tickers-csv audit_2004_2018_missing.csv `
>>     --outdir C:/TSIS_Data/trades_ticks_2004_2018_v2 `
>>     --from 2004-01-01 `
>>     --to 2018-12-31 `
>>     --batch-size 10 `
>>     --max-concurrent 17 `
>>     --rate-limit 0.05 `
>>     --ingest-script scripts\01_agregation_OHLCV\ingest_trades_ticks.py `
>>     --resume
[2025-11-18 12:08:34] Detectado formato CSV: audit_2004_2018_missing.csv
[2025-11-18 12:08:35] Cargados 169 tickers únicos
[2025-11-18 12:08:37] --resume: 2,087 tickers ya con datos | pendientes: 163
[2025-11-18 12:08:37] == Config ==
[2025-11-18 12:08:37]   Universo pendiente: 163 tickers
[2025-11-18 12:08:37]   Batches: 17 x 10 tickers
[2025-11-18 12:08:37]   Concurrencia: 17 batches a la vez
[2025-11-18 12:08:37]   Ventana: 2004-01-01 -> 2018-12-31
[2025-11-18 12:08:37]   Ingestor: scripts\01_agregation_OHLCV\ingest_trades_ticks.py
[2025-11-18 12:08:37] Iniciando ThreadPoolExecutor con 17 workers...
[2025-11-18 12:08:37] Submitting 17 batches...
[2025-11-18 12:08:37] Batches submitted. Waiting for completion...
[2025-11-18 12:21:49] Batch 0004: failed(rc=1) (791.8s) | Progreso 1/17 = 5.9%
[2025-11-18 12:21:49]   -> Velocidad: 4.5 batches/h (45.5 tickers/h)
[2025-11-18 12:21:49]   -> Reciente: 4.5 batches/h (45.5 tickers/h)
[2025-11-18 12:21:49]   -> ETA: 3.5 horas (0.1 dias)
[2025-11-18 12:21:49]   -> Status: 0 OK, 1 errores
[2025-11-18 12:21:49]   -> Errores: failed(rc=1): 1
[2025-11-18 12:22:02] Batch 0013: failed(rc=1) (804.2s) | Progreso 2/17 = 11.8%
[2025-11-18 12:22:24] Batch 0008: failed(rc=1) (826.2s) | Progreso 3/17 = 17.6%
[2025-11-18 12:22:29] Batch 0011: failed(rc=1) (831.4s) | Progreso 4/17 = 23.5%
[2025-11-18 12:22:32] Batch 0006: failed(rc=1) (834.9s) | Progreso 5/17 = 29.4%
[2025-11-18 12:22:34] Batch 0003: failed(rc=1) (836.2s) | Progreso 6/17 = 35.3%
[2025-11-18 12:22:35] Batch 0010: failed(rc=1) (837.4s) | Progreso 7/17 = 41.2%
[2025-11-18 12:22:39] Batch 0014: failed(rc=1) (841.8s) | Progreso 8/17 = 47.1%
[2025-11-18 12:22:49] Batch 0016: failed(rc=1) (851.6s) | Progreso 9/17 = 52.9%
[2025-11-18 12:22:50] Batch 0009: failed(rc=1) (852.1s) | Progreso 10/17 = 58.8%
[2025-11-18 12:22:50]   -> Velocidad: 42.3 batches/h (422.5 tickers/h)
[2025-11-18 12:22:50]   -> Reciente: 537.4 batches/h (5373.9 tickers/h)
[2025-11-18 12:22:50]   -> ETA: 0.2 horas (0.0 dias)
[2025-11-18 12:22:50]   -> Status: 0 OK, 10 errores
[2025-11-18 12:22:50]   -> Errores: failed(rc=1): 10
[2025-11-18 12:22:55] Batch 0005: failed(rc=1) (857.4s) | Progreso 11/17 = 64.7%
[2025-11-18 12:23:03] Batch 0015: failed(rc=1) (866.0s) | Progreso 12/17 = 70.6%
[2025-11-18 12:23:10] Batch 0007: failed(rc=1) (872.6s) | Progreso 13/17 = 76.5%
```

```sh
PS D:\TSIS_SmallCaps> python scripts\01_agregation_OHLCV\batch_trades_wrapper.py `
>>     --tickers-csv audit_2004_2018_missing.csv `
>>     --outdir C:/TSIS_Data/trades_ticks_2004_2018_v2 `
>>     --from 2004-01-01 `
>>     --to 2018-12-31 `
>>     --batch-size 20 `
>>     --max-concurrent 8 `
>>     --rate-limit 0.1 `
>>     --ingest-script scripts\01_agregation_OHLCV\ingest_trades_ticks.py
[2025-11-18 12:24:44] Detectado formato CSV: audit_2004_2018_missing.csv
[2025-11-18 12:24:46] Cargados 169 tickers únicos
[2025-11-18 12:24:46] Pendientes: 169
[2025-11-18 12:24:46] == Config ==
[2025-11-18 12:24:46]   Universo pendiente: 169 tickers
[2025-11-18 12:24:46]   Batches: 9 x 20 tickers
[2025-11-18 12:24:46]   Concurrencia: 8 batches a la vez
[2025-11-18 12:24:46]   Ventana: 2004-01-01 -> 2018-12-31
[2025-11-18 12:24:46]   Ingestor: scripts\01_agregation_OHLCV\ingest_trades_ticks.py
[2025-11-18 12:24:46] Iniciando ThreadPoolExecutor con 8 workers...
[2025-11-18 12:24:46] Submitting 9 batches...
[2025-11-18 12:24:46] Batches submitted. Waiting for completion...
[2025-11-18 12:39:03] Batch 0004: failed(rc=1) (857.2s) | Progreso 1/9 = 11.1%
[2025-11-18 12:39:03]   -> Velocidad: 4.2 batches/h (84.0 tickers/h)
[2025-11-18 12:39:03]   -> Reciente: 4.2 batches/h (84.0 tickers/h)
[2025-11-18 12:39:03]   -> ETA: 1.9 horas (0.1 dias)
[2025-11-18 12:39:03]   -> Status: 0 OK, 1 errores
[2025-11-18 12:39:03]   -> Errores: failed(rc=1): 1
[2025-11-18 12:39:09] Batch 0001: failed(rc=1) (863.5s) | Progreso 2/9 = 22.2%
[2025-11-18 12:39:14] Batch 0007: failed(rc=1) (868.5s) | Progreso 3/9 = 33.3%
[2025-11-18 12:39:20] Batch 0006: failed(rc=1) (874.7s) | Progreso 4/9 = 44.4%
[2025-11-18 12:39:54] Batch 0000: failed(rc=1) (908.4s) | Progreso 5/9 = 55.6%
[2025-11-18 12:39:57] Batch 0003: failed(rc=1) (910.9s) | Progreso 6/9 = 66.7%
[2025-11-18 12:54:03] Batch 0008: failed(rc=1) (899.6s) | Progreso 7/9 = 77.8%
[2025-11-18 12:54:03]   -> Velocidad: 14.3 batches/h (286.9 tickers/h)
[2025-11-18 12:54:03]   -> Reciente: 24.0 batches/h (480.2 tickers/h)
[2025-11-18 12:54:03]   -> ETA: 0.1 horas (0.0 dias)
[2025-11-18 12:54:03]   -> Status: 0 OK, 7 errores
[2025-11-18 12:54:03]   -> Errores: failed(rc=1): 7
[2025-11-18 12:56:31] Batch 0002: failed(rc=1) (1905.2s) | Progreso 8/9 = 88.9%
[2025-11-18 12:56:31]   -> Velocidad: 15.1 batches/h (302.3 tickers/h)
[2025-11-18 12:56:31]   -> Reciente: 24.3 batches/h (485.2 tickers/h)
[2025-11-18 12:56:31]   -> ETA: 0.1 horas (0.0 dias)
[2025-11-18 12:56:31]   -> Status: 0 OK, 8 errores
[2025-11-18 12:56:31]   -> Errores: failed(rc=1): 8
[2025-11-18 12:56:43] Batch 0005: failed(rc=1) (1917.2s) | Progreso 9/9 = 100.0%
[2025-11-18 12:56:43]
============================================================
[2025-11-18 12:56:43] COMPLETADO: 0/9 batches OK | 9 fallidos
[2025-11-18 12:56:43] Tiempo total: 0.53 h
[2025-11-18 12:56:43] Logs por batch: C:\TSIS_Data\trades_ticks_2004_2018_v2\_batch_temp/
PS D:\TSIS_SmallCaps>
```