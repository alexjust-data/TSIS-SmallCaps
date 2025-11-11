# Pipeline de Ingesta Trades Tick-Level

1. [Descarga Trades 2004-2025 (6,405 tickers)](#descarga-trades-tick-level-2004-2025---6405-tickers)
2. [Auditoría de Descarga](#auditoria-de-descarga-en-progreso)
3. [Visualización del Pipeline](#route-map---pipeline-completo)

---

## Route Map - Pipeline Completo

```bash
════════════════════════════════════════════════════════════════════════════════
                    PIPELINE DE INGESTA TRADES TICK-LEVEL
════════════════════════════════════════════════════════════════════════════════

PASO 0: UNIVERSO SMALL CAPS (DESDE FASE A)
────────────────────────────────────────────────────────────────────────────────
Input: processed/universe/smallcaps_universe_2025-11-01.parquet

        🎯 UNIVERSO TARGET: 6,405 tickers
        ├─ Activos (< $2B): 3,105 (48.5%)
        └─ Inactivos preservados: 3,300 (51.5%)
           → ✅ ANTI-SURVIVORSHIP BIAS APLICADO


                          ↓  DESCARGA HISTÓRICA


PASO 1: DESCARGA INICIAL 2019-2025 (COMPLETADA)
────────────────────────────────────────────────────────────────────────────────
Script: batch_trades_wrapper.py + ingest_trades_ticks.py
Período: 2019-01-01 → 2025-11-01 (7 años)
Output: C:\TSIS_Data\trades_ticks_2019_2025\

        📊 DESCARGA 2019-2025: COMPLETADA (2025-11-09)
        ├─ Tickers descargados: 6,274/6,405 (97.95%)
        ├─ Tiempo total: ~36 horas continuas
        ├─ Velocidad promedio: 300-350 tickers/hora
        ├─ Tamaño: 100.41 GB (comprimido ZSTD level 1)
        └─ Errores HTTP 429: 0

        Configuración usada:
        ├─ Batch size: 60 tickers
        ├─ Max concurrent: 50 batches
        ├─ Rate limit: 0.05s (600 req/s teórico)
        └─ Con --resume (idempotente)


                          ↓  PROBLEMA DETECTADO


PASO 2: AUDITORÍA DE --resume (2025-11-10)
────────────────────────────────────────────────────────────────────────────────
Script: audit_download_complete.py
Hallazgo: --resume SALTA tickers con datos existentes

        ⚠️  PROBLEMA IDENTIFICADO:
        ├─ --resume verifica: ¿Tiene ALGÚN parquet?
        │   └─ SI → SALTA ticker completamente
        │   └─ NO → Descarga 2004-2025
        │
        ├─ Consecuencia:
        │   ├─ 6,274 tickers con datos 2019-2025
        │   └─ NO descargarían años 2004-2018
        │
        └─ Solución: Lanzar SIN --resume para 2004-2018


                          ↓  CORRECCIÓN APLICADA


PASO 3: DESCARGA EXPANDIDA 2004-2018 (EN PROGRESO)
────────────────────────────────────────────────────────────────────────────────
Inicio: 2025-11-11 08:38:49
Proceso ID: 126815
Script: batch_trades_wrapper.py (SIN --resume)
Período: 2004-01-01 → 2018-12-31 (15 años)
Output: C:\TSIS_Data\trades_ticks_2019_2025\ (misma carpeta)

        🔄 DESCARGA 2004-2018: EN PROGRESO
        ├─ Tickers procesando: 6,405 (TODOS)
        ├─ Batches totales: 107 batches × 60 tickers
        ├─ Concurrencia: 50 batches simultáneos
        ├─ Merge automático: ✅ (sin duplicados)
        └─ Tiempo estimado: ~18-22 horas

        Configuración actual:
        ├─ Batch size: 60 tickers
        ├─ Max concurrent: 50 batches
        ├─ Rate limit: 0.05s (600 req/s teórico)
        └─ SIN --resume (procesa todos los tickers)

        Merge automático verificado:
        ├─ Función: write_trades_by_day() (líneas 223-271)
        ├─ Lógica: Si archivo existe:
        │   ├─ Lee archivo existente
        │   ├─ Concatena nuevos datos
        │   ├─ Elimina duplicados por timestamp
        │   ├─ Mantiene último valor (keep="last")
        │   └─ Sobrescribe archivo
        └─ ✅ IDEMPOTENTE: Puede interrumpirse y relanzarse


                          ↓  RESULTADO ESPERADO


PASO 4: RESULTADO FINAL (ETA: 2025-11-12)
────────────────────────────────────────────────────────────────────────────────
Output: C:\TSIS_Data\trades_ticks_2019_2025\

        🎉 COBERTURA COMPLETA: 6,405 tickers × 22 años
        ├─ Años 2004-2018: 15 años históricos
        ├─ Años 2019-2025: 7 años recientes
        ├─ Tamaño estimado: ~250-300 GB (comprimido)
        └─ Espacio usado: ~800 GB disponibles

        Distribución de datos:
        ├─ Trades totales: Miles de millones
        ├─ Período: 2004-01-01 → 2025-11-01
        ├─ Archivos/ticker: ~5,500 (22 años × 250 días)
        └─ Estructura: ticker/year=YYYY/month=MM/day=DD/[session].parquet


════════════════════════════════════════════════════════════════════════════════
✅ DESCARGA EN PROGRESO - ETA 18-22 HORAS
════════════════════════════════════════════════════════════════════════════════

RESULTADO ESPERADO: 6,405 tickers con cobertura 2004-2025
├─ Período completo: 22 años (2004-2025)
├─ Tamaño total: ~250-300 GB comprimidos
├─ Merge automático: Sin duplicados
└─ Listo para: Análisis de microestructura 2004-2025

READY FOR: Feature Engineering & ML Training
════════════════════════════════════════════════════════════════════════════════
```

## Descarga Trades Tick-Level 2004-2025 - 6,405 tickers

* **Objetivo**: Descargar trades tick-level históricos (2004-2025) para 6,405 tickers Small Caps. Este proceso descarga trades individuales con separación premarket/market para análisis de microestructura y liquidez.
* **Fuente de datos**: Polygon API `/v3/trades/{ticker}` (timestamp-based)
* **Script principal**: [scripts/01_agregation_OHLCV/ingest_trades_ticks.py](../../scripts/01_agregation_OHLCV/ingest_trades_ticks.py)
* **Wrapper**: [scripts/01_agregation_OHLCV/batch_trades_wrapper.py](../../scripts/01_agregation_OHLCV/batch_trades_wrapper.py)
* **Output**: [C:\TSIS_Data\trades_ticks_2019_2025\](C:\TSIS_Data\trades_ticks_2019_2025\)

```bash
D:\TSIS_SmallCaps\
├── scripts/
│   └── 01_agregation_OHLCV/
│       ├── ingest_trades_ticks.py          # Core: Descarga diaria + separación premarket/market
│       └── batch_trades_wrapper.py         # Wrapper: Micro-batches + paralelismo
│
├── processed/universe/
│   └── smallcaps_universe_2025-11-01.parquet   # 6,405 tickers Small Caps
│
└── C:\TSIS_Data\
    └── trades_ticks_2019_2025/             # OUTPUT FINAL
        ├── _batch_temp/                    # Logs temporales de batches
        │   ├── batch_0000.csv              # Lista de tickers por batch
        │   ├── batch_0000.log              # Log de progreso
        │   └── batch_XXXX.log              # 107 batches totales
        │
        └── {TICKER}/                       # 6,405 tickers
            └── year={YYYY}/                # 22 años (2004-2025)
                └── month={MM}/             # 12 meses
                    └── day={YYYY-MM-DD}/   # ~250 días/año
                        ├── premarket.parquet    # 04:00-09:30 ET
                        └── market.parquet       # 09:30-16:00 ET
```

**Comando de ejecución (ACTUAL - EN PROGRESO):**

```bash
cd "D:\TSIS_SmallCaps" && python scripts/01_agregation_OHLCV/batch_trades_wrapper.py \
    --tickers-csv processed/universe/smallcaps_universe_2025-11-01.parquet \
    --outdir "C:\TSIS_Data\trades_ticks_2019_2025" \
    --from 2004-01-01 \
    --to 2018-12-31 \
    --batch-size 60 \
    --max-concurrent 50 \
    --rate-limit 0.05 \
    --ingest-script scripts/01_agregation_OHLCV/ingest_trades_ticks.py
    # SIN --resume para descargar años faltantes
```

```sh
📊 1. Estructura de datos descargados
----------------------------------------------------------------------------------------------------
Ejemplo ticker GGL con datos 2006-2008:

C:\TSIS_Data\trades_ticks_2019_2025\GGL\
├── year=2006\
│   └── month=04\
│       └── day=2006-04-06\
│           └── market.parquet              # 153,238 trades (09:30-16:00 ET)
│   └── month=05\
│       └── day=2006-05-01\
│           ├── premarket.parquet           # Trades 04:00-09:30 ET
│           └── market.parquet              # Trades 09:30-16:00 ET
├── year=2007\
└── year=2008\

Columnas en cada parquet:
┌────────┬───────────┬─────────────────────┬───────┬──────┬──────────┬─────────────┐
│ ticker ┆ date      ┆ timestamp           ┆ price ┆ size ┆ exchange ┆ conditions  │
├────────┼───────────┼─────────────────────┼───────┼──────┼──────────┼─────────────┤
│ GGL    ┆2006-04-06 ┆1144318200000000000 ┆ 15.20 ┆ 100  ┆ Q        ┆ [@, T, I]   │
│ GGL    ┆2006-04-06 ┆1144318201234567890 ┆ 15.21 ┆ 200  ┆ Q        ┆ [@, T]      │
└────────┴───────────┴─────────────────────┴───────┴──────┴──────────┴─────────────┘

📊 2. Configuración técnica (ingest_trades_ticks.py)
----------------------------------------------------------------------------------------------------
BASE_URL = "https://api.polygon.io"
PAGE_LIMIT = 50000                    # Trades por página
TIMEOUT = 45                          # Segundos
RETRY_MAX = 8                         # Reintentos
BACKOFF = 1.6                         # Factor exponencial
COMPRESSION = "zstd"                  # Compresión
COMPRESSION_LEVEL = 1                 # Nivel (1-22)

Horarios de mercado (ET):
  PREMARKET:  04:00 - 09:30 ET  → premarket.parquet
  MARKET:     09:30 - 16:00 ET  → market.parquet

📊 3. Configuración técnica (batch_trades_wrapper.py)
----------------------------------------------------------------------------------------------------
BATCH_SIZE = 60                       # Tickers por batch
MAX_CONCURRENT = 50                   # Batches simultáneos
RATE_LIMIT = 0.05                     # Segundos entre requests (600 req/s)
```

## Auditoría de Descarga (EN PROGRESO)

* **Objetivo**: Verificar integridad de descarga, detectar problemas con --resume, y monitorear velocidad de descarga en tiempo real.
* **Script**: [scripts/audit_download_complete.py](../../scripts/audit_download_complete.py)
* **Uso**: Ejecutar periódicamente durante descarga larga para verificar progreso y detectar bloqueos.

**Comando de ejecución:**

```bash
python D:\TSIS_SmallCaps\scripts\audit_download_complete.py
```

**Verificaciones realizadas:**

```sh
====================================================================================================
AUDITORÍA - VERIFICACIONES AUTOMÁTICAS
====================================================================================================

1. Estado del proceso
   - ¿Está corriendo?
   - ¿Cuándo fue última actualización de logs?
   - ¿Qué batches están activos?

2. Funcionamiento de --resume
   - ¿Cómo decide qué tickers saltar?
   - ¿Verifica años específicos o solo existencia de datos?
   - Impacto en descarga expandida 2004-2025

3. Velocidad de descarga
   - MB/minuto escribiéndose a disco
   - API requests/minuto a Polygon
   - Trades/minuto descargados

4. Años descargados por ticker
   - Sample de tickers recientemente modificados
   - ¿Qué rango temporal tiene cada ticker?
   - ¿Faltan años 2004-2018 en tickers existentes?

5. Errores en logs
   - HTTP 429 (rate limit)
   - HTTP 500/503 (servidor)
   - Excepciones Python
```

**Ejemplo de output:**

```sh
================================================================================
AUDITORÍA COMPLETA - DESCARGA TRADES TICK-LEVEL 2004-2025
================================================================================

1. ESTADO DEL PROCESO
--------------------------------------------------------------------------------
OK PROCESO ACTIVO - Logs modificados recientemente:
   Batch 0000: ultima actualizacion hace 37s (08:31:47)
   Batch 0001: ultima actualizacion hace 9s (08:32:16)

2. FUNCIONAMIENTO DE --resume
--------------------------------------------------------------------------------
LOGICA: Si un ticker tiene CUALQUIER archivo parquet (de cualquier anio),
        entonces --resume lo marca como 'completado' y lo SALTA.

CONSECUENCIA:
  [OK] Tickers con datos 2019-2025 -> NO se re-descargan esos anios
  [OK] Tickers con datos 2019-2025 -> NO se descargan anios 2004-2018
  [OK] Solo descarga los 131 tickers sin ningun dato previo

SOLUCION ACTUAL:
  Lanzar SIN --resume para descargar años 2004-2018 en todos los tickers
  Merge automático evita duplicados en años 2019-2025

3. VELOCIDAD DE DESCARGA (ÚLTIMOS 5 MINUTOS)
--------------------------------------------------------------------------------
Archivos creados/modificados: 45
Datos escritos: 12.34 MB
Velocidad: 2.47 MB/minuto (148.2 MB/hora)

Ultimos 10 archivos:
  08:32:15 |   245.3 KB | GGL/year=2007/month=03/day=2007-03-15/market.parquet
  08:32:10 |   198.7 KB | GGL/year=2007/month=03/day=2007-03-14/market.parquet
  08:32:05 |   312.1 KB | GGL/year=2007/month=03/day=2007-03-13/market.parquet

4. REQUESTS A POLYGON API (ÚLTIMOS 5 MINUTOS)
--------------------------------------------------------------------------------
Actualizaciones de progreso: 42 (ultimos 5 minutos)
Velocidad estimada: ~8.4 actualizaciones/minuto

Por batch:
  Batch 0000: 22 actualizaciones
  Batch 0001: 20 actualizaciones

ALERTA  Cada actualizacion = ~200 API requests a Polygon
   Total estimado: ~8,400 requests en 5 minutos
   Rate: ~28.0 requests/segundo

5. COBERTURA DE AÑOS POR TICKER
--------------------------------------------------------------------------------
Tickers modificados en últimos 5 minutos:
  GGL        |  3 años (2006-2008) | 08:17:29
  SMTK       |  7 años (2019-2025) | 07:24:47
  STBZ       |  7 años (2019-2025) | 05:55:58

6. VERIFICACIÓN --resume CON TICKER EXISTENTE
--------------------------------------------------------------------------------
Ticker: AAPL
Años existentes: 2019, 2020, 2021, 2022, 2023, 2024, 2025
Total años: 7

Desglose:
  2004-2018: 0 años → NINGUNO
  2019-2025: 7 años → ['2019', '2020', '2021', '2022', '2023', '2024', '2025']

ALERTA  CONFIRMADO: AAPL tiene 2019-2025 pero NO 2004-2018
   --resume lo SALTO porque ya tiene datos

7. ERRORES EN LOGS (ÚLTIMAS 100 LÍNEAS)
--------------------------------------------------------------------------------
OK No se encontraron errores en logs recientes

================================================================================
RESUMEN DE AUDITORÍA
================================================================================

[OK] PROCESO: ACTIVO
[OK] Velocidad: 2.47 MB/minuto (normal para años históricos)
[OK] API Requests: ~8,400 en ultimos 5 min
[OK] Sin errores HTTP 429 o 500
[OK] Merge automático funcionando (sin duplicados)

================================================================================
```

## Histórico de Comandos Ejecutados

```bash
# COMANDO 1: Descarga inicial 2019-2025 con --resume (COMPLETADA 2025-11-09)
────────────────────────────────────────────────────────────────────────────────
cd "D:\TSIS_SmallCaps" && python scripts/01_agregation_OHLCV/batch_trades_wrapper.py \
    --tickers-csv processed/universe/smallcaps_universe_2025-11-01.parquet \
    --outdir "C:\TSIS_Data\trades_ticks_2019_2025" \
    --from 2019-01-01 \
    --to 2025-11-01 \
    --batch-size 60 \
    --max-concurrent 50 \
    --rate-limit 0.05 \
    --ingest-script scripts/01_agregation_OHLCV/ingest_trades_ticks.py \
    --resume

Resultado:
  ✅ 6,274/6,405 tickers descargados (97.95%)
  ✅ 100.41 GB comprimidos (ZSTD level 1)
  ✅ ~36 horas tiempo total
  ✅ 300-350 tickers/hora velocidad promedio
  ✅ 0 errores HTTP 429


# COMANDO 2: Intento expansión 2004-2025 con --resume (MATADO 2025-11-10)
────────────────────────────────────────────────────────────────────────────────
cd "D:\TSIS_SmallCaps" && python scripts/01_agregation_OHLCV/batch_trades_wrapper.py \
    --tickers-csv processed/universe/smallcaps_universe_2025-11-01.parquet \
    --outdir "C:\TSIS_Data\trades_ticks_2019_2025" \
    --from 2004-01-01 \
    --to 2025-11-01 \
    --batch-size 60 \
    --max-concurrent 50 \
    --rate-limit 0.05 \
    --ingest-script scripts/01_agregation_OHLCV/ingest_trades_ticks.py \
    --resume

Proceso ID: 06d021
Tiempo ejecutado: 11.7 horas
Motivo de terminación: PROBLEMA DETECTADO

Problema:
  ⚠️  --resume saltó 6,274 tickers con datos 2019-2025
  ⚠️  Solo procesó 131 tickers sin datos previos
  ⚠️  Velocidad: 14 tickers/hora (21.4x más lento, warrants sin trades)

Acción tomada:
  🔴 Proceso matado a las 08:38:49 (2025-11-11)


# COMANDO 3: Descarga 2004-2018 SIN --resume (EN PROGRESO 2025-11-11)
────────────────────────────────────────────────────────────────────────────────
cd "D:\TSIS_SmallCaps" && python scripts/01_agregation_OHLCV/batch_trades_wrapper.py \
    --tickers-csv processed/universe/smallcaps_universe_2025-11-01.parquet \
    --outdir "C:\TSIS_Data\trades_ticks_2019_2025" \
    --from 2004-01-01 \
    --to 2018-12-31 \
    --batch-size 60 \
    --max-concurrent 50 \
    --rate-limit 0.05 \
    --ingest-script scripts/01_agregation_OHLCV/ingest_trades_ticks.py
    # SIN --resume

Proceso ID: 126815
Inicio: 2025-11-11 08:38:49
Estado: ACTIVO

Estrategia:
  ✅ Descargar SOLO años 2004-2018
  ✅ Para TODOS los 6,405 tickers
  ✅ Merge automático evita duplicados en 2019-2025
  ✅ ETA: ~18-22 horas
```

## Resumen de Métricas

```sh
====================================================================================================
📊 FASE 1: DESCARGA 2019-2025 (COMPLETADA)
====================================================================================================
Inicio:                   2025-11-08 ~12:00
Fin:                      2025-11-09 ~00:00
Tiempo total:             ~36 horas continuas
Velocidad promedio:       300-350 tickers/hora
Tickers procesados:       6,274/6,405 (97.95%)
Errores HTTP 429:         0 (rate limit perfecto)
Tamaño descargado:        100.41 GB (comprimido ZSTD level 1)

Configuración:
  - Período: 2019-01-01 → 2025-11-01 (7 años)
  - Batch size: 60 tickers
  - Max concurrent: 50 batches
  - Rate limit: 0.05s (600 req/s teórico)
  - Con --resume: ✅

Completitud:
  - Tickers completos: 2,760 (43.09%)
  - Tickers parciales: 3,514 (56.04%)
  - Sin datos: 131 (2.05%)


====================================================================================================
📊 FASE 2: DESCARGA 2004-2018 (EN PROGRESO)
====================================================================================================
Inicio:                   2025-11-11 08:38:49
Proceso ID:               126815
Estado:                   ACTIVO
Tickers procesando:       6,405 (TODOS)
Batches totales:          107 batches × 60 tickers
Tiempo transcurrido:      ~1 hora
Tiempo estimado restante: ~18-22 horas

Configuración:
  - Período: 2004-01-01 → 2018-12-31 (15 años)
  - Batch size: 60 tickers
  - Max concurrent: 50 batches
  - Rate limit: 0.05s (600 req/s teórico)
  - Sin --resume: ✅

Merge automático:
  - Función: write_trades_by_day() (ingest_trades_ticks.py:223-271)
  - Lógica: Concatena + unique(subset=["timestamp"], keep="last")
  - Resultado: Sin duplicados en años 2019-2025 existentes


====================================================================================================
📊 RESULTADO FINAL ESPERADO (ETA: 2025-11-12)
====================================================================================================
Años 2004-2018:           ~150-200 GB (estimado)
Años 2019-2025:           100.41 GB (ya descargado)
TOTAL ESPERADO:           ~250-300 GB (22 años completos)
Espacio disponible:       800 GB (suficiente con margen)

Trades estimados:
  - Total trades: Miles de millones
  - Promedio/ticker-año: ~5-10 millones de trades
  - Variabilidad: Alta (small caps tienen menos volumen)

Archivos por ticker:
  - Total archivos: ~5,500 (22 años × 250 días trading)
  - Premarket.parquet: ~2,750 archivos
  - Market.parquet: ~2,750 archivos

Promedio por ticker:
  - Tamaño: ~40-50 MB (22 años comprimidos)
  - Trades: ~100-200 millones (lifetime)
```

## Monitoreo en Tiempo Real

```bash
# Ver progreso del wrapper principal
tail -f C:/TSIS_Data/trades_ticks_2019_2025/_batch_temp/batch_0000.log

# Ver batches activos recientemente
ls -lah C:/TSIS_Data/trades_ticks_2019_2025/_batch_temp/ | grep "nov. 11"

# Contar archivos descargados por ticker
find C:/TSIS_Data/trades_ticks_2019_2025/GGL -name "*.parquet" | wc -l

# Ver últimos archivos creados (últimos 5 minutos)
find C:/TSIS_Data/trades_ticks_2019_2025 -name "*.parquet" -mmin -5 | head -20

# Ejecutar auditoría completa
python D:\TSIS_SmallCaps\scripts\audit_download_complete.py
```

---
