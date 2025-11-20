# Pipeline de Universo Small Caps 2004-2018

1. [Filtro Temporal 2004-2018 (24,883 tickers)](#filtro-temporal-2004-2018)
2. [Filtro CS + XNAS/XNYS (7,070 tickers)](#filtrado-por-cs-stocks--xnasxnys-exchange)
3. [Enriquecimiento Dual (7,070 tickers + 26 columnas)](#universo-hibrido-enriquecido)
4. [Filtro Small Caps < $2B (5,168 tickers - Poblacion Target)](#filtro-small-caps-target-population--2b-con-anti-survivorship-bias)
5. [Ping Test de Disponibilidad (2,249 tickers con datos confirmados)](#ping-test-de-disponibilidad-en-polygon-api)

---

## Route Map - Pipeline Completo

```bash
================================================================================
                 PIPELINE DE CONSTRUCCION DE UNIVERSO 2004-2018
================================================================================

PASO 0: UNIVERSO COMPLETO (Compartido con 2019-2025 y 2007-2018)
--------------------------------------------------------------------------------
Input: raw/polygon/reference/tickers_snapshot/snapshot_date=2025-11-01/

        UNIVERSO COMPLETO: 34,324 tickers
        |- Activos: 11,899 (34.7%)
        +- Inactivos: 22,425 (65.3%)


                              |  FILTRO TEMPORAL


PASO 1: FILTRO TEMPORAL 2004-2018
--------------------------------------------------------------------------------
Script: filter_universe_2007_2018.py (reutilizado con params 2004-2018)
Criterio: Activos HOY + Delistados entre 2004-01-01 y 2018-12-31
Output: processed/universe/tickers_2004_2018.parquet

        TEMPORAL 2004-2018: 24,883 tickers (-27.5%)
        |- +4,914 tickers vs 2007-2018 (tickers delistados 2004-2006)
        |- Incluye: list_date filtering (con ticker_details)
        +- Mejora precision vs solo delisted_date


                          |  FILTRO CS + EXCHANGES


PASO 2: FILTRO CS + XNAS/XNYS
--------------------------------------------------------------------------------
Script: filter_universe_cs_exchanges_2007_2018.py (reutilizado)
Criterio: type=CS + primary_exchange IN [XNAS, XNYS]
Output: processed/universe/tickers_cs_exchanges_2004_2018.parquet

        COMMON STOCKS NASDAQ/NYSE: 7,070 tickers (-71.6%)
        |- +2,101 tickers vs 2007-2018


                      |  ENRIQUECIMIENTO DUAL (ESTRATEGIA HIBRIDA)


PASO 3: ENRIQUECIMIENTO CON DATOS CORPORATIVOS
--------------------------------------------------------------------------------
Script: create_hybrid_enriched_universe.py (mismo que 2019-2025 y 2007-2018)
Input: tickers_cs_exchanges_2004_2018.parquet
Output: processed/universe/hybrid_enriched_2004_2018.parquet

        ENRIQUECIDO (26 columnas): 7,070 tickers
        |- ACTIVOS (5,007):
        |  |- market_cap: 4,883/5,007 (97.5%) <- Para filtrar Small Caps
        |  |- description: 4,934/5,007 (98.5%)
        |  +- employees: 4,509/5,007 (90.1%)
        |
        +- INACTIVOS (2,063):
           +- delisted_utc: 2,063/2,063 (100.0%) <- Para feature engineering ML


                        |  FILTRO SMALL CAPS < $2B


PASO 4: FILTRO SMALL CAPS (< $2B) - POBLACION TARGET
--------------------------------------------------------------------------------
Script: filter_smallcaps_population.py (mismo que 2019-2025 y 2007-2018)
Criterio DUAL:
  - ACTIVOS: market_cap < $2,000,000,000
  - INACTIVOS: SIN FILTRO (preservar TODOS para anti-survivorship bias)
Output: processed/universe/smallcaps_universe_2004_2018.parquet

        SMALL CAPS TARGET: 5,168 tickers (-26.9%)
        |- ACTIVOS (3,105 - 60.1%): <- FILTRADOS por market cap
        |  |- Micro caps (< $300M): 1,747 (56.3%)
        |  |- Small caps ($300M-$1B): 863 (27.8%)
        |  +- Mid-small caps ($1B-$2B): 495 (15.9%)
        |
        +- INACTIVOS (2,063 - 39.9%): <- TODOS preservados
           +- ANTI-SURVIVORSHIP BIAS APLICADO


                    |  PING TEST DE DISPONIBILIDAD


PASO 5: PING TEST EN POLYGON API (2004-2018)
--------------------------------------------------------------------------------
Script: ping_polygon_binary.py
Metodo: Busqueda binaria para detectar primer dia con datos
Input: smallcaps_universe_2004_2018.parquet (5,168 tickers)
Output: processed/universe/ping_binary_2004_2018.parquet

        TICKERS CON DATOS CONFIRMADOS: 2,249 tickers (43.5%)
        |- Tickers SIN datos en Polygon: 2,919 (56.5%)
        |- Tiempo de ejecucion: ~15 minutos (16 workers)
        +- Precision: 100% (busqueda binaria exhaustiva)


================================================================================
   RESULTADO FINAL - ANALISIS CRITICO
================================================================================

UNIVERSO TEORICO 2004-2018: 5,168 tickers Small Caps

DATOS CONFIRMADOS EN POLYGON: 2,249 tickers (43.5%)
   - Verificado mediante ping test exhaustivo
   - 2,919 tickers NO tienen datos disponibles (56.5%)
   - Comparacion con 2007-2018: 2,212 tickers (63.0%)
   - Tickers adicionales periodo 2004-2006: +92 tickers con datos

COMPARACION 2007-2018 vs 2004-2018:
   - Universo 2007-2018: 3,509 tickers -> 2,212 con datos (63.0%)
   - Universo 2004-2018: 5,168 tickers -> 2,249 con datos (43.5%)
   - Diferencia: +1,659 tickers adicionales (periodo 2004-2006)
   - De esos 1,659: solo 92 tienen datos (5.5%)
   - Conclusion: Periodo 2004-2006 tiene muy baja disponibilidad de datos

PROXIMO PASO RECOMENDADO:
   -> Descargar SOLO los 2,249 tickers con datos confirmados
   -> Evitar 2,919 requests innecesarios a tickers sin datos
   -> Ahorro estimado: 56.5% de tiempo y API requests

OPCIONES DE DESCARGA:
   A) Descargar 2,249 tickers confirmados (2004-2018) - RECOMENDADO
   B) Descargar 2,212 tickers confirmados (2007-2018) - Menos datos antiguos
   C) Comparar overlap entre ambos periodos para optimizar descarga

================================================================================
```

---

## Filtro Temporal 2004-2018

**Objetivo**: Filtrar tickers que estuvieron listados entre 2004 y 2018 desde el snapshot completo.

**Fuente de datos**: Snapshot completo de Polygon (34,324 tickers)

**Script**: [scripts/00_universe_ingest/filter_universe_2007_2018.py](../../scripts/00_universe_ingest/filter_universe_2007_2018.py) (reutilizado con parametros diferentes)

**Logica de filtrado**:
- **Activos**: Incluye activos HOY que podrian haber existido en 2004-2018
- **Inactivos**: Incluye SOLO los delistados entre 2004-01-01 y 2018-12-31
- **Mejora**: Usa `list_date` de ticker_details para filtrar activos listados despues de 2018

**Output**: [processed/universe/tickers_2004_2018.parquet](../../processed/universe/tickers_2004_2018.parquet)
- Total: 24,883 tickers (+4,914 vs 2007-2018)

**Comando de ejecucion**:
```bash
python scripts/00_universe_ingest/filter_universe_2007_2018.py \
  --input raw/polygon/reference/tickers_snapshot/snapshot_date=2025-11-01/tickers_all.parquet \
  --output processed/universe/tickers_2004_2018.parquet \
  --start-year 2004 \
  --end-year 2018
```

**Resultado**:
```
Total filtrado 2004-2018: 24,883
```

---

## Filtrado por CS (Stocks) + XNAS/XNYS (Exchange)

**Objetivo**: Filtrar unicamente Common Stocks (type=CS) listados en NASDAQ (XNAS) o NYSE (XNYS) del universo temporal 2004-2018.

**Script**: [scripts/00_universe_ingest/filter_universe_cs_exchanges_2007_2018.py](../../scripts/00_universe_ingest/filter_universe_cs_exchanges_2007_2018.py) (reutilizado)

**Input**: `processed/universe/tickers_2004_2018.parquet` (24,883 tickers)

**Filtros aplicados**:
- `type = "CS"` (Common Stock)
- `primary_exchange IN ["XNAS", "XNYS"]` (NASDAQ o NYSE)

**Output**: `processed/universe/tickers_cs_exchanges_2004_2018.parquet`

**Comando de ejecucion**:
```bash
python scripts/00_universe_ingest/filter_universe_cs_exchanges_2007_2018.py \
  --input processed/universe/tickers_2004_2018.parquet \
  --output processed/universe/tickers_cs_exchanges_2004_2018.parquet
```

**Resultado**:
```
CS+XNAS/XNYS 2004-2018: 7,070
```

---

## Universo Hibrido Enriquecido

**Objetivo**: Enriquecer el universo filtrado CS+NASDAQ/NYSE con datos corporativos mediante estrategia dual.

**Script**: [scripts/00_universe_ingest/create_hybrid_enriched_universe.py](../../scripts/00_universe_ingest/create_hybrid_enriched_universe.py)

**Input**:
- Base: `processed/universe/tickers_cs_exchanges_2004_2018.parquet` (7,070 tickers)
- Details: `raw/polygon/reference/ticker_details/as_of_date=2025-11-01/details.parquet`
- Snapshot: `raw/polygon/reference/tickers_snapshot/snapshot_date=2025-11-01/tickers_all.parquet`

**Output**: `processed/universe/hybrid_enriched_2004_2018.parquet`

**Comando de ejecucion**:
```bash
python scripts/00_universe_ingest/create_hybrid_enriched_universe.py \
  --base processed/universe/tickers_cs_exchanges_2004_2018.parquet \
  --details raw/polygon/reference/ticker_details/as_of_date=2025-11-01/details.parquet \
  --snapshot raw/polygon/reference/tickers_snapshot/snapshot_date=2025-11-01/tickers_all.parquet \
  --output processed/universe/hybrid_enriched_2004_2018.parquet
```

**Resultado**:
```
Total tickers enriquecidos: 7,070
   Activos: 5,007
   Inactivos: 2,063

Market Cap (activos): 4,883/5,007 (97.5%)
Delisted UTC (inactivos): 2,063/2,063 (100.0%)

Small caps (< $2B): 3,105 (63.6% de activos con market_cap)
```

---

## Filtro Small Caps Target Population (< $2B) con Anti-Survivorship Bias

**Objetivo**: Filtrar la poblacion target de Small Caps (market cap < $2B) preservando TODOS los tickers inactivos para eliminar survivorship bias.

**Script**: [scripts/00_universe_ingest/filter_smallcaps_population.py](../../scripts/00_universe_ingest/filter_smallcaps_population.py)

**Input**: `processed/universe/hybrid_enriched_2004_2018.parquet` (7,070 tickers)

**Estrategia dual de filtrado**:
- **ACTIVOS**: `market_cap < $2,000,000,000` -> 3,105 tickers
- **INACTIVOS**: **SIN FILTRO** (todos preservados) -> 2,063 tickers

**Output**: `processed/universe/smallcaps_universe_2004_2018.parquet`

**Comando de ejecucion**:
```bash
python scripts/00_universe_ingest/filter_smallcaps_population.py \
  --input processed/universe/hybrid_enriched_2004_2018.parquet \
  --output processed/universe/smallcaps_universe_2004_2018.parquet \
  --market-cap-threshold 2000000000
```

**Resultado**:
```
Total poblacion target: 5,168
   Small Caps activos: 3,105 (60.1%)
   Inactivos preservados: 2,063 (39.9%)
   Sin survivorship bias: SI

Distribucion por rangos:
   < $300M         1,747 ( 56.3%)
   $300M - $1B       863 ( 27.8%)
   $1B - $2B         495 ( 15.9%)

Distribucion por exchange:
   XNAS: 2,960 (57.3%)
   XNYS: 2,208 (42.7%)
```

---

## Ping Test de Disponibilidad en Polygon API

**Objetivo**: Verificar que tickers del universo Small Caps 2004-2018 tienen datos historicos disponibles en Polygon API.

**Metodo**: Busqueda binaria del primer dia con datos en el rango 2004-2018.

**Script**: [scripts/utils/ping_polygon_binary.py](../../scripts/utils/ping_polygon_binary.py)

**Input**: `processed/universe/smallcaps_universe_2004_2018.parquet` (5,168 tickers)

**Output**: `processed/universe/ping_binary_2004_2018.parquet`

**Comando de ejecucion**:
```bash
python scripts/utils/ping_polygon_binary.py \
  --tickers processed/universe/smallcaps_universe_2004_2018.parquet \
  --out processed/universe/ping_binary_2004_2018.parquet \
  --workers 16
```

**Resultado**:
```
Tickers cargados: 5,168
Workers: 16
[Processing 100% completed]

Total tickers: 5,168
Con datos 2004-2018: 2,249 (43.5%)
Sin datos: 2,919 (56.5%)
```

**Analisis critico**:

1. **Baja disponibilidad de datos**: Solo el 43.5% de los tickers teoricos tienen datos reales
2. **Comparacion con 2007-2018**:
   - Universo 2007-2018: 3,509 tickers -> 2,212 con datos (63.0%)
   - Universo 2004-2018: 5,168 tickers -> 2,249 con datos (43.5%)
3. **Periodo 2004-2006 tiene muy pocos datos**:
   - 1,659 tickers adicionales en periodo 2004-2006
   - De esos, solo 92 tienen datos (5.5%)
   - 1,567 tickers (94.5%) NO tienen datos disponibles
4. **Overlap entre periodos**:
   - 2,157 tickers tienen datos en AMBOS periodos (2004-2018 Y 2007-2018)
   - 92 tickers tienen datos SOLO en periodo 2004-2006
   - 55 tickers tienen datos SOLO en periodo 2007-2018

**Conclusiones**:

1. **NO descargar el universo completo de 5,168 tickers**
   - 56.5% son requests innecesarios a tickers sin datos

2. **Recomendacion**: Descargar solo los 2,249 tickers confirmados
   - Ahorro: 2,919 requests innecesarios
   - Reduccion: 56.5% de tiempo y API calls

3. **Periodo 2004-2006 aporta poco valor**:
   - Solo 92 tickers adicionales vs 2007-2018
   - Considerar si vale la pena los 3-4 anos extra de datos antiguos

---

## Archivos Generados

```
processed/universe/
├── tickers_2004_2018.parquet                    # 24,883 tickers (filtro temporal)
├── tickers_cs_exchanges_2004_2018.parquet       # 7,070 tickers (CS + NASDAQ/NYSE)
├── hybrid_enriched_2004_2018.parquet            # 7,070 tickers (enriquecido)
├── smallcaps_universe_2004_2018.parquet         # 5,168 tickers (Small Caps < $2B)
└── ping_binary_2004_2018.parquet                # 5,168 tickers (con flag has_data)
```

**Output final para descarga**:
```bash
# Generar CSV con solo tickers que tienen datos confirmados
python -c "
import polars as pl
df = pl.read_parquet('processed/universe/ping_binary_2004_2018.parquet')
confirmed = df.filter(pl.col('has_history_2004_2018') == True)
confirmed.select('ticker').write_csv('processed/universe/tickers_confirmed_2004_2018.csv')
print(f'Tickers confirmados: {len(confirmed):,}')
"
```

**Resultado**:
```
processed/universe/tickers_confirmed_2004_2018.csv  # 2,249 tickers para descargar
```

---

## Proximos Pasos

1. **Generar CSV de tickers confirmados** (2,249 tickers)
2. **Ejecutar descarga masiva** usando `batch_trades_wrapper.py`:
   ```bash
   python scripts/01_agregation_OHLCV/batch_trades_wrapper.py \
     --tickers processed/universe/tickers_confirmed_2004_2018.csv \
     --start 2004-01-01 \
     --end 2018-12-31 \
     --out-dir C:\TSIS_Data\trades_ticks_2004_2018 \
     --batch-size 60 \
     --max-concurrent 50
   ```
3. **Monitorear progreso** via logs en `D:\TSIS_SmallCaps\`
4. **Tiempo estimado**: ~30-45 dias para 2,249 tickers (vs 2-3 meses para 5,168)

---
