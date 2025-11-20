# Pipeline de Universo Small Caps 2007-2018

1. [Filtro Temporal 2007-2018 (19,969 tickers)](#filtro-temporal-2007-2018)
2. [Filtro CS + XNAS/XNYS (4,969 tickers)](#filtrado-por-cs-stocks--xnasxnys-exchange)
3. [Enriquecimiento Dual (4,969 tickers + 26 columnas)](#universo-hibrido-enriquecido)
4. [Filtro Small Caps < $2B (3,509 tickers - Poblacion Target)](#filtro-small-caps-target-population--2b-con-anti-survivorship-bias)
5. [Analisis de Datos Descargados (175 tickers con datos, 55 Small Caps)](#analisis-de-datos-ya-descargados)

---

## Route Map - Pipeline Completo

```bash
================================================================================
                 PIPELINE DE CONSTRUCCION DE UNIVERSO 2007-2018
================================================================================

PASO 0: UNIVERSO COMPLETO (Compartido con 2019-2025)
--------------------------------------------------------------------------------
Input: raw/polygon/reference/tickers_snapshot/snapshot_date=2025-11-01/

        UNIVERSO COMPLETO: 34,324 tickers
        |- Activos: 11,899 (34.7%)
        +- Inactivos: 22,425 (65.3%)


                              |  FILTRO TEMPORAL


PASO 1: FILTRO TEMPORAL 2007-2018
--------------------------------------------------------------------------------
Script: filter_universe_2007_2018.py
Criterio: Activos HOY + Delistados entre 2007-01-01 y 2018-12-31
Output: processed/universe/tickers_2007_2018.parquet

        TEMPORAL 2007-2018: 19,969 tickers (-41.8%)
        |- Incluye: list_date filtering (con ticker_details)
        +- Mejora precision vs solo delisted_date


                          |  FILTRO CS + EXCHANGES


PASO 2: FILTRO CS + XNAS/XNYS
--------------------------------------------------------------------------------
Script: filter_universe_cs_exchanges_2007_2018.py
Criterio: type=CS + primary_exchange IN [XNAS, XNYS]
Output: processed/universe/tickers_2007_2018_cs_exchanges.parquet

        COMMON STOCKS NASDAQ/NYSE: 4,969 tickers (-75.1%)


                      |  ENRIQUECIMIENTO DUAL (ESTRATEGIA HIBRIDA)


PASO 3: ENRIQUECIMIENTO CON DATOS CORPORATIVOS
--------------------------------------------------------------------------------
Script: create_hybrid_enriched_universe.py (mismo que 2019-2025)
Input: tickers_2007_2018_cs_exchanges.parquet
Output: processed/universe/hybrid_enriched_2007_2018.parquet

        ENRIQUECIDO (26 columnas): 4,969 tickers
        |- ACTIVOS (3,132):
        |  |- market_cap: 3,103/3,132 (99.1%) <- Para filtrar Small Caps
        |  |- description: 3,106/3,132 (99.2%)
        |  +- employees: 3,043/3,132 (97.2%)
        |
        +- INACTIVOS (1,837):
           +- delisted_utc: 1,837/1,837 (100.0%) <- Para feature engineering ML


                        |  FILTRO SMALL CAPS < $2B


PASO 4: FILTRO SMALL CAPS (< $2B) - POBLACION TARGET
--------------------------------------------------------------------------------
Script: filter_smallcaps_population.py (mismo que 2019-2025)
Criterio DUAL:
  - ACTIVOS: market_cap < $2,000,000,000
  - INACTIVOS: SIN FILTRO (preservar TODOS para anti-survivorship bias)
Output: processed/universe/smallcaps_universe_2007_2018.parquet

        SMALL CAPS TARGET: 3,509 tickers (-29.3%)
        |- ACTIVOS (1,672 - 47.6%): <- FILTRADOS por market cap
        |  |- Micro caps (< $300M): 813 (48.6%)
        |  |- Small caps ($300M-$1B): 531 (31.8%)
        |  +- Mid-small caps ($1B-$2B): 328 (19.6%)
        |
        +- INACTIVOS (1,837 - 52.4%): <- TODOS preservados
           +- ANTI-SURVIVORSHIP BIAS APLICADO


                    |  ANALISIS DE DATOS YA DESCARGADOS




================================================================================
   RESULTADO FINAL - LIMITACIONES IMPORTANTES
================================================================================

UNIVERSO TEORICO 2007-2018: 3,509 tickers Small Caps

DATOS CONFIRMADOS EN DISCO: 55 tickers (1.6%)
   - Basado en descarga previa parcial
   - NO sabemos si los otros 3,454 tickers tienen datos en Polygon
   - Requiere "ping test" a Polygon API para confirmar disponibilidad real

PROXIMO PASO RECOMENDADO:
   -> Ejecutar ping test (5-10 min) para identificar tickers con datos reales
   -> Descargar solo tickers confirmados (vs descarga masiva de 3,509)
   -> Ahorro estimado: 95-98% de tiempo

================================================================================
```

---

## Filtro Temporal 2007-2018

**Objetivo**: Filtrar tickers que estuvieron listados entre 2007 y 2018 desde el snapshot completo.

**Fuente de datos**: Snapshot completo de Polygon (34,324 tickers)

**Script**: [scripts/00_universe_ingest/filter_universe_2007_2018.py](../../scripts/00_universe_ingest/filter_universe_2007_2018.py)

**Logica de filtrado**:
- **Activos**: Incluye activos HOY que podrian haber existido en 2007-2018
- **Inactivos**: Incluye SOLO los delistados entre 2007-01-01 y 2018-12-31
- **Mejora**: Usa `list_date` de ticker_details para filtrar activos listados despues de 2018

**Output**: [processed/universe/tickers_2007_2018.parquet](../../processed/universe/tickers_2007_2018.parquet)
- Total: 19,969 tickers

**Comando de ejecucion**:
```bash
python scripts/00_universe_ingest/filter_universe_2007_2018.py \
  --input raw/polygon/reference/tickers_snapshot/snapshot_date=2025-11-01/tickers_all.parquet \
  --output processed/universe/tickers_2007_2018.parquet \
  --start-year 2007 \
  --end-year 2018 \
  --details raw/polygon/reference/ticker_details/as_of_date=2025-11-01/details.parquet
```

**Resultado**:
```
Total filtrado 2007-2018: 19,969
```

---

## Filtrado por CS (Stocks) + XNAS/XNYS (Exchange)

**Objetivo**: Filtrar unicamente Common Stocks (type=CS) listados en NASDAQ (XNAS) o NYSE (XNYS) del universo temporal 2007-2018.

**Script**: [scripts/00_universe_ingest/filter_universe_cs_exchanges_2007_2018.py](../../scripts/00_universe_ingest/filter_universe_cs_exchanges_2007_2018.py)

**Input**: `processed/universe/tickers_2007_2018.parquet` (19,969 tickers)

**Filtros aplicados**:
- `type = "CS"` (Common Stock)
- `primary_exchange IN ["XNAS", "XNYS"]` (NASDAQ o NYSE)

**Output**: `processed/universe/tickers_2007_2018_cs_exchanges.parquet`

**Comando de ejecucion**:
```bash
python scripts/00_universe_ingest/filter_universe_cs_exchanges_2007_2018.py \
  --input processed/universe/tickers_2007_2018.parquet \
  --output processed/universe/tickers_2007_2018_cs_exchanges.parquet
```

**Resultado**:
```
CS+XNAS/XNYS 2007-2018: 4,969
```

---

## Universo Hibrido Enriquecido

**Objetivo**: Enriquecer el universo filtrado CS+NASDAQ/NYSE con datos corporativos mediante estrategia dual.

**Script**: [scripts/00_universe_ingest/create_hybrid_enriched_universe.py](../../scripts/00_universe_ingest/create_hybrid_enriched_universe.py)

**Input**:
- Base: `processed/universe/tickers_2007_2018_cs_exchanges.parquet` (4,969 tickers)
- Details: `raw/polygon/reference/ticker_details/as_of_date=2025-11-01/details.parquet`
- Snapshot: `raw/polygon/reference/tickers_snapshot/snapshot_date=2025-11-01/tickers_all.parquet`

**Output**: `processed/universe/hybrid_enriched_2007_2018.parquet`

**Comando de ejecucion**:
```bash
python scripts/00_universe_ingest/create_hybrid_enriched_universe.py \
  --base processed/universe/tickers_2007_2018_cs_exchanges.parquet \
  --details raw/polygon/reference/ticker_details/as_of_date=2025-11-01/details.parquet \
  --snapshot raw/polygon/reference/tickers_snapshot/snapshot_date=2025-11-01/tickers_all.parquet \
  --output processed/universe/hybrid_enriched_2007_2018.parquet
```

**Resultado**:
```
Total tickers enriquecidos: 4,969
   Activos: 3,132
   Inactivos: 1,837

Market Cap (activos): 3,103/3,132 (99.1%)
Delisted UTC (inactivos): 1,837/1,837 (100.0%)

Small caps (< $2B): 1,672 (53.9% de activos con market_cap)
```

---

## Filtro Small Caps Target Population (< $2B) con Anti-Survivorship Bias

**Objetivo**: Filtrar la poblacion target de Small Caps (market cap < $2B) preservando TODOS los tickers inactivos para eliminar survivorship bias.

**Script**: [scripts/00_universe_ingest/filter_smallcaps_population.py](../../scripts/00_universe_ingest/filter_smallcaps_population.py)

**Input**: `processed/universe/hybrid_enriched_2007_2018.parquet` (4,969 tickers)

**Estrategia dual de filtrado**:
- **ACTIVOS**: `market_cap < $2,000,000,000` -> 1,672 tickers
- **INACTIVOS**: **SIN FILTRO** (todos preservados) -> 1,837 tickers

**Output**: `processed/universe/smallcaps_universe_2007_2018.parquet`

**Comando de ejecucion**:
```bash
python scripts/00_universe_ingest/filter_smallcaps_population.py \
  --input processed/universe/hybrid_enriched_2007_2018.parquet \
  --output processed/universe/smallcaps_universe_2007_2018.parquet \
  --market-cap-threshold 2000000000
```

**Resultado**:
```
Total poblacion target: 3,509
   Small Caps activos: 1,672 (47.6%)
   Inactivos preservados: 1,837 (52.4%)
   Sin survivorship bias: SI

Distribucion por rangos:
   < $300M           813 ( 48.6%)
   $300M - $1B       531 ( 31.8%)
   $1B - $2B         328 ( 19.6%)

Distribucion por exchange:
   XNAS: 1,736 (49.5%)
   XNYS: 1,773 (50.5%)
```

---

