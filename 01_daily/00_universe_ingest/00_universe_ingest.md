# Pipeline de Universo Small Caps


1. [Descarga Universo Completo (34,324 tickers)](#descarga-universo--34380-tickers---activos--inactivos)
2. [Filtro Temporal 2019-2025 (20,471 tickers)](#filtra-los-tickers-que-estuvieron-listados-entre-2019-y-2025)
3. [Filtro CS + XNAS/XNYS (8,307 tickers)](#filtrado-por-cs-stocks--xnasxnys-exchange)
4. [Enriquecimiento Dual (8,307 tickers + 25 columnas)](#universo-híbrido-enriquecido-market-cap-description-employees-sic-code-delisted-utc)
5. [Filtro Small Caps < $2B (6,405 tickers - Población Target)](#filtro-small-caps-target-population--2b-con-anti-survivorship-bias)
6. [Visualización del Pipeline Completo en jupyter notebook](#notebook-resultados)

---

## Route Map - Pipeline Completo

```bash
════════════════════════════════════════════════════════════════════════════════
                        PIPELINE DE CONSTRUCCIÓN DE UNIVERSO
════════════════════════════════════════════════════════════════════════════════

PASO 1: DESCARGA SNAPSHOT COMPLETO
────────────────────────────────────────────────────────────────────────────────
Script: download_universe.py
Output: raw/polygon/reference/tickers_snapshot/snapshot_date=2025-11-01/

        📦 UNIVERSO COMPLETO: 34,324 tickers
        ├─ Activos: 11,899 (34.7%)
        └─ Inactivos: 22,425 (65.3%)


                              ↓  FILTRO TEMPORAL


PASO 2: FILTRO TEMPORAL 2019-2025
────────────────────────────────────────────────────────────────────────────────
Script: filter_universe_2019_2025.py
Criterio: Activos HOY + Delistados desde 2019-01-01 en adelante
Output: processed/universe/tickers_2019_2025.parquet

        📊 TEMPORAL 2019-2025: 20,471 tickers (-40.3%)
        ├─ Activos: 11,897 (58.1%)  ← Todos los activos actuales
        └─ Inactivos: 8,574 (41.9%) ← Solo delistados desde 2019+


                          ↓  FILTRO CS + EXCHANGES


PASO 3: FILTRO CS + XNAS/XNYS
────────────────────────────────────────────────────────────────────────────────
Script: filter_universe_cs_exchanges.py
Criterio: type=CS + primary_exchange IN [XNAS, XNYS]
Output: processed/universe/tickers_2019_2025_cs_exchanges.parquet

        🏛️  COMMON STOCKS NASDAQ/NYSE: 8,307 tickers (-59.4%)
        ├─ Activos: 5,007 (60.3%)
        └─ Inactivos: 3,300 (39.7%)

        Por exchange:
        ├─ NASDAQ (XNAS): 5,619 (67.6%)
        └─ NYSE (XNYS): 2,688 (32.4%)


                      ↓  ENRIQUECIMIENTO DUAL (ESTRATEGIA HÍBRIDA)


PASO 4: ENRIQUECIMIENTO CON DATOS CORPORATIVOS
────────────────────────────────────────────────────────────────────────────────
Script 4a: enrich_ticker_details.py (descarga desde /v3/reference/tickers/{ticker})
Script 4b: create_hybrid_enriched_universe.py (merge dual: ticker_details + snapshot)
Output: processed/universe/hybrid_enriched_2025-11-01.parquet

        📈 ENRIQUECIDO (25 columnas): 8,307 tickers
        ├─ ACTIVOS (5,007):
        │  ├─ market_cap: 4,883/5,007 (97.5%) ← Para filtrar Small Caps
        │  ├─ description: 4,934/5,007 (98.5%)
        │  ├─ employees: 4,509/5,007 (90.1%)
        │  └─ sic_code: 4,134/5,007 (82.6%)
        │
        └─ INACTIVOS (3,300):
           └─ delisted_utc: 3,300/3,300 (100.0%) ← Para feature engineering ML


                        ↓  FILTRO SMALL CAPS < $2B


PASO 5: FILTRO SMALL CAPS (< $2B) - POBLACIÓN TARGET
────────────────────────────────────────────────────────────────────────────────
Script: filter_smallcaps_population.py
Criterio DUAL:
  - ACTIVOS: market_cap < $2,000,000,000
  - INACTIVOS: SIN FILTRO (preservar TODOS para anti-survivorship bias)
Output: processed/universe/smallcaps_universe_2025-11-01.parquet

        🎯 SMALL CAPS TARGET: 6,405 tickers (-22.9%)
        ├─ ACTIVOS (3,105 - 48.5%): ← FILTRADOS por market cap
        │  ├─ Micro caps (< $300M): 1,747 (56.3%)
        │  ├─ Small caps ($300M-$1B): 863 (27.8%)
        │  └─ Mid-small caps ($1B-$2B): 495 (15.9%)
        │
        └─ INACTIVOS (3,300 - 51.5%): ← TODOS preservados
           └─ ✅ ANTI-SURVIVORSHIP BIAS APLICADO


                    ↓  ENRIQUECIMIENTO EVENTOS CORPORATIVOS


PASO 6: INGESTA GLOBAL DE SPLITS & DIVIDENDS
────────────────────────────────────────────────────────────────────────────────
Script: ingest_splits_dividends.py
Fuente: Polygon /v3/reference/splits y /v3/reference/dividends (SIN FILTROS)
Output: raw/polygon/reference/splits/year=*/ y dividends/year=*/

        📊 SPLITS GLOBALES: ~26,696 registros (1978-2025)
        ├─ Tickers únicos: 18,454
        ├─ Período: 1978-2025 (48 años)
        └─ Particionado por: execution_date (año)

        💰 DIVIDENDS GLOBALES: ~1.8M registros (histórico completo)
        ├─ Tickers únicos: Miles
        ├─ Período: Histórico completo
        └─ Particionado por: ex_dividend_date (año)

        ⚠️  NOTA: Datos GLOBALES (todos los tickers)
            Siguiente paso: Filtrar para 6,405 tickers Small Caps


════════════════════════════════════════════════════════════════════════════════
✅ RESULTADO FINAL: 6,405 tickers Small Caps listos para FASE B (OHLCV)
════════════════════════════════════════════════════════════════════════════════
```

## descarga universo  (34,380 tickers - activos + inactivos)

* **Objetivo**: Descargar universo completo 2004-2025 (34,380 tickers - activos + inactivos). Este archivo es el primer paso del pipeline de Small Caps. Sirve para descargar el universo completo de compañías (activas + delistadas) de Polygon.io y dejarlo guardado como snapshot diario en formato Parquet particionado.  
* **Fuente de datos**: Polygon API (tickers snapshot histórico) 
* **Script**: [scripts/download_universe.py](../../scripts/download_universe.py)    
* **Output**: [raw/polygon/reference/tickers_snapshot/snapshot_date=2025-11-01/](../../raw/polygon/reference/tickers_snapshot/snapshot_date=2025-11-01/)  

```bash
D:\TSIS_SmallCaps\
├── scripts/
│   └── download_universe.py                   # Paso 1: Descarga snapshot
│
├── raw/polygon/reference/tickers_snapshot/
│   │
│   └── snapshot_date=2025-11-01/              # UNIVERSO COMPLETO
│        ├── tickers_all.parquet                # 34,324 tickers (activos + inactivos)
│        ├── tickers_active.parquet             # 11,899 tickers (solo activos)
│        └── tickers_inactive.parquet           # 22,425 tickers (solo inactivos)
│        └── summary.csv
├── 
```

```sh
📊 1. tickers_all.parquet
----------------------------------------------------------------------------------------------------
Total rows: 34,324

📊 2. tickers_active.parquet (11,853 active tickers)
----------------------------------------------------------------------------------------------------
Total rows: 11,899

HEAD(2):
shape: (14, 3)
┌──────────────────┬────────────────────────────────┬────────────────────────────────┐
│ column           ┆ column_0                       ┆ column_1                       │
╞══════════════════╪════════════════════════════════╪════════════════════════════════╡
│ ticker           ┆ A                              ┆ AA                             │
│ name             ┆ Agilent Technologies Inc.      ┆ Alcoa Corporation              │
│ market           ┆ stocks                         ┆ stocks                         │
│ locale           ┆ us                             ┆ us                             │
│ primary_exchange ┆ XNYS                           ┆ XNYS                           │
│ type             ┆ CS                             ┆ CS                             │
│ active           ┆ true                           ┆ true                           │
│ currency_name    ┆ usd                            ┆ usd                            │
│ cik              ┆ 0001090872                     ┆ 0001675149                     │
│ composite_figi   ┆ BBG000C2V3D6                   ┆ BBG00B3T3HD3                   │
│ share_class_figi ┆ BBG001SCTQY4                   ┆ BBG00B3T3HF1                   │
│ last_updated_utc ┆ 2025-11-01T06:07:02.287917761Z ┆ 2025-11-01T06:07:02.287918242Z │
│ snapshot_date    ┆ 2025-11-01                     ┆ 2025-11-01                     │
│ delisted_utc     ┆ null                           ┆ null                           │
└──────────────────┴────────────────────────────────┴────────────────────────────────┘

📊 3. tickers_inactive.parquet (22,557 inactive tickers)
----------------------------------------------------------------------------------------------------
Total rows: 22,557

HEAD(2):
shape: (14, 3)
┌──────────────────┬────────────────────────────────┬────────────────────────────────┐
│ column           ┆ column_0                       ┆ column_1                       │
╞══════════════════╪════════════════════════════════╪════════════════════════════════╡
│ ticker           ┆ A                              ┆ AA                             │
│ name             ┆ Agilent Technologies Inc.      ┆ Alcoa Corporation              │
│ market           ┆ stocks                         ┆ stocks                         │
│ locale           ┆ us                             ┆ us                             │
│ primary_exchange ┆ XNYS                           ┆ XNYS                           │
│ type             ┆ CS                             ┆ CS                             │
│ active           ┆ true                           ┆ true                           │
│ currency_name    ┆ usd                            ┆ usd                            │
│ cik              ┆ 0001090872                     ┆ 0001675149                     │
│ composite_figi   ┆ BBG000C2V3D6                   ┆ BBG00B3T3HD3                   │
│ share_class_figi ┆ BBG001SCTQY4                   ┆ BBG00B3T3HF1                   │
│ last_updated_utc ┆ 2025-11-01T06:07:02.287917761Z ┆ 2025-11-01T06:07:02.287918242Z │
│ snapshot_date    ┆ 2025-11-01                     ┆ 2025-11-01                     │
│ delisted_utc     ┆ null                           ┆ null                           │
└──────────────────┴────────────────────────────────┴────────────────────────────────┘
```

## filtra los tickers que estuvieron listados entre 2019 y 2025

**Objetivo**: Filtrar tickers que estuvieron listados entre 2019 y 2025 desde el snapshot completo (20,471 tickers). Este paso elimina tickers que no tienen datos en el período de interés, manteniendo tanto activos como inactivos para evitar survivorship bias.  
**Fuente de datos**: Snapshot completo de Polygon (34,324 tickers)  
**Script**: [scripts/filter_universe_2019_2025.py](../../../scripts/filter_universe_2019_2025.py)   

**Lógica de filtrado**:  
- **Activos (11,897)**: Incluye TODOS los tickers activos hoy (asumiendo que existían en 2019+)  
- **Inactivos (8,574)**: Incluye SOLO los delistados desde 2019-01-01 en adelante  
- **Limitación**: Polygon no proporciona `list_date`, solo `delisted_utc`  

**Output**: [processed/universe/](../../../processed/universe/)  
- `tickers_2019_2025.parquet` (20,471 tickers, 15 columnas)  
- `tickers_2019_2025.csv` (versión CSV)  

```sh
D:\TSIS_SmallCaps\
├── scripts/
│   ├── download_universe.py                   # Paso 1: Descarga snapshot
│   └── filter_universe_2019_2025.py           # Paso 2: Filtro temporal
│
├── raw/polygon/reference/tickers_snapshot/
│   └── snapshot_date=2025-11-01/              # UNIVERSO COMPLETO (Paso 1)
│       ├── tickers_all.parquet                # 34,324 tickers (activos + inactivos)
│       ├── tickers_active.parquet             # 11,899 tickers (solo activos)
│       ├── tickers_inactive.parquet           # 22,425 tickers (solo inactivos)
│       └── summary.csv
│
├── processed/universe/
│   ├── tickers_2019_2025.parquet              # FILTRO TEMPORAL (Paso 2)
│   │                                          # 20,471 tickers (listados 2019-2025)
│   │                                          # Activos: 11,897 | Inactivos: 8,574
│   └── tickers_2019_2025.csv                  # (versión CSV)
```

**Comando de ejecución**:
```bash
python scripts/filter_universe_2019_2025.py \
  --input raw/polygon/reference/tickers_snapshot/snapshot_date=2025-11-01/tickers_all.parquet \
  --output processed/universe/tickers_2019_2025.csv \
  --start-year 2019 \
  --end-year 2025 
```

```sh
📊 4. tickers_2019_2025.parquet (20,471 tickers filtrados)
----------------------------------------------------------------------------------------------------
Total rows: 20,471
Total columns: 15

📈 Distribución por Status:
shape: (2, 2)
┌────────┬───────┐
│ active ┆ len   │
╞════════╪═══════╡
│ false  ┆ 8574  │
│ true   ┆ 11897 │
└────────┴───────┘

📈 Distribución por Exchange:
shape: (5, 2)
┌──────────────────┬──────┐
│ primary_exchange ┆ len  │
╞══════════════════╪══════╡
│ XNAS             ┆ 9796 │
│ XNYS             ┆ 5451 │
│ ARCX             ┆ 3258 │
│ BATS             ┆ 1348 │
│ XASE             ┆ 618  │
└──────────────────┴──────┘

📈 Distribución por Type:
shape: (15, 2)
┌─────────┬──────┐
│ type    ┆ len  │
╞═════════╪══════╡
│ CS      ┆ 8698 │
│ ETF     ┆ 5475 │
│ WARRANT ┆ 1869 │
│ UNIT    ┆ 1239 │
│ PFD     ┆ 953  │
│ ADRC    ┆ 616  │
│ FUND    ┆ 519  │
│ SP      ┆ 306  │
│ RIGHT   ┆ 306  │
│ ETN     ┆ 178  │
│ ETS     ┆ 139  │
│ null    ┆ 91   │
│ ETV     ┆ 75   │
│ INDEX   ┆ 6    │
│ ADRR    ┆ 1    │
└─────────┴──────┘

📈 Con delisted_utc: 8,574 (41.9%)

📋 Ejemplo de Compañía ACTIVA:
----------------------------------------------------------------------------------------------------
shape: (15, 2)
┌──────────────────┬────────────────────────────────┐
│ column           ┆ column_0                       │
╞══════════════════╪════════════════════════════════╡
│ ticker           ┆ A                              │
│ name             ┆ Agilent Technologies Inc.      │
│ market           ┆ stocks                         │
│ locale           ┆ us                             │
│ primary_exchange ┆ XNYS                           │
│ type             ┆ CS                             │
│ active           ┆ true                           │
│ currency_name    ┆ usd                            │
│ cik              ┆ 0001090872                     │
│ composite_figi   ┆ BBG000C2V3D6                   │
│ share_class_figi ┆ BBG001SCTQY4                   │
│ last_updated_utc ┆ 2025-11-01T06:07:02.287917761Z │
│ snapshot_date    ┆ 2025-11-01                     │
│ delisted_utc     ┆ null                           │
│ delisted_date    ┆ null                           │
└──────────────────┴────────────────────────────────┘

📋 Ejemplo de Compañía INACTIVA:
----------------------------------------------------------------------------------------------------
shape: (15, 2)
┌──────────────────┬─────────────────────────────┐
│ column           ┆ column_0                    │
╞══════════════════╪═════════════════════════════╡
│ ticker           ┆ AABA                        │
│ name             ┆ Altaba Inc. Common Stock    │
│ market           ┆ stocks                      │
│ locale           ┆ us                          │
│ primary_exchange ┆ XNAS                        │
│ type             ┆ CS                          │
│ active           ┆ false                       │
│ currency_name    ┆ usd                         │
│ cik              ┆ 0001011006                  │
│ composite_figi   ┆ BBG000KB2D74                │
│ share_class_figi ┆ BBG001S8V781                │
│ last_updated_utc ┆ 2024-12-03T21:33:22.821052Z │
│ snapshot_date    ┆ 2025-11-01                  │
│ delisted_utc     ┆ 2019-10-07T04:00:00Z        │
│ delisted_date    ┆ 2019-10-07                  │
└──────────────────┴─────────────────────────────┘

📊 Distribución por año (Activos - cuántos estaban activos cada año):
----------------------------------------------------------------------------------------------------
Total activos hoy: 11,897
shape: (7, 2)
┌─────────────┬───────┐
│ active_year ┆ len   │
╞═════════════╪═══════╡
│ 2019        ┆ 19644 │
│ 2020        ┆ 18696 │
│ 2021        ┆ 17401 │
│ 2022        ┆ 16031 │
│ 2023        ┆ 14221 │
│ 2024        ┆ 12835 │
│ 2025        ┆ 11897 │
└─────────────┴───────┘

📊 Distribución por año (Deslistados - cuántos fueron deslistados cada año):
----------------------------------------------------------------------------------------------------
Total inactivos con fecha de delist: 8,574
shape: (7, 2)
┌─────────────┬──────┐
│ delist_year ┆ len  │
╞═════════════╪══════╡
│ 2019        ┆ 827  │
│ 2020        ┆ 948  │
│ 2021        ┆ 1295 │
│ 2022        ┆ 1370 │
│ 2023        ┆ 1810 │
│ 2024        ┆ 1386 │
│ 2025        ┆ 938  │
└─────────────┴──────┘

Columnas disponibles:
['ticker', 'name', 'market', 'locale', 
 'primary_exchange', 'type', 'active', 
 'currency_name', 'cik', 'composite_figi', 
 'share_class_figi', 'last_updated_utc', 
 'snapshot_date', 'delisted_utc', 'delisted_date']
```

## Filtrado por CS (Stocks) + XNAS/XNYS (Exchange)

* **Objetivo**: Filtrar únicamente Common Stocks (type=CS) listados en NASDAQ (XNAS) o NYSE (XNYS) del universo temporal 2019-2025. Este filtro excluye ETFs, Warrants, Units, Preferred Stocks y otros tipos de instrumentos, así como tickers en exchanges menores (ARCX, BATS, XASE).  
* **Fuente de datos**: Universo filtrado temporalmente 2019-2025 (20,471 tickers)  
* **Script**: [scripts/filter_universe_cs_exchanges.py](../../../scripts/filter_universe_cs_exchanges.py)  
* **Input**: `processed/universe/tickers_2019_2025.parquet` (20,471 tickers)  
* **Filtros aplicados**:  
    - `type = "CS"` (Common Stock)  
    - `primary_exchange IN ["XNAS", "XNYS"]` (NASDAQ o NYSE)  

**Resultado**:  
- **Total filtrado**: 8,307 tickers (40.6% retención)  
- **Activos hoy**: 5,007 (60.3%)  
- **Inactivos hoy**: 3,300 (39.7%)  
- **NASDAQ (XNAS)**: 5,619 tickers (67.6%)  
- **NYSE (XNYS)**: 2,688 tickers (32.4%)  

**Output**: `processed/universe/tickers_2019_2025_cs_exchanges.parquet`  

```sh
D:\TSIS_SmallCaps\
├── scripts/
│   ├── download_universe.py                   # Paso 1: Descarga snapshot
│   ├── filter_universe_2019_2025.py           # Paso 2: Filtro temporal
│   └── filter_universe_cs_exchanges.py        # Paso 3: Filtro CS+XNAS/XNYS
│
│
├── raw/polygon/reference/tickers_snapshot/
│   └── snapshot_date=2025-11-01/              # UNIVERSO COMPLETO (Paso 1)
│       ├── tickers_all.parquet                # 34,324 tickers (activos + inactivos)
│       ├── tickers_active.parquet             # 11,899 tickers (solo activos)
│       ├── tickers_inactive.parquet           # 22,425 tickers (solo inactivos)
│       └── summary.csv
│
├── processed/universe/
│   ├── tickers_2019_2025.parquet              # FILTRO TEMPORAL (Paso 2)
│   │                                          # 20,471 tickers (listados 2019-2025)
│   │                                          # Activos: 11,897 | Inactivos: 8,574
│   ├── tickers_2019_2025.csv                  # (versión CSV)
│   │
│   ├── tickers_2019_2025_cs_exchanges.parquet # FILTRO CS + XNAS/XNYS (Paso 3)
│   │                                          # 8,307 tickers (CS en NASDAQ/NYSE)
│   │                                          # Activos: 5,007 | Inactivos: 3,300
│   │                                          # XNAS: 5,619 | XNYS: 2,688
│   └── tickers_2019_2025_cs_exchanges.csv     # (versión CSV)
│
```

**Comando de ejecución**:  
```bash
python scripts/filter_universe_cs_exchanges.py \
  --input processed/universe/tickers_2019_2025.parquet \
  --output processed/universe/tickers_2019_2025_cs_exchanges.csv
```

```sh
====================================================================================================
📊 5. tickers_2019_2025_cs_exchanges.parquet - Common Stocks en NASDAQ/NYSE
====================================================================================================

✅ Total tickers CS en XNAS/XNYS: 8,307
   Columnas disponibles: 15

📋 Columnas:
    1. ticker
    2. name
    3. market
    4. locale
    5. primary_exchange
    6. type
    7. active
    8. currency_name
    9. cik
   10. composite_figi
   11. share_class_figi
   12. last_updated_utc
   13. snapshot_date
   14. delisted_utc
   15. delisted_date

📈 Distribución:
   Activos hoy:   5,007 (60.3%)
   Inactivos hoy: 3,300 (39.7%)

🏛️  Distribución por Exchange:
   XNAS  : 5,619 (67.6%)
   XNYS  : 2,688 (32.4%)

✅ Verificación de tipo:
   CS: 8,307

----------------------------------------------------------------------------------------------------
📋 EJEMPLO: Compañía ACTIVA (CS en NASDAQ/NYSE)
----------------------------------------------------------------------------------------------------
shape: (15, 2)
┌──────────────────┬────────────────────────────────┐
│ column           ┆ column_0                       │
╞══════════════════╪════════════════════════════════╡
│ ticker           ┆ A                              │
│ name             ┆ Agilent Technologies Inc.      │
│ market           ┆ stocks                         │
│ locale           ┆ us                             │
│ primary_exchange ┆ XNYS                           │
│ type             ┆ CS                             │
│ active           ┆ true                           │
│ currency_name    ┆ usd                            │
│ cik              ┆ 0001090872                     │
│ composite_figi   ┆ BBG000C2V3D6                   │
│ share_class_figi ┆ BBG001SCTQY4                   │
│ last_updated_utc ┆ 2025-11-01T06:07:02.287917761Z │
│ snapshot_date    ┆ 2025-11-01                     │
│ delisted_utc     ┆ null                           │
│ delisted_date    ┆ null                           │
└──────────────────┴────────────────────────────────┘

----------------------------------------------------------------------------------------------------
📋 EJEMPLO: Compañía INACTIVA (CS en NASDAQ/NYSE)
----------------------------------------------------------------------------------------------------
shape: (15, 2)
┌──────────────────┬─────────────────────────────┐
│ column           ┆ column_0                    │
╞══════════════════╪═════════════════════════════╡
│ ticker           ┆ AABA                        │
│ name             ┆ Altaba Inc. Common Stock    │
│ market           ┆ stocks                      │
│ locale           ┆ us                          │
│ primary_exchange ┆ XNAS                        │
│ type             ┆ CS                          │
│ active           ┆ false                       │
│ currency_name    ┆ usd                         │
│ cik              ┆ 0001011006                  │
│ composite_figi   ┆ BBG000KB2D74                │
│ share_class_figi ┆ BBG001S8V781                │
│ last_updated_utc ┆ 2024-12-03T21:33:22.821052Z │
│ snapshot_date    ┆ 2025-11-01                  │
│ delisted_utc     ┆ 2019-10-07T04:00:00Z        │
│ delisted_date    ┆ 2019-10-07                  │
└──────────────────┴─────────────────────────────┘

----------------------------------------------------------------------------------------------------
📊 Distribución por año - ACTIVOS (tickers que estaban activos en cada año)
----------------------------------------------------------------------------------------------------
shape: (7, 2)
┌──────┬─────────┐
│ year ┆ activos │
╞══════╪═════════╡
│ 2019 ┆ 7956    │
│ 2020 ┆ 7629    │
│ 2021 ┆ 7162    │
│ 2022 ┆ 6656    │
│ 2023 ┆ 5960    │
│ 2024 ┆ 5423    │
│ 2025 ┆ 5007    │
└──────┴─────────┘

----------------------------------------------------------------------------------------------------
📊 Distribución por año - DESLISTADOS (tickers delistados en cada año)
----------------------------------------------------------------------------------------------------
shape: (7, 2)
┌──────┬─────────────┐
│ year ┆ deslistados │
╞══════╪═════════════╡
│ 2019 ┆ 351         │
│ 2020 ┆ 327         │
│ 2021 ┆ 467         │
│ 2022 ┆ 506         │
│ 2023 ┆ 696         │
│ 2024 ┆ 537         │
│ 2025 ┆ 416         │
└──────┴─────────────┘

====================================================================================================
📊 RESUMEN COMPARATIVO - Pipeline de Filtrado
====================================================================================================
shape: (3, 5)
┌─────────────────────────────────┬─────────┬─────────┬───────────┬─────────────┐
│ Paso                            ┆ Tickers ┆ Activos ┆ Inactivos ┆ % del total │
╞═════════════════════════════════╪═════════╪═════════╪═══════════╪═════════════╡
│ 1. Snapshot completo (2025-11-… ┆ 34324   ┆ 11899   ┆ 22557     ┆ 100.0       │
│ 2. Filtro temporal (2019-2025)  ┆ 20471   ┆ 11897   ┆ 8574      ┆ 59.6        │
│ 3. Filtro CS + XNAS/XNYS        ┆ 8307    ┆ 5007    ┆ 3300      ┆ 24.2        │
└─────────────────────────────────┴─────────┴─────────┴───────────┴─────────────┘

✅ Filtrado completado exitosamente!
   De 34,324 tickers iniciales → 8,307 Common Stocks en NASDAQ/NYSE (2019-2025)
   Retención: 24.2%
```

## Universo Híbrido Enriquecido (Market Cap, Description, Employees, SIC Code, Delisted UTC)

* **Objetivo**: Enriquecer el universo filtrado CS+NASDAQ/NYSE con datos corporativos mediante una estrategia dual de enriquecimiento.  
Esta estrategia permite filtrar small caps activos (< $2B) sin perder tickers inactivos críticos para eliminar survivorship bias. 

* **Estrategia Dual de Enriquecimiento**:
    * Enriquecer para **ACTIVOS** (5,007 tickers): Obtienen datos desde `/v3/reference/tickers/{ticker}` (ticker_details API)
        - `market_cap` - Capitalización de mercado (para filtrar small caps < $2B)
        - `description` - Descripción del negocio
        - `weighted_shares_outstanding` - Acciones en circulación
        - `total_employees` - Número de empleados
        - `sic_code` / `sic_description` - Código de industria
        - `homepage_url` - Sitio web corporativo
        - `phone_number`, `address` - Contacto

    * Enriquecer para **INACTIVOS** (3,300 tickers):  NO tienen datos en ticker_details (404), pero SÍ tienen `delisted_utc` en snapshot original  
        - `delisted_utc` - Fecha de delisting (CRÍTICO para ML feature engineering)
        - `cik` - SEC CIK number
        - `composite_figi` - FIGI identifier
        - `list_date` - Fecha de listado original

```sh
D:\TSIS_SmallCaps\
├── scripts/
│   ├── download_universe.py                   # Paso 1: Descarga snapshot
│   ├── filter_universe_2019_2025.py           # Paso 2: Filtro temporal
│   ├── filter_universe_cs_exchanges.py        # Paso 3: Filtro CS+XNAS/XNYS
│   ├── enrich_ticker_details.py               # Paso 4a: Descarga ticker details
│   └── create_hybrid_enriched_universe.py     # Paso 4b: Merge dual
│
├── raw/polygon/reference/
│   ├── tickers_snapshot/
│   │   └── snapshot_date=2025-11-01/          # UNIVERSO COMPLETO (Paso 1)
│   │       ├── tickers_all.parquet            # 34,324 tickers (activos + inactivos)
│   │       ├── tickers_active.parquet         # 11,899 tickers (solo activos)
│   │       ├── tickers_inactive.parquet       # 22,425 tickers (solo inactivos)
│   │       └── summary.csv
│   │
│   └── ticker_details/
│       └── as_of_date=2025-11-01/             # TICKER DETAILS (Paso 4a)
│           └── details.parquet                # 8,307 tickers procesados
│                                              # Con datos: 5,204 (activos)
│                                              # Not found (404): 3,103 (inactivos)
│
├── processed/universe/
│   ├── tickers_2019_2025.parquet              # FILTRO TEMPORAL (Paso 2)
│   │                                          # 20,471 tickers (listados 2019-2025)
│   │                                          # Activos: 11,897 | Inactivos: 8,574
│   ├── tickers_2019_2025.csv
│   │
│   ├── tickers_2019_2025_cs_exchanges.parquet # FILTRO CS + XNAS/XNYS (Paso 3)
│   │                                          # 8,307 tickers (CS en NASDAQ/NYSE)
│   │                                          # Activos: 5,007 | Inactivos: 3,300
│   │                                          # XNAS: 5,619 | XNYS: 2,688
│   ├── tickers_2019_2025_cs_exchanges.csv
│   │
│   └── hybrid_enriched_2025-11-01.parquet     # ENRIQUECIMIENTO DUAL (Paso 4b)
│                                              # 8,307 tickers enriquecidos (25 columnas)
│                                              # ACTIVOS (5,007): 97.5% con market_cap
│                                              # INACTIVOS (3,300): 100% con delisted_utc
│
```

**Ejecutar pipeline completo:**

```bash
# PASO 1: Descargar ticker details (3 min)
python scripts/enrich_ticker_details.py \
    --input processed/universe/tickers_2019_2025_cs_exchanges.parquet \
    --output raw/polygon/reference/ticker_details \
    --as-of-date 2025-11-01 \
    --max-workers 16

# PASO 2: Crear universo híbrido enriquecido (instantáneo)
python scripts/create_hybrid_enriched_universe.py \
    --base processed/universe/tickers_2019_2025_cs_exchanges.parquet \
    --details raw/polygon/reference/ticker_details/as_of_date=2025-11-01/details.parquet \
    --snapshot raw/polygon/reference/tickers_snapshot/snapshot_date=2025-11-01/tickers_all.parquet \
    --output processed/universe/hybrid_enriched_2025-11-01.parquet
```

```sh
================================================================================
VERIFICACIÓN: UNIVERSO HÍBRIDO ENRIQUECIDO
================================================================================

Archivo: processed\universe\hybrid_enriched_2025-11-01.parquet
   Tamaño: 1.39 MB

📊 ESTADÍSTICAS GENERALES
   Total tickers: 8,307
   Total columnas: 25

   Activos: 5,007 (60.3%)
   Inactivos: 3,300 (39.7%)

================================================================================
ESTRATEGIA DUAL DE ENRIQUECIMIENTO
================================================================================

ACTIVOS (5,007 tickers) - Desde ticker_details API:
   Campo                            No-Null         % Completitud
   ----------------------------------------------------------------------
   Market Cap                           4,883/5,007    97.5%
   Shares Outstanding                   4,900/5,007    97.9%
   Description                          4,934/5,007    98.5%
   Employees                            4,509/5,007    90.1%
   SIC Code                             4,134/5,007    82.6%
   Homepage URL                         4,797/5,007    95.8%
   Phone Number                         4,204/5,007    84.0%

INACTIVOS (3,300 tickers) - Desde snapshot original:
   Campo                            No-Null         % Completitud
   ----------------------------------------------------------------------
   Delisted UTC (CRÍTICO ML)            3,300/3,300   100.0%
   CIK                                  3,211/3,300    97.3%
   FIGI                                 2,009/3,300    60.9%

================================================================================
TABLA DE COMPLETITUD CONSOLIDADA
================================================================================

shape: (9, 5)
┌─────────────────────────────┬───────────────┬───────────────┬─────────────┬───────────────────┐
│ Campo                       ┆ No-Null       ┆ % Completitud ┆ Tipo Ticker ┆ Fuente            │
╞═════════════════════════════╪═══════════════╪═══════════════╪═════════════╪═══════════════════╡
│ market_cap                  ┆ 4,883 / 5,007 ┆ 97.5%         ┆ ACTIVOS     ┆ ticker_details    │
│ delisted_utc                ┆ 3,300 / 3,300 ┆ 100.0%        ┆ INACTIVOS   ┆ snapshot original │
│ weighted_shares_outstanding ┆ 4,900 / 5,007 ┆ 97.9%         ┆ ACTIVOS     ┆ ticker_details    │
│ description                 ┆ 4,934 / 5,007 ┆ 98.5%         ┆ ACTIVOS     ┆ ticker_details    │
│ sic_code                    ┆ 4,134 / 5,007 ┆ 82.6%         ┆ ACTIVOS     ┆ ticker_details    │
│ homepage_url                ┆ 4,797 / 5,007 ┆ 95.8%         ┆ ACTIVOS     ┆ ticker_details    │
│ total_employees             ┆ 4,509 / 5,007 ┆ 90.1%         ┆ ACTIVOS     ┆ ticker_details    │
│ cik                         ┆ 8,202 / 8,307 ┆ 98.7%         ┆ TODOS       ┆ snapshot original │
│ composite_figi              ┆ 6,040 / 8,307 ┆ 72.7%         ┆ TODOS       ┆ snapshot original │
└─────────────────────────────┴───────────────┴───────────────┴─────────────┴───────────────────┘

================================================================================
VERIFICACIÓN: SMALL CAPS (< $2B)
================================================================================

Activos con market_cap: 4,883 / 5,007

Small caps (< $2B):
   Total: 3,105 / 4,883 (63.6%)

   Distribución por rangos:
   < $300M         1,747 ( 35.8%)
   $300M - $1B       863 ( 17.7%)
   $1B - $2B         495 ( 10.1%)
   > $2B           1,778 ( 36.4%)

================================================================================
VERIFICACIÓN: SURVIVORSHIP BIAS ELIMINADO
================================================================================

✅ Inactivos preservados: 3,300
✅ Con delisted_utc: 3,300 / 3,300 (100.0%)

   Esto permite:
   • Calcular days_to_delisting (feature ML)
   • Detectar pump & dumps terminales
   • Eliminar survivorship bias en backtesting

================================================================================
RESUMEN FINAL
================================================================================

Total tickers enriquecidos: 8,307
   • Activos con market_cap: 4,883 (97.5%)
   • Inactivos con delisted_utc: 3,300 (100.0%)
   • Small caps identificados: 3,105
   • Sin survivorship bias: ✅
```

**Observaciones Críticas**:
- ✅ **97.5% activos** tienen `market_cap` → Podemos filtrar small caps < $2B
- ✅ **100% inactivos** tienen `delisted_utc` → Podemos calcular `days_to_delisting` (feature ML)
- ✅ **Sin survivorship bias** → 3,300 inactivos preservados
- ⚠️ `sic_code` solo 79.2% → Considerar imputation por sector
---


## Filtro Small Caps Target Population (< $2B) con Anti-Survivorship Bias

>**[ADVERTENCIA]**  
**Polygon API `/v3/reference/tickers/{ticker}` NO devuelve `market_cap` para tickers inactivos/delistados.**  
>Esto significa:
>- [X] Imposible filtrar inactivos por market_cap historico
>- [X] Si solo usamos activos < $2B -> **SURVIVORSHIP BIAS SEVERO**
>- [X] Perdemos 3,300 tickers delistados (los MAS importantes para entrenar pump & dump terminal)

* **Objetivo**: Filtrar la población target de Small Caps (market cap < $2B) preservando TODOS los tickers inactivos para eliminar survivorship bias. Este filtro dual aplica el umbral de capitalización SOLO a activos, mientras mantiene el 100% de inactivos como contrapartes históricas necesarias para ML.
* **Fuente de datos**: Universo enriquecido CS+XNAS/XNYS (8,307 tickers con market_cap y delisted_utc)
* **Script**: [scripts/filter_smallcaps_population.py](../../scripts/filter_smallcaps_population.py)
* **Input**: `processed/universe/hybrid_enriched_2025-11-01.parquet` (8,307 tickers enriquecidos)
* **Estrategia dual de filtrado**:
    - **ACTIVOS**: `market_cap < $2,000,000,000` → 3,105 tickers
    - **INACTIVOS**: **SIN FILTRO** (todos preservados) → 3,300 tickers
* **Resultado**:
    - **Total población target**: 6,405 tickers (77.1% retención)
    - **Small Caps activos**: 3,105 (48.5%)
        - Micro caps (< $300M): 1,747 (56.3%)
        - Small caps ($300M-$1B): 863 (27.8%)
        - Mid-small caps ($1B-$2B): 495 (15.9%)
    - **Inactivos preservados**: 3,300 (51.5%)
* **Output**: `processed/universe/smallcaps_universe_2025-11-01.parquet` (6,405 tickers, 25 columnas)

```sh
D:\TSIS_SmallCaps\
├── scripts/
│   ├── download_universe.py                   # Paso 1: Descarga snapshot
│   ├── filter_universe_2019_2025.py           # Paso 2: Filtro temporal
│   ├── filter_universe_cs_exchanges.py        # Paso 3: Filtro CS+XNAS/XNYS
│   ├── enrich_ticker_details.py               # Paso 4a: Descarga ticker details
│   ├── create_hybrid_enriched_universe.py     # Paso 4b: Merge dual
│   └── filter_smallcaps_population.py         # Paso 5: Filtro Small Caps < $2B
│
├── raw/polygon/reference/
│   ├── tickers_snapshot/
│   │   └── snapshot_date=2025-11-01/          # UNIVERSO COMPLETO (Paso 1)
│   │       ├── tickers_all.parquet            # 34,324 tickers (activos + inactivos)
│   │       ├── tickers_active.parquet         # 11,899 tickers (solo activos)
│   │       ├── tickers_inactive.parquet       # 22,425 tickers (solo inactivos)
│   │       └── summary.csv
│   │
│   └── ticker_details/
│       └── as_of_date=2025-11-01/             # TICKER DETAILS (Paso 4a)
│           └── details.parquet                # 8,307 tickers procesados
│                                              # Con datos: 5,204 (activos)
│                                              # Not found (404): 3,103 (inactivos)
│
├── processed/universe/
│   ├── tickers_2019_2025.parquet              # FILTRO TEMPORAL (Paso 2)
│   │                                          # 20,471 tickers (listados 2019-2025)
│   │                                          # Activos: 11,897 | Inactivos: 8,574
│   ├── tickers_2019_2025.csv
│   │
│   ├── tickers_2019_2025_cs_exchanges.parquet # FILTRO CS + XNAS/XNYS (Paso 3)
│   │                                          # 8,307 tickers (CS en NASDAQ/NYSE)
│   │                                          # Activos: 5,007 | Inactivos: 3,300
│   │                                          # XNAS: 5,619 | XNYS: 2,688
│   ├── tickers_2019_2025_cs_exchanges.csv
│   │
│   ├── hybrid_enriched_2025-11-01.parquet     # ENRIQUECIMIENTO DUAL (Paso 4b)
│   │                                          # 8,307 tickers enriquecidos (25 columnas)
│   │                                          # ACTIVOS (5,007): 97.5% con market_cap
│   │                                          # INACTIVOS (3,300): 100% con delisted_utc
│   │
│   └── smallcaps_universe_2025-11-01.parquet  # POBLACIÓN TARGET (Paso 5)
│                                              # 6,405 tickers Small Caps
│                                              # ┌─ ACTIVOS: 3,105 (market_cap < $2B)
│                                              # │   
│                                              # │   
│                                              # │  
│                                              # └─ INACTIVOS: 3,300 (TODOS preservados)
│                                              #     → ANTI-SURVIVORSHIP BIAS
│
```

**Comando de ejecución**:
```bash
python scripts/filter_smallcaps_population.py \
  --input processed/universe/hybrid_enriched_2025-11-01.parquet \
  --output processed/universe/smallcaps_universe_2025-11-01.parquet \
  --market-cap-threshold 2000000000
```

```sh
====================================================================================================
VERIFICACIÓN: SMALL CAPS TARGET POPULATION (< $2B) - ANÁLISIS DETALLADO
====================================================================================================

📊 RESUMEN GENERAL
   Total tickers: 6,405
   Total columnas: 25

   Small Caps activos: 3,105 (48.5%)
   Inactivos preservados: 3,300 (51.5%)

====================================================================================================
📊 HEAD(2) - TODAS LAS COLUMNAS (Small Cap ACTIVO + Small Cap ACTIVO)
====================================================================================================

shape: (25, 3)
shape: (25, 3)
┌────────────────────────────────┬─────────────────────────────────┬─────────────────────────────────┐
│ column                         ┆ column_0                        ┆ column_1                        │
╞════════════════════════════════╪═════════════════════════════════╪═════════════════════════════════╡
│ ticker                         ┆ AACB                            ┆ AAM                             │
│ name                           ┆ Artius II Acquisition Inc. Cla… ┆ AA Mission Acquisition Corp.    │
│ market                         ┆ stocks                          ┆ stocks                          │
│ locale                         ┆ us                              ┆ us                              │
│ primary_exchange               ┆ XNAS                            ┆ XNYS                            │
│ type                           ┆ CS                              ┆ CS                              │
│ active                         ┆ true                            ┆ true                            │
│ currency_name                  ┆ usd                             ┆ usd                             │
│ cik                            ┆ 0002034334                      ┆ 0002012964                      │
│ composite_figi                 ┆ null                            ┆ null                            │
│ share_class_figi               ┆ null                            ┆ null                            │
│ last_updated_utc               ┆ 2025-11-01T06:07:02.287919033Z  ┆ 2025-11-01T06:07:02.287920616Z  │
│ snapshot_date                  ┆ 2025-11-01                      ┆ 2025-11-01                      │
│ delisted_utc                   ┆ null                            ┆ null                            │
│ delisted_date                  ┆ null                            ┆ null                            │
│ market_cap                     ┆ 283181670.0                     ┆ 466124400.0                     │
│ weighted_shares_outstanding    ┆ 27675000                        ┆ 43974000                        │
│ share_class_shares_outstanding ┆ 23650000                        ┆ 43974000                        │
│ total_employees                ┆ null                            ┆ null                            │
│ description                    ┆ Artius II Acquisition Inc is a… ┆ AA Mission Acquisition Corp is… │
│ sic_code                       ┆ 6770                            ┆ 6770                            │
│ sic_description                ┆ BLANK CHECKS                    ┆ BLANK CHECKS                    │
│ homepage_url                   ┆ null                            ┆ https://aamission.net           │
│ phone_number                   ┆ 212 309 7668                    ┆ 832-336-8887                    │
│ address                        ┆ {3 COLUMBUS CIRCLE,SUITE 22…    ┆ null                            │
└────────────────────────────────┴─────────────────────────────────┴─────────────────────────────────┘

====================================================================================================
📊 HEAD(2) - Solo columnas clave:
====================================================================================================

shape: (7, 3)
shape: (7, 3)
┌──────────────────┬─────────────────────────────────┬──────────────────────────────┐
│ column           ┆ column_0                        ┆ column_1                     │
╞══════════════════╪═════════════════════════════════╪══════════════════════════════╡
│ ticker           ┆ AACB                            ┆ AAM                          │
│ name             ┆ Artius II Acquisition Inc. Cla… ┆ AA Mission Acquisition Corp. │
│ market_cap       ┆ 283181670.0                     ┆ 466124400.0                  │
│ primary_exchange ┆ XNAS                            ┆ XNYS                         │
│ active           ┆ true                            ┆ true                         │
│ type             ┆ CS                              ┆ CS                           │
│ delisted_utc     ┆ null                            ┆ null                         │
└──────────────────┴─────────────────────────────────┴──────────────────────────────┘

====================================================================================================
📊 COMPLETITUD DE CAMPOS:
====================================================================================================

ticker                             :  6,405 /  6,405 (100.0%)
name                               :  6,405 /  6,405 (100.0%)
market                             :  6,405 /  6,405 (100.0%)
locale                             :  6,405 /  6,405 (100.0%)
primary_exchange                   :  6,405 /  6,405 (100.0%)
type                               :  6,405 /  6,405 (100.0%)
active                             :  6,405 /  6,405 (100.0%)
currency_name                      :  6,405 /  6,405 (100.0%)
cik                                :  6,309 /  6,405 ( 98.5%)
composite_figi                     :  4,429 /  6,405 ( 69.1%)
share_class_figi                   :  4,429 /  6,405 ( 69.1%)
last_updated_utc                   :  6,405 /  6,405 (100.0%)
snapshot_date                      :  6,405 /  6,405 (100.0%)
delisted_utc                       :  3,300 /  6,405 ( 51.5%)
delisted_date                      :  3,300 /  6,405 ( 51.5%)
market_cap                         :  3,105 /  6,405 ( 48.5%)
weighted_shares_outstanding        :  3,105 /  6,405 ( 48.5%)
share_class_shares_outstanding     :  2,912 /  6,405 ( 45.5%)
total_employees                    :  2,743 /  6,405 ( 42.8%)
description                        :  3,304 /  6,405 ( 51.6%)
sic_code                           :  2,476 /  6,405 ( 38.7%)
sic_description                    :  2,461 /  6,405 ( 38.4%)
homepage_url                       :  2,977 /  6,405 ( 46.5%)
phone_number                       :  2,516 /  6,405 ( 39.3%)
address                            :  2,514 /  6,405 ( 39.3%)

====================================================================================================
📊 COMPARACIÓN: SMALL CAP ACTIVO vs INACTIVO
====================================================================================================

EJEMPLO 1: Small Cap ACTIVO (< $2B)
shape: (25, 2)
┌────────────────────────────────┬─────────────────────────────────┐
│ column                         ┆ column_0                        │
╞════════════════════════════════╪═════════════════════════════════╡
│ ticker                         ┆ AACB                            │
│ name                           ┆ Artius II Acquisition Inc. Cla… │
│ market                         ┆ stocks                          │
│ locale                         ┆ us                              │
│ primary_exchange               ┆ XNAS                            │
│ type                           ┆ CS                              │
│ active                         ┆ true                            │
│ currency_name                  ┆ usd                             │
│ cik                            ┆ 0002034334                      │
│ composite_figi                 ┆ null                            │
│ share_class_figi               ┆ null                            │
│ last_updated_utc               ┆ 2025-11-01T06:07:02.287919033Z  │
│ snapshot_date                  ┆ 2025-11-01                      │
│ delisted_utc                   ┆ null                            │
│ delisted_date                  ┆ null                            │
│ market_cap                     ┆ 283181670.0                     │
│ weighted_shares_outstanding    ┆ 27675000                        │
│ share_class_shares_outstanding ┆ 23650000                        │
│ total_employees                ┆ null                            │
│ description                    ┆ Artius II Acquisition Inc is a… │
│ sic_code                       ┆ 6770                            │
│ sic_description                ┆ BLANK CHECKS                    │
│ homepage_url                   ┆ null                            │
│ phone_number                   ┆ 212 309 7668                    │
│ address                        ┆ 3 COLUMBUS CIRCLE,SUITE 22…     │
└────────────────────────────────┴─────────────────────────────────┘

====================================================================================================
EJEMPLO 2: INACTIVO PRESERVADO (delisted)
shape: (25, 2)
┌────────────────────────────────┬─────────────────────────────┐
│ column                         ┆ column_0                    │
╞════════════════════════════════╪═════════════════════════════╡
│ ticker                         ┆ AABA                        │
│ name                           ┆ Altaba Inc. Common Stock    │
│ market                         ┆ stocks                      │
│ locale                         ┆ us                          │
│ primary_exchange               ┆ XNAS                        │
│ type                           ┆ CS                          │
│ active                         ┆ false                       │
│ currency_name                  ┆ usd                         │
│ cik                            ┆ 0001011006                  │
│ composite_figi                 ┆ BBG000KB2D74                │
│ share_class_figi               ┆ BBG001S8V781                │
│ last_updated_utc               ┆ 2024-12-03T21:33:22.821052Z │
│ snapshot_date                  ┆ 2025-11-01                  │
│ delisted_utc                   ┆ 2019-10-07T04:00:00Z        │
│ delisted_date                  ┆ 2019-10-07                  │
│ market_cap                     ┆ null                        │
│ weighted_shares_outstanding    ┆ null                        │
│ share_class_shares_outstanding ┆ null                        │
│ total_employees                ┆ null                        │
│ description                    ┆ null                        │
│ sic_code                       ┆ null                        │
│ sic_description                ┆ null                        │
│ homepage_url                   ┆ null                        │
│ phone_number                   ┆ null                        │
│ address                        ┆ null                        │
└────────────────────────────────┴─────────────────────────────┘

====================================================================================================
DISTRIBUCIÓN POR MARKET CAP - SMALL CAPS ACTIVOS
====================================================================================================

Distribución por rangos:
   Micro caps (< $300M)           1,747 ( 56.3%)
   Small caps ($300M-$1B)           863 ( 27.8%)
   Mid-small caps ($1B-$2B)         495 ( 15.9%)

====================================================================================================
DISTRIBUCIÓN POR AÑO DE DELISTING - INACTIVOS
====================================================================================================

Inactivos con delisted_utc: 3,300 / 3,300 (100.0%)

Distribución por año de delisting:
   2019: 351
   2020: 327
   2021: 467
   2022: 506
   2023: 696
   2024: 537
   2025: 416

====================================================================================================
✅ RESUMEN FINAL - POBLACIÓN TARGET SMALL CAPS
====================================================================================================

Total tickers: 6,405
   • Small Caps activos (< $2B): 3,105 (48.5%)
   • Inactivos preservados: 3,300 (51.5%)
   • Ratio activos/inactivos: 0.94

Anti-survivorship bias: ✅ APLICADO
Market cap range: $512,354,312 - $673,058,430

Distribución por exchange:
   • NASDAQ (XNAS): 4,814 (75.2%)
   • NYSE (XNYS): 1,591 (24.8%)

Status: ✅ LISTO PARA FASE B (Ingesta Daily/Minute)
```

## Paso 6: Ingesta Global de Splits & Dividends

### 6a. Descarga Global (Sin Filtros)

* **Objetivo**: Descargar eventos corporativos históricos GLOBALES (splits, dividends) para posterior ajuste de precios y feature engineering ML.
* **Script**: [scripts/ingest_splits_dividends.py](../../scripts/ingest_splits_dividends.py)
* **Fuente de datos**: Polygon `/v3/reference/splits` y `/v3/reference/dividends` (sin filtros)
* **Output**:
  - `raw/polygon/reference/splits/year=*/splits.parquet`
  - `raw/polygon/reference/dividends/year=*/dividends.parquet`

**IMPORTANTE**: Esta es una descarga GLOBAL de todos los eventos corporativos disponibles en Polygon. Los datos se filtrarán posteriormente para nuestro universo de 6,405 tickers Small Caps.

**Características**:
- Paginación eficiente con manejo de cursores
- Rate limiting con reintentos automáticos
- Particionado por año (execution_date para splits, ex_dividend_date para dividends)
- Limpieza automática: eliminación de duplicados, cálculo de ratios

**Comando de ejecución**:

```bash
python scripts/ingest_splits_dividends.py --outdir raw/polygon/reference
```

**Datos descargados**:

**SPLITS** (26,696 registros):
- **Período**: 1978-2025 (48 años)
- **Tickers únicos**: 18,454
- **Campos clave**: `ticker`, `execution_date`, `split_from`, `split_to`, `ratio`, `declared_date`

**DIVIDENDS** (~1.8M registros):
- **Período**: Histórico completo
- **Tickers únicos**: Miles
- **Campos clave**: `ticker`, `ex_dividend_date`, `cash_amount`, `declaration_date`, `record_date`, `payable_date`, `frequency`, `dividend_type`

---

### 6b. Filtrado por Universo Small Caps

* **Objetivo**: Filtrar splits y dividends globales para quedarnos SOLO con los eventos de nuestro universo de 6,405 tickers Small Caps.
* **Script**: [scripts/filter_splits_dividends_universe.py](../../scripts/filter_splits_dividends_universe.py)
* **Input**:
  - Universo: `processed/universe/smallcaps_universe_2025-11-01.parquet` (6,405 tickers)
  - Splits globales: `raw/polygon/reference/splits/year=*/`
  - Dividends globales: `raw/polygon/reference/dividends/year=*/`
* **Output**:
  - `processed/corporate_actions/splits/year=*/splits.parquet` (filtrado)
  - `processed/corporate_actions/dividends/year=*/dividends.parquet` (filtrado)
  - `processed/corporate_actions/summary.csv` (estadísticas)

**Comando de ejecución**:

```bash
python scripts/filter_splits_dividends_universe.py \
    --universe processed/universe/smallcaps_universe_2025-11-01.parquet \
    --splits-dir raw/polygon/reference/splits \
    --dividends-dir raw/polygon/reference/dividends \
    --output-dir processed/corporate_actions
```

**Resultado esperado**:
- Reducción significativa del volumen de datos (solo eventos relevantes para nuestro universo)
- Misma estructura particionada por año para eficiencia
- Estadísticas detalladas: % retenido, tickers con splits/dividends, distribuciones

```sh
D:\TSIS_SmallCaps\
├── scripts/
│   ├── download_universe.py                      # Paso 1: Descarga snapshot
│   ├── filter_universe_2019_2025.py              # Paso 2: Filtro temporal
│   ├── filter_universe_cs_exchanges.py           # Paso 3: Filtro CS+XNAS/XNYS
│   ├── enrich_ticker_details.py                  # Paso 4a: Descarga ticker details
│   ├── create_hybrid_enriched_universe.py        # Paso 4b: Merge dual
│   ├── filter_smallcaps_population.py            # Paso 5: Filtro Small Caps < $2B
│   ├── ingest_splits_dividends.py                # Paso 6a: Descarga global splits/dividends
│   └── filter_splits_dividends_universe.py       # Paso 6b: Filtrar por universo
│
├── raw/polygon/reference/
│   ├── tickers_snapshot/
│   │   └── snapshot_date=2025-11-01/             # UNIVERSO COMPLETO (Paso 1)
│   │       ├── tickers_all.parquet               # 34,324 tickers (activos + inactivos)
│   │       ├── tickers_active.parquet            # 11,899 tickers (solo activos)
│   │       ├── tickers_inactive.parquet          # 22,425 tickers (solo inactivos)
│   │       └── summary.csv
│   │
│   ├── ticker_details/
│   │   └── as_of_date=2025-11-01/                # TICKER DETAILS (Paso 4a)
│   │       └── details.parquet                   # 8,307 tickers procesados
│   │                                             # Con datos: 5,204 (activos)
│   │                                             # Not found (404): 3,103 (inactivos)
│   │
│   ├── splits/
│   │   └── year=*/                               # SPLITS GLOBALES (Paso 6a)
│   │       └── splits.parquet                    # 26,696 splits (1978-2025)
│   │                                             # 18,454 tickers únicos
│   │
│   └── dividends/
│       └── year=*/                               # DIVIDENDS GLOBALES (Paso 6a)
│           └── dividends.parquet                 # ~1.8M dividends (histórico)
│                                                 # Miles de tickers únicos
│
├── processed/
│   ├── universe/
│   │   ├── tickers_2019_2025.parquet             # FILTRO TEMPORAL (Paso 2)
│   │   │                                         # 20,471 tickers (listados 2019-2025)
│   │   │                                         # Activos: 11,897 | Inactivos: 8,574
│   │   ├── tickers_2019_2025.csv
│   │   │
│   │   ├── tickers_2019_2025_cs_exchanges.parquet # FILTRO CS + XNAS/XNYS (Paso 3)
│   │   │                                         # 8,307 tickers (CS en NASDAQ/NYSE)
│   │   │                                         # Activos: 5,007 | Inactivos: 3,300
│   │   │                                         # XNAS: 5,619 | XNYS: 2,688
│   │   ├── tickers_2019_2025_cs_exchanges.csv
│   │   │
│   │   ├── hybrid_enriched_2025-11-01.parquet    # ENRIQUECIMIENTO DUAL (Paso 4b)
│   │   │                                         # 8,307 tickers enriquecidos (25 columnas)
│   │   │                                         # ACTIVOS (5,007): 97.5% con market_cap
│   │   │                                         # INACTIVOS (3,300): 100% con delisted_utc
│   │   │
│   │   └── smallcaps_universe_2025-11-01.parquet # POBLACIÓN TARGET (Paso 5)
│   │                                             # 6,405 tickers Small Caps
│   │                                             # ┌─ ACTIVOS: 3,105 (market_cap < $2B)
│   │                                             # │   • Micro caps (< $300M): 1,747 (56.3%)
│   │                                             # │   • Small caps ($300M-$1B): 863 (27.8%)
│   │                                             # │   • Mid-small caps ($1B-$2B): 495 (15.9%)
│   │                                             # └─ INACTIVOS: 3,300 (TODOS preservados)
│   │                                             #     → ANTI-SURVIVORSHIP BIAS
│   │
│   └── corporate_actions/
│       ├── splits/
│       │   └── year=*/                           # SPLITS FILTRADOS (Paso 6b)
│       │       └── splits.parquet                # Solo 6,405 tickers Small Caps
│       │
│       ├── dividends/
│       │   └── year=*/                           # DIVIDENDS FILTRADOS (Paso 6b)
│       │       └── dividends.parquet             # Solo 6,405 tickers Small Caps
│       │
│       └── summary.csv                           # Estadísticas de filtrado
```

---



## notebook resultados

* [Resultados jupyter notebook](./00_universe.ipynb)

