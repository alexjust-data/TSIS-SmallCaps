# Auditoría Optimizada - Datos Polygon/Massive

**Fecha:** 2025-12-03 09:41:32

## ⚠️ Nota sobre Metodología

Esta auditoría utiliza **muestreo inteligente** para datasets masivos:
- Analiza estructura completa de directorios (primeros 2-3 niveles)
- Muestrea archivos representativos de cada tipo
- Proporciona estimaciones estadísticas cuando hay millones de archivos
- Genera código ejecutable para cada tipo de dato encontrado

---

## Resumen Ejecutivo

Ubicaciones analizadas:
- `C:\TSIS_Data`
- `D:\TSIS_SmallCaps\raw\polygon`

---

## 📁 C:\TSIS_Data

**Archivos estimados:** ~33,340
**Tamaño estimado:** ~120.89 MB
**Directorios escaneados:** 56
**Subdirectorios totales:** 17,587

### Tipos de Archivo Encontrados

| Extensión | Cantidad | Ejemplo de Archivo |
|-----------|----------|--------------------|
| `.json` | 5 | `.cache_intraday.json` |
| `.log` | 5 | `trades_download.log` |
| `.parquet` | 4 | `splits.parquet` |
| `(sin ext)` | 1 | `nul` |

### Estructura de Directorios (Muestra)

#### 📂 `[RAÍZ]`

**Archivos:** 0 | **Subdirectorios:** 14

**Subdirectorios:** `additional`, `fundamentals`, `ohlcv_intraday_1m`, `quotes_2004_2018`, `quotes_2029_2025`, `quotes_p95_2004_2018`, `quotes_p95_2019_2025`, `quotes_test`, `reference`, `regime_indicators` _(+4 más)_

#### 📂 `additional`

**Archivos:** 0 | **Subdirectorios:** 4

**Subdirectorios:** `corporate_actions`, `economic`, `ipos`, `news`

#### 📂 `fundamentals`

**Archivos:** 0 | **Subdirectorios:** 5

**Subdirectorios:** `balance_sheets`, `cash_flow_statements`, `financial_ratios`, `income_statements`, `smallcap_ratios`

#### 📂 `ohlcv_intraday_1m`

**Archivos:** 0 | **Subdirectorios:** 2

**Subdirectorios:** `2004_2018`, `2019_2025`

#### 📂 `quotes_2004_2018`

#### 📂 `quotes_2029_2025`

#### 📂 `quotes_p95_2004_2018`

**Archivos:** 1 | **Subdirectorios:** 2,187

**Subdirectorios:** `AAI`, `AAT`, `AAV`, `ABAT`, `ABAX`, `ABCD`, `ABCO`, `ABH`, `ABK`, `ABN` _(+2177 más)_

**Archivos (muestra):**

- `.quotes_cache.json` (335.00 B)

#### 📂 `quotes_p95_2019_2025`

**Archivos:** 1 | **Subdirectorios:** 4,620

**Subdirectorios:** `AABA`, `AAC`, `AACI`, `AACQ`, `AACT`, `AAGR`, `AAIC`, `AAM`, `AAME`, `AAN` _(+4610 más)_

**Archivos (muestra):**

- `.quotes_cache.json` (11.39 KB)

#### 📂 `quotes_test`

**Archivos:** 1 | **Subdirectorios:** 1

**Subdirectorios:** `VSTS`

**Archivos (muestra):**

- `.quotes_cache.json` (61.00 B)

#### 📂 `reference`

**Archivos:** 4 | **Subdirectorios:** 0

**Archivos (muestra):**

- `condition_codes.parquet` (5.44 KB)
- `exchanges.parquet` (5.20 KB)
- `market_status_upcoming.parquet` (2.75 KB)
- `ticker_types.parquet` (1.89 KB)

#### 📂 `regime_indicators`

**Archivos:** 2 | **Subdirectorios:** 2

**Subdirectorios:** `etfs`, `indices`

**Archivos (muestra):**

- `download_metadata.json` (71.00 B)
- `ticker_ranges.json` (4.03 KB)

#### 📂 `short_data`

**Archivos:** 0 | **Subdirectorios:** 2

**Subdirectorios:** `short_interest`, `short_volume`

#### 📂 `trades_ticks`

**Archivos:** 3 | **Subdirectorios:** 961

**Subdirectorios:** `BOSC`, `BRCC`, `BRPA`, `BRPM`, `BSGA`, `BSQR`, `BTAQ`, `BTCS`, `BTCT`, `BTM` _(+951 más)_

**Archivos (muestra):**

- `nul` (0.00 B)
- `trades_download.log` (26.88 KB)
- `trades_download.partial.log` (636.00 B)

#### 📂 `trades_ticks_2004_2018`

**Archivos:** 1 | **Subdirectorios:** 3,478

**Subdirectorios:** `AACB`, `AAI`, `AAME`, `AAN.A`, `AAPC`, `AAT`, `AAV`, `ABAT`, `ABAX`, `ABCD` _(+3468 más)_

**Archivos (muestra):**

- `trades_download.partial.log` (1.09 KB)

#### 📂 `trades_ticks_2019_2025`

**Archivos:** 2 | **Subdirectorios:** 6,311

**Subdirectorios:** `AABA`, `AAC`, `AACB`, `AACI`, `AACQ`, `AACT`, `AAGR`, `AAIC`, `AAM`, `AAME` _(+6301 más)_

**Archivos (muestra):**

- `trades_download.log` (140.67 KB)
- `trades_download.partial.log` (636.00 B)

  #### 📂 `additional\corporate_actions`

  **Archivos:** 2 | **Subdirectorios:** 0

  **Archivos (muestra):**

  - `splits.parquet` (1.13 MB)
  - `ticker_events.parquet` (33.89 KB)

  #### 📂 `additional\economic`

  **Archivos:** 3 | **Subdirectorios:** 0

  **Archivos (muestra):**

  - `inflation.parquet` (10.55 KB)
  - `inflation_expectations.parquet` (13.04 KB)
  - `treasury_yields.parquet` (64.10 KB)

  #### 📂 `additional\ipos`

  **Archivos:** 1 | **Subdirectorios:** 0

  **Archivos (muestra):**

  - `all_ipos.parquet` (161.46 KB)

  #### 📂 `additional\news`

  **Archivos:** 82 | **Subdirectorios:** 0

  **Archivos (muestra):**

  - `news_batch_0000.parquet` (1.16 MB)
  - `news_batch_0001.parquet` (1.11 MB)
  - `news_batch_0002.parquet` (1.17 MB)
  - `news_batch_0003.parquet` (2.47 MB)
  - `news_batch_0004.parquet` (1.21 MB)

  #### 📂 `fundamentals\balance_sheets`

  **Archivos:** 5,622 | **Subdirectorios:** 0

  **Archivos (muestra):**

  - `AAC.parquet` (15.72 KB)
  - `AACQ.parquet` (12.09 KB)
  - `AACT.parquet` (11.79 KB)
  - `AAGR.parquet` (11.38 KB)
  - `AAIC.parquet` (11.32 KB)


## 📁 D:\TSIS_SmallCaps\raw\polygon

**Archivos estimados:** ~2
**Tamaño estimado:** ~350.68 KB
**Directorios escaneados:** 24
**Subdirectorios totales:** 15,542

### Tipos de Archivo Encontrados

| Extensión | Cantidad | Ejemplo de Archivo |
|-----------|----------|--------------------|
| `.log` | 2 | `daily_download.log` |

### Estructura de Directorios (Muestra)

#### 📂 `[RAÍZ]`

**Archivos:** 0 | **Subdirectorios:** 4

**Subdirectorios:** `reference`, `ohlcv_daily`, `ohlcv_intraday_1m`, `trades_ticks`

#### 📂 `reference`

**Archivos:** 0 | **Subdirectorios:** 4

**Subdirectorios:** `tickers_snapshot`, `ticker_details`, `splits`, `dividends`

#### 📂 `ohlcv_daily`

**Archivos:** 1 | **Subdirectorios:** 8,174

**Subdirectorios:** `GLS`, `EGGF`, `JCIC`, `GLADZ`, `CHS`, `ZYNE`, `WVVI`, `EVER`, `INN`, `SNDL` _(+8164 más)_

**Archivos (muestra):**

- `daily_download.log` (75.99 KB)

#### 📂 `ohlcv_intraday_1m`

**Archivos:** 1 | **Subdirectorios:** 6,297

**Subdirectorios:** `_batch_temp`, `DHX`, `LMRK`, `OXM`, `XEC`, `NWL`, `TELA`, `NBST`, `IMG`, `IGIC` _(+6287 más)_

**Archivos (muestra):**

- `minute_download.log` (274.69 KB)

#### 📂 `trades_ticks`

**Archivos:** 0 | **Subdirectorios:** 1,063

**Subdirectorios:** `_batch_temp`, `VMAR`, `EPSM`, `WAFU`, `FFIC`, `IMTX`, `NAUT`, `AGAC`, `PVAC`, `EB` _(+1053 más)_

  #### 📂 `reference\tickers_snapshot`

  **Archivos:** 0 | **Subdirectorios:** 1

  **Subdirectorios:** `snapshot_date=2025-11-01`

  #### 📂 `reference\ticker_details`

  **Archivos:** 0 | **Subdirectorios:** 1

  **Subdirectorios:** `as_of_date=2025-11-01`

  #### 📂 `reference\splits`

  **Archivos:** 0 | **Subdirectorios:** 31

  **Subdirectorios:** `year=2025`, `year=2003`, `year=2002`, `year=2020`, `year=2010`, `year=2012`, `year=2017`, `year=1980`, `year=2023`, `year=1979` _(+21 más)_

  #### 📂 `reference\dividends`

  **Archivos:** 0 | **Subdirectorios:** 31

  **Subdirectorios:** `year=2027`, `year=2006`, `year=2004`, `year=2024`, `year=2005`, `year=2015`, `year=2002`, `year=2029`, `year=2007`, `year=2011` _(+21 más)_

  #### 📂 `ohlcv_daily\LYLT`

  **Archivos:** 0 | **Subdirectorios:** 3

  **Subdirectorios:** `year=2022`, `year=2023`, `year=2021`

  #### 📂 `ohlcv_daily\HUB.B`

  **Archivos:** 0 | **Subdirectorios:** 12

  **Subdirectorios:** `year=2015`, `year=2014`, `year=2013`, `year=2007`, `year=2005`, `year=2004`, `year=2008`, `year=2012`, `year=2006`, `year=2009` _(+2 más)_

  #### 📂 `ohlcv_daily\MOT`

  **Archivos:** 0 | **Subdirectorios:** 8

  **Subdirectorios:** `year=2004`, `year=2011`, `year=2009`, `year=2010`, `year=2006`, `year=2008`, `year=2007`, `year=2005`

  #### 📂 `ohlcv_daily\LMS`

  **Archivos:** 0 | **Subdirectorios:** 4

  **Subdirectorios:** `year=2004`, `year=2006`, `year=2005`, `year=2007`

  #### 📂 `ohlcv_daily\RKLY`

  **Archivos:** 0 | **Subdirectorios:** 3

  **Subdirectorios:** `year=2023`, `year=2021`, `year=2022`

  #### 📂 `ohlcv_intraday_1m\ACAM`

  **Archivos:** 0 | **Subdirectorios:** 3

  **Subdirectorios:** `year=2019`, `year=2020`, `year=2021`

  #### 📂 `ohlcv_intraday_1m\KORE`

  **Archivos:** 0 | **Subdirectorios:** 5

  **Subdirectorios:** `year=2021`, `year=2022`, `year=2023`, `year=2024`, `year=2025`

  #### 📂 `ohlcv_intraday_1m\CENQ`

  **Archivos:** 0 | **Subdirectorios:** 3

  **Subdirectorios:** `year=2021`, `year=2022`, `year=2023`

  #### 📂 `ohlcv_intraday_1m\BIOA`

  **Archivos:** 0 | **Subdirectorios:** 2

  **Subdirectorios:** `year=2024`, `year=2025`

  #### 📂 `ohlcv_intraday_1m\JSYN`

  **Archivos:** 0 | **Subdirectorios:** 1

  **Subdirectorios:** `year=2019`

  #### 📂 `trades_ticks\HUDI`

  **Archivos:** 0 | **Subdirectorios:** 5

  **Subdirectorios:** `year=2021`, `year=2022`, `year=2023`, `year=2024`, `year=2025`


---

## 💻 Código para Acceder a los Datos

### Por Tipo de Archivo

#### Archivos `.log` (encontrados: 7)

```python
# Read as binary
with open(r'C:\TSIS_Data\trades_ticks\trades_download.log', 'rb') as f:
    data = f.read()
print(f"Size: {len(data)} bytes")
```

#### Archivos `.json` (encontrados: 5)

```python
import json
import pandas as pd

# Read JSON
with open(r'C:\TSIS_Data\ohlcv_intraday_1m\2004_2018\.cache_intraday.json', 'r') as f:
    data = json.load(f)
print(type(data))
print(list(data.keys()) if isinstance(data, dict) else len(data))

# Or with pandas
df = pd.read_json(r'C:\TSIS_Data\ohlcv_intraday_1m\2004_2018\.cache_intraday.json')
print(df.head())
```

#### Archivos `.parquet` (encontrados: 4)

```python
import pandas as pd
import pyarrow.parquet as pq

# Read parquet file
df = pd.read_parquet(r'C:\TSIS_Data\additional\corporate_actions\splits.parquet')
print(f"Shape: {df.shape}")
print(f"Columns: {df.columns.tolist()}")
print(df.head())

# Check schema
table = pq.read_table(r'C:\TSIS_Data\additional\corporate_actions\splits.parquet')
print(table.schema)
```

#### Archivos `` (encontrados: 1)

```python
# Read as binary
with open(r'C:\TSIS_Data\trades_ticks\nul', 'rb') as f:
    data = f.read()
print(f"Size: {len(data)} bytes")
```

---

## 📊 Resumen Global

**Total estimado de archivos:** ~33,342
**Tamaño total estimado:** ~121.24 MB
**Tipos de archivo diferentes:** 4

### Distribución por Extensión

| Extensión | Cantidad Estimada |
|-----------|-------------------|
| `.log` | 7 |
| `.json` | 5 |
| `.parquet` | 4 |
| `(sin ext)` | 1 |


---

## 📝 Notas Técnicas

- **Metodología:** Muestreo estratificado para datasets masivos
- **Precisión:** Las estimaciones se basan en muestras representativas
- **Código:** Todo el código proporcionado es ejecutable en Jupyter Notebook
- **Actualización:** 2025-12-03 09:41:32

---

*Generado por audit_polygon_data_optimized.py*
