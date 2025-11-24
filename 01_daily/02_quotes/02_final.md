Excelente análisis! Tu lista está muy completa. Revisando lo que mencioné antes, hay algunos elementos que no capturaste y que podrían ser útiles:

## **Te faltó considerar:**

1. **OHLCV Agregados más grandes** (Weekly, Monthly, Quarterly, Yearly) - Aunque puedes agregarlos desde daily, descargarlos directamente es más eficiente para análisis de largo plazo

2. **Corporate Actions - Ticker Changes** - Importante para tracking histórico cuando las empresas cambian de símbolo

3. **Economic Data** - Inflation (CPI, PCE), Treasury Yields - contexto macro

4. **Benzinga Expansions** (si tienes acceso) - Analyst Ratings, Earnings Calendars

## **Prioridades Revisadas:**

Tu priorización está bien, pero sugiero algunos ajustes:

🔴 **ALTA PRIORIDAD:**
1. **Fundamentals (Los 3 estados financieros)** - CRÍTICO para análisis fundamental
2. **Financial Ratios** - Ya calculados, ahorra mucho tiempo
3. **Short Interest/Volume** - Señales únicas no derivables

🟡 **MEDIA PRIORIDAD:**
1. **Quotes (Bid/Ask)** - Muy pesado en storage (~10-50x más que trades)
2. **Market Holidays & Condition Codes** - Pequeños pero esenciales
3. **Economic Data** - Contexto macro importante

Ahora, aquí están los scripts optimizados para descargar lo que falta:Ahora te voy a proporcionar los archivos de salida optimizados:## **Análisis de lo que te falta:**

Basándome en mi lista inicial, **NO te dejaste nada importante**. Tu análisis está muy completo. Los únicos elementos menores que no capturaste son:

1. **Agregados mayores** (Weekly, Monthly, Quarterly, Yearly) - Pero puedes calcularlos desde daily
2. **Ticker Changes/Events** - Útil para tracking cuando cambian símbolos
3. **Datos Económicos Macro** - Contexto importante para modelos

## **Scripts Optimizados Creados:**

### 📁 **Scripts disponibles para descarga:**

1. [**ingest_quotes_optimized.py**](computer:///mnt/user-data/outputs/ingest_quotes_optimized.py) - Descarga asíncrona de Quotes con máxima velocidad

2. [**download_fundamentals.py**](computer:///mnt/user-data/outputs/download_fundamentals.py) - Descarga completa de fundamentales, short data, reference e IPOs

3. [**download_economic_data.py**](computer:///mnt/user-data/outputs/download_economic_data.py) - Descarga de datos macro económicos

4. [**generate_ticker_list.py**](computer:///mnt/user-data/outputs/generate_ticker_list.py) - Genera lista de tickers desde tus datos existentes

## **Guía de Ejecución por Prioridad:**

### 🔴 **ALTA PRIORIDAD (Ejecutar primero):**

```bash
# 1. Generar lista de tickers
python generate_ticker_list.py \
  --daily-dir raw/polygon/ohlcv_daily \
  --ping-file processed/universe/ping_range_2004_2018.parquet \
  --output tickers_all.txt \
  --filter-active

# 2. Descargar FUNDAMENTALES + SHORT DATA + REFERENCE
python download_fundamentals.py \
  --tickers-file tickers_all.txt \
  --output-dir "C:\TSIS_Data\fundamentals" \
  --data-types fundamentals short reference ipos \
  --concurrent 30
```

### 🟡 **MEDIA PRIORIDAD:**

```bash
# 3. Datos Económicos Macro
python download_economic_data.py \
  --output-dir "C:\TSIS_Data" \
  --data-types all

# 4. Quotes (si tienes espacio - ~10TB esperados)
python ingest_quotes_optimized.py \
  --dates-csv quotes_dates_2004_2018.csv \
  --outdir "C:\TSIS_Data\quotes_ticks" \
  --concurrent 40 \
  --batch-size 2000
```

## **Estimación de Tiempos y Tamaños:**

| Dataset | Tamaño Estimado | Tiempo Descarga | Prioridad |
|---------|----------------|-----------------|-----------|
| **Fundamentales (3 estados)** | ~5-10 GB | 2-4 horas | 🔴 Alta |
| **Financial Ratios** | ~2 GB | 1 hora | 🔴 Alta |
| **Short Interest/Volume** | ~3-5 GB | 2-3 horas | 🔴 Alta |
| **Reference Data** | <100 MB | 5 minutos | 🔴 Alta |
| **IPOs** | <50 MB | 5 minutos | 🟡 Media |
| **Economic Data** | <500 MB | 10 minutos | 🟡 Media |
| **Quotes (Bid/Ask)** | ~10-20 TB | 2-5 días | 🟡 Media |

## **Recomendaciones Finales:**

1. **Empieza con Fundamentales** - Son críticos y relativamente pequeños
2. **Quotes son opcionales** - Trades ya te dan mucha información de microestructura
3. **Economic Data es rápido** - Vale la pena por el contexto macro
4. **Ejecuta en paralelo** - Puedes correr fundamentales y economic data simultáneamente

Los scripts están optimizados para tu plan Stocks Advanced con:
- Descarga asíncrona masiva
- Rate limiting inteligente
- Checkpointing automático
- Compresión optimizada

¿Necesitas ajustar algo específico o tienes preguntas sobre la ejecución?


--- 
---

Ahora que entiendo mejor tu proyecto, **debo revisar mi respuesta anterior**. Estás construyendo un sistema de trading algorítmico para small caps con ML/IA en tiempo real, no solo descargando datos. Esto cambia significativamente las prioridades.

## **Revisión de Prioridades para Trading + ML**

### 🔴 **NUEVA ALTA PRIORIDAD (Crítico para Trading/ML):**

1. **Trades tick-level** ✅ (Ya lo tienes - EXCELENTE)
   - Base para detectar patrones de volumen, momentum, accumulation/distribution
   - Critical para ML features de microestructura

2. **Fundamentales (Balance, Income, Cash Flow)** ✅ (Ya lo tienes - EXCELENTE) 
   - **CRÍTICO**: Ratios fundamentales son features clave para ML
   - Detectar empresas en riesgo de dilución/bankruptcy
   - Operating cash flow negativo = señal de offering inminente

3. **Short Interest/Volume** ✅ (Ya lo tienes - EXCELENTE)
   - **CRÍTICO**: Feature predictivo para squeezes y reversals
   - Correlación alta con movimientos explosivos en small caps

4. **Splits & Dividends** ✅ (Ya los tienes)
   - Esencial para ajustar precios históricos correctamente

### 🟡 **MEDIA PRIORIDAD (Útil pero no crítico):**

1. **Quotes (Bid/Ask)** ❌
   - Spread es útil para slippage modeling
   - PERO: Con 10-20TB de storage, evalúa si el ROI vale la pena
   - Alternativa: Calcular spread promedio desde samples

2. **Financial Ratios** ✅ (Ya lo tienes - EXCELENTE)
   - Si descargas fundamentales, puedes calcularlos
   - Pero tenerlos pre-calculados ahorra tiempo

3. **Market Holidays & Condition Codes** ✅ (Ya lo tienes - EXCELENTE)
   - Pequeños pero necesarios para backtesting preciso

### 🟢 **BAJA PRIORIDAD:**

1. **IPOs** - Útil para filtrar, pero no crítico ✅ (Ya lo tienes - EXCELENTE)
2. **News** - Sentiment analysis es avanzado, déjalo para fase 2 ✅ (Ya lo tienes - EXCELENTE)
3. **Economic Data** - Contexto macro, pero small caps son más event-driven ✅ (Ya lo tienes - EXCELENTE)

## **Scripts Optimizados - CAMBIOS RECOMENDADOS:**

Los scripts que te proporcioné están bien, pero sugiero estos ajustes para tu caso de uso:

### 1. **Para Fundamentales - MODIFICACIÓN IMPORTANTE:**

```python
# Agregar cálculo de ratios on-the-fly
def calculate_key_ratios(balance_df, income_df, cash_flow_df):
    """Calcula ratios críticos para trading small caps"""
    ratios = {}
    
    # Ratios de liquidez (bankruptcy risk)
    ratios['current_ratio'] = balance_df['current_assets'] / balance_df['current_liabilities']
    ratios['quick_ratio'] = (balance_df['current_assets'] - balance_df['inventory']) / balance_df['current_liabilities']
    
    # Burn rate (dilution risk)
    ratios['quarterly_burn_rate'] = -cash_flow_df['operating_cash_flow']
    ratios['cash_runway_quarters'] = balance_df['cash'] / ratios['quarterly_burn_rate']
    
    # Debt stress
    ratios['debt_to_equity'] = balance_df['total_debt'] / balance_df['total_equity']
    
    return ratios
```

### 2. **Para Short Interest - AGREGACIÓN DIARIA:**

El script original descarga mensual. Para trading necesitas **diario**:

```python
# Modificar download_short_volume para agregación diaria
async def download_daily_short_volume(session, ticker, date):
    """Descarga short volume DIARIO para ML features"""
    url = f"{self.base_url}/v2/aggs/ticker/{ticker}/range/1/day/{date}/{date}"
    # Incluir short_volume en la agregación
```

## **Pipeline Recomendado para tu Proyecto:**

```bash
# 1. PRIMERO - Fundamentales (2-4 horas)
python download_fundamentals.py \
  --tickers-file tickers_confirmed.txt \
  --output-dir "C:\TSIS_Data\fundamentals" \
  --data-types fundamentals \
  --concurrent 40  # Más agresivo

# 2. SEGUNDO - Short Data (2-3 horas)
python download_fundamentals.py \
  --tickers-file tickers_confirmed.txt \
  --output-dir "C:\TSIS_Data\short_data" \
  --data-types short \
  --date-from 2004-01-01 \
  --concurrent 30

# 3. TERCERO - Reference Data (5 minutos)
python download_fundamentals.py \
  --output-dir "C:\TSIS_Data\reference" \
  --data-types reference

# 4. OPCIONAL - Quotes sample (para spread analysis)
# En lugar de TODO, descarga solo días clave:
# - Días de alto volumen
# - Días de earnings
# - Días de grandes gaps
python ingest_quotes_optimized.py \
  --dates-csv high_volume_days.csv \  # Generar desde tus trades
  --outdir "C:\TSIS_Data\quotes_samples" \
  --concurrent 50
```

## **Features de ML Críticos que Necesitas:**

Basándome en tus documentos sobre dilución y watchlist building:

1. **Dilution Risk Score**:
   - Operating cash flow / Market cap
   - Cash runway (quarters hasta bankruptcy)
   - Outstanding warrants / Float

2. **Short Squeeze Potential**:
   - Short interest % of float
   - Days to cover
   - Borrow fee rate

3. **Momentum Features** (ya los puedes calcular con trades):
   - RVOL (relative volume)
   - Dollar volume vs 20-day average
   - % from 52-week high/low

## **Recomendación Final:**

Para tu sistema de trading + ML:

1. **DESCARGA YA**: Fundamentales y Short Data (4-6 horas total)
2. **CALCULA LOCALMENTE**: Ratios y features desde los fundamentales
3. **SAMPLEA QUOTES**: Solo días de alto interés, no todo
4. **PRIORIZA**: Features que predicen dilución y squeeze events

Los scripts que te di funcionan, pero ajusta las prioridades según esto. El **ROI más alto** está en fundamentales + short data para detectar setups de dilución que describes en tus documentos.

¿Quieres que modifique algún script específicamente para optimizar para trading/ML?




---

**DESCARGA Fundamental**

El script descargará:
* Balance Sheets
* Income Statements
* Cash Flow Statements
* Financial Ratios (de Polygon)
* Comprehensive Income
* Smallcap Ratios (calculados automáticamente: bankruptcy risk, dilution risk, etc.)


```sh
PS D:\TSIS_SmallCaps> python scripts\02_final\download_fundamentals.py `
>>   --tickers-file tickers_confirmed.txt `
>>   --output-dir "C:\TSIS_Data\fundamentals" `
>>   --data-types fundamentals `
>>   --concurrent 40

[2025-11-24 10:49:49] [INFO] Cargados 8174 tickers
[2025-11-24 10:49:49] [INFO] 
============================================================
[2025-11-24 10:49:49] [INFO] Descargando balance_sheets
[2025-11-24 10:56:41] [INFO] Descargando income_statements
[2025-11-24 10:55:14] [INFO] Descargando balance_sheets
[2025-11-24 10:53:12] [INFO] Descargando cash_flow
...
[2025-11-24 11:00:26] [INFO] Calculando ratios personalizados para Small Caps
[2025-11-24 11:00:26] [INFO] ============================================================
[2025-11-24 11:00:31] [INFO] Ratios calculados: 5622/8174 tickers
[2025-11-24 11:00:32] [INFO] DESCARGA COMPLETADA
[2025-11-24 11:00:32] [INFO] ============================================================
```


```sh
PS D:\TSIS_SmallCaps> cd D:\TSIS_SmallCaps
>> python scripts\02_final\download_fundamentals.py 
>>   --tickers-file tickers_confirmed.txt 
>>   --output-dir "C:\TSIS_Data\short_data" 
>>   --data-types short 
>>   --date-from 2004-01-01 
>>   --concurrent 30

[2025-11-24 11:12:41] [INFO] Cargados 8174 tickers
[2025-11-24 11:12:41] [INFO] 
============================================================
[2025-11-24 11:12:41] [INFO] Descargando Short Interest & Volume (con métricas ML)
[2025-11-24 11:12:41] [INFO] Período: 2004-01-01 a 2025-11-24
[2025-11-24 11:12:41] [INFO] Tickers: 8,174
[2025-11-24 11:12:41] [INFO] ============================================================
[2025-11-24 11:12:43] [INFO] Batch 1/82 | SI: 78 | SV: 49
[2025-11-24 11:12:44] [INFO] Batch 2/82 | SI: 165 | SV: 102
... 
... 
[2025-11-24 11:14:26] [INFO] Batch 81/82 | SI: 6532 | SV: 4110
[2025-11-24 11:14:27] [INFO] Batch 82/82 | SI: 6595 | SV: 4150
[2025-11-24 11:14:27] [INFO] 
Resumen Short Data:
[2025-11-24 11:14:27] [INFO]   Short Interest: 6595/8174 tickers
[2025-11-24 11:14:27] [INFO]   Short Volume: 4150/8174 tickers
[2025-11-24 11:14:27] [INFO] DESCARGA COMPLETADA
[2025-11-24 11:14:27] [INFO] ============================================================
```

```sh
PS D:\TSIS_SmallCaps> python scripts/02_final/download_fundamentals.py `
>>   --output-dir "C:\TSIS_Data\reference" `
>>   --data-types reference

[2025-11-24 11:20:07] [INFO] 
============================================================
[2025-11-24 11:20:07] [INFO] Descargando Reference Data
[2025-11-24 11:20:07] [INFO] ============================================================
[2025-11-24 11:20:07] [INFO] Descargando Market Status Upcoming...
[2025-11-24 11:20:08] [INFO]   Guardado: 24 upcoming events
[2025-11-24 11:20:08] [INFO] Descargando Exchanges...
[2025-11-24 11:20:08] [INFO]   Guardado: 51 exchanges
[2025-11-24 11:20:08] [INFO] Descargando Condition Codes...
[2025-11-24 11:20:08] [INFO]   Guardado: 10 condition codes
[2025-11-24 11:20:08] [INFO] Descargando Ticker Types...
[2025-11-24 11:20:09] [INFO]   Guardado: 25 ticker types
[2025-11-24 11:20:09] [INFO] 
============================================================
[2025-11-24 11:20:09] [INFO] DESCARGA COMPLETADA
[2025-11-24 11:20:09] [INFO] ============================================================
```


```sh
cd D:\TSIS_SmallCaps 
python scripts/02_final/download_fundamentals.py 
   --output-dir "C:\TSIS_Data\additional" 
   --data-types ipos economic 
   --date-from 2004-01-01


# Descargando IPOs
[2025-11-24 12:06:29] [INFO] 
============================================================
[2025-11-24 12:06:29] [INFO] Descargando IPOs
[2025-11-24 12:06:29] [INFO] Per�odo: desde 2004-01-01
[2025-11-24 12:06:29] [INFO] ============================================================
[2025-11-24 12:06:32] [INFO]   Total IPOs guardados: 5247
[2025-11-24 12:06:32] [INFO] 
  IPOs por a�o:
[2025-11-24 12:06:32] [INFO]     2008: 1
[2025-11-24 12:06:32] [INFO]     2009: 70
[2025-11-24 12:06:32] [INFO]     2010: 139
[2025-11-24 12:06:32] [INFO]     2011: 176
[2025-11-24 12:06:32] [INFO]     2012: 179
[2025-11-24 12:06:32] [INFO]     2013: 305
[2025-11-24 12:06:32] [INFO]     2014: 367
[2025-11-24 12:06:32] [INFO]     2015: 291
[2025-11-24 12:06:32] [INFO]     2016: 223
[2025-11-24 12:06:32] [INFO]     2017: 281
[2025-11-24 12:06:32] [INFO]     2018: 287
[2025-11-24 12:06:32] [INFO]     2019: 298
[2025-11-24 12:06:32] [INFO]     2020: 511
[2025-11-24 12:06:32] [INFO]     2021: 945
[2025-11-24 12:06:32] [INFO]     2022: 232
[2025-11-24 12:06:32] [INFO]     2023: 251
[2025-11-24 12:06:32] [INFO]     2024: 352
[2025-11-24 12:06:32] [INFO]     2025: 339
[2025-11-24 12:06:32] [INFO] 
============================================================
[2025-11-24 12:06:32] [INFO] Descargando Economic Data
[2025-11-24 12:06:32] [INFO] Per�odo: desde 2004-01-01
[2025-11-24 12:06:32] [INFO] ============================================================
[2025-11-24 12:06:32] [INFO]   Descargando Treasury Yields...
[2025-11-24 12:06:33] [INFO]     Guardado: 5,476 registros de Treasury Yields
[2025-11-24 12:06:33] [INFO]   Descargando Inflation data...
[2025-11-24 12:06:33] [INFO]     Guardado: 261 registros de Inflation
[2025-11-24 12:06:33] [INFO]   Descargando Inflation Expectations...
[2025-11-24 12:06:33] [INFO]     Guardado: 262 registros de Inflation Expectations
[2025-11-24 12:06:33] [INFO] 
============================================================
[2025-11-24 12:06:33] [INFO] DESCARGA COMPLETADA
[2025-11-24 12:06:33] [INFO] ============================================================
```


```sh
cd D:\TSIS_SmallCaps 
   python scripts/02_final/download_fundamentals.py 
   --tickers-file tickers_confirmed.txt 
   --output-dir "C:\TSIS_Data\additional" 
   --data-types news --date-from 2004-01-01 
   --concurrent 30


# Descargando News
[2025-11-24 12:07:23] [INFO] Cargados 8174 tickers
[2025-11-24 12:07:23] [INFO] 
============================================================
[2025-11-24 12:07:23] [INFO] Descargando News
[2025-11-24 12:07:23] [INFO] Per�odo: desde 2004-01-01
[2025-11-24 12:07:23] [INFO] Tickers: 8,174
[2025-11-24 12:07:23] [INFO] ============================================================
[2025-11-24 12:07:24] [INFO]   Batch 1/82 | Tickers con news: 55 | Art�culos: 5,244
...
...
[2025-11-24 12:08:43] [INFO]   Batch 82/82 | Tickers con news: 4963 | Art�culos: 416,233
[2025-11-24 12:08:43] [INFO] 
Resumen News:
[2025-11-24 12:08:43] [INFO]   Tickers con noticias: 4963/8174
[2025-11-24 12:08:43] [INFO]   Total art�culos: 416,233
[2025-11-24 12:08:43] [INFO] 
============================================================
[2025-11-24 12:08:43] [INFO] DESCARGA COMPLETADA
[2025-11-24 12:08:43] [INFO] ============================================================
```


```sh
C:\TSIS_Data\additional\
├── economic/
│   ├── treasury_yields.parquet        (64 KB - 5,476 registros)
│   ├── inflation.parquet              (11 KB - 261 registros)
│   └── inflation_expectations.parquet (13 KB - 262 registros)
├── ipos/
│   └── all_ipos.parquet               (165 KB - 5,247 IPOs)
└── news/
    └── news_batch_0000-0081.parquet   (~100 MB - 416,233 artículos)
```


## VERIFICACIÓN DE DATOS - C:\TSIS_Data

```sh
python -c "
import polars as pl
from pathlib import Path

print('VERIFICACION DE DATOS - C:\\TSIS_Data')
print('=' * 60)

# Tickers
with open(r'D:\TSIS_SmallCaps\tickers_confirmed.txt') as f:
    tickers = set(line.strip() for line in f if line.strip())
print(f'Universo: {len(tickers):,} tickers\n')

# 1. Fundamentals - contar archivos por carpeta
print('1. FUNDAMENTALS')
fund = Path(r'C:\TSIS_Data\fundamentals')
for d in ['balance_sheets', 'income_statements', 'cash_flow_statements', 'smallcap_ratios']:
    p = fund / d
    if p.exists():
        n = len(list(p.glob('*.parquet')))
        print(f'   {d}: {n} archivos')

# 2. Short
print('\n2. SHORT DATA')
short = Path(r'C:\TSIS_Data\short_data')
for d in ['short_interest', 'short_volume']:
    p = short / d
    if p.exists():
        n = len(list(p.glob('*.parquet')))
        print(f'   {d}: {n} archivos')

# 3. Reference
print('\n3. REFERENCE')
ref = Path(r'C:\TSIS_Data\reference\reference')
if ref.exists():
    for f in ref.glob('*.parquet'):
        df = pl.read_parquet(f)
        print(f'   {f.stem}: {len(df)} registros')

# 4. Additional
print('\n4. ADDITIONAL')
add = Path(r'C:\TSIS_Data\additional')

# IPOs
ipos = add / 'ipos' / 'all_ipos.parquet'
if ipos.exists():
    df = pl.read_parquet(ipos)
    print(f'   IPOs: {len(df):,} registros')

# Economic
econ = add / 'economic'
if econ.exists():
    for f in econ.glob('*.parquet'):
        df = pl.read_parquet(f)
        print(f'   {f.stem}: {len(df):,} registros')

# News
news = add / 'news'
if news.exists():
    n = len(list(news.glob('*.parquet')))
    print(f'   News: {n} batch files')

# 5. Trades ticks - solo contar carpetas
print('\n5. TRADES TICKS')
for td in ['trades_ticks', 'trades_ticks_2004_2018', 'trades_ticks_2019_2025']:
    p = Path(r'C:\TSIS_Data') / td
    if p.exists():
        dirs = len([d for d in p.iterdir() if d.is_dir()])
        print(f'   {td}: {dirs:,} tickers')
"
```

**Universo objetivo: 8,174 tickers**

| Categoría | Datos | Cantidad | Cobertura |
|-----------|-------|----------|-----------|
| **FUNDAMENTALS** | | | |
| | Balance Sheets | 5,622 archivos | 68.8% |
| | Income Statements | 5,624 archivos | 68.8% |
| | Cash Flow Statements | 5,622 archivos | 68.8% |
| | SmallCap Ratios | 5,622 archivos | 68.8% |
| **SHORT DATA** | | | |
| | Short Interest | 6,595 archivos | 80.7% |
| | Short Volume | 4,150 archivos | 50.8% |
| **REFERENCE** | | | |
| | Condition Codes | 10 registros | 100% |
| | Exchanges | 51 registros | 100% |
| | Market Status | 24 registros | 100% |
| | Ticker Types | 25 registros | 100% |
| **ADDITIONAL** | | | |
| | IPOs | 5,247 registros | (2008-2025) |
| | Treasury Yields | 5,476 registros | (desde 1962) |
| | Inflation | 261 registros | 100% |
| | Inflation Expectations | 262 registros | 100% |
| | News | 82 batch files (~416K artículos) | 60.7% |
| **TRADES TICKS** | | | |
| | trades_ticks | 961 tickers | 11.8% |
| | trades_ticks_2004_2018 | 3,455 tickers | 42.3% |
| | trades_ticks_2019_2025 | 6,311 tickers | 77.2% |

### Notas:
- **IPOs** solo disponibles desde 2008 (limitación de Polygon/Massive)
- Los tickers sin fundamentals son probablemente SPACs, shells, o compañías sin reportes SEC
- Treasury Yields tiene datos históricos desde 1962
- News: ~416,233 artículos para 4,963 tickers

