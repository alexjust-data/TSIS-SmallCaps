
## Descraga para régimen de mercado

**Totales:**
* 34 tickers (3 índices + 31 ETFs)
* 64.3M rows de datos minute
* 3.6 minutos de descarga
* Solo I:SOX no tiene datos minute


| Categoría | Tickers | Registros | Rango |
|-----------|---------|-----------|-------|
| **Índices** | I:NDX, I:COMP, I:SOX | 2,115 rows | 2023-02-15 → 2025-12-01 |
| **ETFs principales** | SPY, QQQ, IWM, DIA | ~22,000 rows | 2003-09-10 → 2025-12-01 |
| **Volatilidad** | VIXY, VXX, UVXY | ~11,400 rows | 2009+ → 2025-12-01 |
| **Bonos** | TLT, HYG, LQD | ~15,900 rows | 2003+ → 2025-12-01 |
| **Sectores** | XLK, XLF, XLE, XLV, XLI, XLP, XLY, XLB, XLRE, XLU, XLC | ~55,000 rows | 2003+ → 2025-12-01 |
| **Commodities** | GLD, SLV, USO, UNG | ~19,800 rows | 2004+ → 2025-12-01 |
| **FX/Internacional** | UUP, FXE, SPSM, VB, EFA, EEM | ~27,000 rows | 2003+ → 2025-12-01 |

scripts 
* `D:\TSIS_SmallCaps\scripts\02_final\download_indices_ultra_fast.py`
* `D:\TSIS_SmallCaps\scripts\02_final\download_indices.py`

```sh
PS D:\TSIS_SmallCaps> python "D:\TSIS_SmallCaps\scripts\02_final\download_indices_ultra_fast.py" --output "C:/TSIS_Data/regime_indicators" --timespan minute --start-year 2004 --end-year 2025 --concurrent 50
Cache de rangos cargado: 34 tickers
======================================================================
DESCARGA ULTRA-RAPIDA DE INDICES/ETFs
======================================================================
Tickers solicitados: 34
Periodo solicitado: 2004 - 2025
Timespan: minute
Concurrencia: 50
======================================================================

FASE 1: Detectando rangos disponibles...
----------------------------------------------------------------------

Tickers con datos: 34/34

FASE 2: Generando tareas...
----------------------------------------------------------------------
  I:NDX: 35 meses (2023-02-15 -> 2025-12-01)
  I:COMP: 35 meses (2023-02-15 -> 2025-12-01)
  I:SOX: 35 meses (2023-02-15 -> 2025-12-01)
  SPY: 264 meses (2004-01-01 -> 2025-12-01)
  QQQ: 264 meses (2004-01-01 -> 2025-12-01)
  IWM: 264 meses (2004-01-01 -> 2025-12-01)
  DIA: 264 meses (2004-01-01 -> 2025-12-01)
  VIXY: 180 meses (2011-01-04 -> 2025-12-01)
  VXX: 204 meses (2009-01-30 -> 2025-12-01)
  UVXY: 171 meses (2011-10-04 -> 2025-12-01)
  TLT: 264 meses (2004-01-01 -> 2025-12-01)
  HYG: 263 meses (2004-02-04 -> 2025-12-01)
  LQD: 264 meses (2004-01-01 -> 2025-12-01)
  XLK: 264 meses (2004-01-01 -> 2025-12-01)
  XLF: 264 meses (2004-01-01 -> 2025-12-01)
  XLE: 264 meses (2004-01-01 -> 2025-12-01)
  XLV: 264 meses (2004-01-01 -> 2025-12-01)
  XLI: 264 meses (2004-01-01 -> 2025-12-01)
  XLP: 264 meses (2004-01-01 -> 2025-12-01)
  XLY: 264 meses (2004-01-01 -> 2025-12-01)
  XLB: 264 meses (2004-01-01 -> 2025-12-01)
  XLRE: 123 meses (2015-10-08 -> 2025-12-01)
  XLU: 264 meses (2004-01-01 -> 2025-12-01)
  XLC: 91 meses (2018-06-19 -> 2025-12-01)
  GLD: 254 meses (2004-11-18 -> 2025-12-01)
  SLV: 237 meses (2006-04-28 -> 2025-12-01)
  UUP: 227 meses (2007-02-20 -> 2025-12-01)
  FXE: 241 meses (2005-12-12 -> 2025-12-01)
  USO: 237 meses (2006-04-10 -> 2025-12-01)
  UNG: 225 meses (2007-04-18 -> 2025-12-01)
  SPSM: 99 meses (2017-10-16 -> 2025-12-01)
  VB: 264 meses (2004-01-30 -> 2025-12-01)
  EFA: 264 meses (2004-01-01 -> 2025-12-01)
  EEM: 264 meses (2004-01-01 -> 2025-12-01)

Total tareas: 7,409

FASE 3: Descargando...
======================================================================
[15:24:04] Progress: 100/7409 (1.3%) | Rate: 16.1/s | Rows: 35,185 | NoData: 0 | Errors: 95
[15:24:07] Progress: 200/7409 (2.7%) | Rate: 21.7/s | Rows: 1,307,016 | NoData: 0 | Errors: 100
...
...
[15:31:01] Progress: 7409/7409 (100.0%) | Rate: 38.1/s | Rows: 64,348,953 | NoData: 112 | Errors: 100

======================================================================
GUARDANDO ARCHIVOS...
======================================================================
  I:NDX: 25,701 rows -> C:\TSIS_Data\regime_indicators\indices\I_NDX\minute.parquet
  I:COMP: 8,993 rows -> C:\TSIS_Data\regime_indicators\indices\I_COMP\minute.parquet
  SPY: 4,021,590 rows -> C:\TSIS_Data\regime_indicators\etfs\SPY\minute.parquet
  QQQ: 2,715,473 rows -> C:\TSIS_Data\regime_indicators\etfs\QQQ\minute.parquet
  IWM: 3,120,102 rows -> C:\TSIS_Data\regime_indicators\etfs\IWM\minute.parquet
  DIA: 2,542,922 rows -> C:\TSIS_Data\regime_indicators\etfs\DIA\minute.parquet
  VIXY: 1,529,589 rows -> C:\TSIS_Data\regime_indicators\etfs\VIXY\minute.parquet
  VXX: 2,476,083 rows -> C:\TSIS_Data\regime_indicators\etfs\VXX\minute.parquet
  UVXY: 2,375,050 rows -> C:\TSIS_Data\regime_indicators\etfs\UVXY\minute.parquet
  TLT: 2,448,661 rows -> C:\TSIS_Data\regime_indicators\etfs\TLT\minute.parquet
  HYG: 1,714,912 rows -> C:\TSIS_Data\regime_indicators\etfs\HYG\minute.parquet
  LQD: 1,825,488 rows -> C:\TSIS_Data\regime_indicators\etfs\LQD\minute.parquet
  XLK: 2,145,101 rows -> C:\TSIS_Data\regime_indicators\etfs\XLK\minute.parquet
  XLF: 2,489,540 rows -> C:\TSIS_Data\regime_indicators\etfs\XLF\minute.parquet
  XLE: 2,425,286 rows -> C:\TSIS_Data\regime_indicators\etfs\XLE\minute.parquet
  XLV: 2,064,906 rows -> C:\TSIS_Data\regime_indicators\etfs\XLV\minute.parquet
  XLI: 2,032,353 rows -> C:\TSIS_Data\regime_indicators\etfs\XLI\minute.parquet
  XLP: 2,023,297 rows -> C:\TSIS_Data\regime_indicators\etfs\XLP\minute.parquet
  XLY: 1,992,213 rows -> C:\TSIS_Data\regime_indicators\etfs\XLY\minute.parquet
  XLB: 2,071,123 rows -> C:\TSIS_Data\regime_indicators\etfs\XLB\minute.parquet
  XLRE: 901,803 rows -> C:\TSIS_Data\regime_indicators\etfs\XLRE\minute.parquet
  XLU: 2,121,133 rows -> C:\TSIS_Data\regime_indicators\etfs\XLU\minute.parquet
  XLC: 728,393 rows -> C:\TSIS_Data\regime_indicators\etfs\XLC\minute.parquet
  GLD: 2,693,592 rows -> C:\TSIS_Data\regime_indicators\etfs\GLD\minute.parquet
  SLV: 2,435,839 rows -> C:\TSIS_Data\regime_indicators\etfs\SLV\minute.parquet
  UUP: 1,266,245 rows -> C:\TSIS_Data\regime_indicators\etfs\UUP\minute.parquet
  FXE: 970,892 rows -> C:\TSIS_Data\regime_indicators\etfs\FXE\minute.parquet
  USO: 2,367,075 rows -> C:\TSIS_Data\regime_indicators\etfs\USO\minute.parquet
  UNG: 2,204,269 rows -> C:\TSIS_Data\regime_indicators\etfs\UNG\minute.parquet
  SPSM: 576,798 rows -> C:\TSIS_Data\regime_indicators\etfs\SPSM\minute.parquet
  VB: 1,444,291 rows -> C:\TSIS_Data\regime_indicators\etfs\VB\minute.parquet
  EFA: 2,199,125 rows -> C:\TSIS_Data\regime_indicators\etfs\EFA\minute.parquet
  EEM: 2,391,115 rows -> C:\TSIS_Data\regime_indicators\etfs\EEM\minute.parquet

======================================================================
COMPLETADO
======================================================================
Tiempo: 3.6 minutos
Meses con datos: 7,197
Meses sin datos: 112
Errores: 100
Total rows: 64,348,953
Velocidad: 33.9 tareas/s
======================================================================
```

## **📁 Estructura final después de completar:**

```
C:\TSIS_Data\regime_indicators\
├── ticker_ranges.json
├── etfs/
│   ├── DIA/          (day.parquet + minute.parquet)
│   ├── EEM/          
│   ├── EFA/          
│   ├── FXE/          
│   ├── GLD/          
│   ├── HYG/          
│   ├── IWM/          
│   ├── LQD/          
│   ├── QQQ/          
│   ├── SLV/          
│   ├── SPSM/         
│   ├── SPY/          
│   ├── TLT/          
│   ├── UNG/          
│   ├── USO/          
│   ├── UUP/          
│   ├── UVXY/         
│   ├── VB/           
│   ├── VIXY/         
│   ├── VXX/          
│   ├── XLB/          
│   ├── XLC/          
│   ├── XLE/          
│   ├── XLF/          
│   ├── XLI/          
│   ├── XLK/          
│   ├── XLP/          
│   ├── XLRE/         
│   ├── XLU/          
│   ├── XLV/          
│   └── XLY/          (31 ETFs)
└── indices/
    ├── I_COMP/       (day + minute)
    ├── I_NDX/        (day + minute)
    └── I_SOX/        (solo day - no hay minute disponible)

```

