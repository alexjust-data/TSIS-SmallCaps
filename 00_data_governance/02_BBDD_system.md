Transformar tu proyecto *TSIS (Trading Scientific Intelligence System)* en una **plataforma real de ejecución y aprendizaje**.  

Vamos a construir **el diseño integral de la base de datos (BBDD)** que soportará **todo el ciclo de vida del sistema: datos brutos → features → señales → ejecución → métricas → aprendizaje**.

Te lo explico paso a paso, con estructura modular (científica + operativa).

---

## 🧩 1️⃣ Principios de diseño de la BBDD TSIS

Tu sistema mezcla tres mundos:

| Capa                                  | Rol                                                     | Naturaleza               |
| ------------------------------------- | ------------------------------------------------------- | ------------------------ |
| **Científica (backtesting)**          | Almacena históricos, labeling, features y resultados ML | Batch, reproducible      |
| **Operativa (real-time trading)**     | Procesa flujos en vivo, genera señales, órdenes, logs   | Streaming, baja latencia |
| **Analítica (monitoring y feedback)** | Calcula métricas de performance y mejora continua       | Reporting, queries       |

Por tanto, la BBDD debe tener **dos modos de acceso**:

1. **Data Lake (Parquet / S3 / Arrow / Polars)** → para históricos y batch ML.
2. **Data Warehouse / Store (PostgreSQL, DuckDB o ClickHouse)** → para consultas rápidas y estado en vivo.

Ambos deben sincronizarse automáticamente.

---

## 🧠 2️⃣ Esquema global del “universo TSIS”

```
📂 TSIS_DATABASE
├── reference/                → catálogos estáticos y metadatos
│   ├── tickers_dim.parquet
│   ├── sectors_dim.parquet
│   └── exchanges_dim.parquet
│
├── raw_market_data/          → datos brutos (Polygon, DAS feed, SEC)
│   ├── ohlcv_daily/
│   ├── ohlcv_intraday_1m/
│   ├── trades_tick/
│   ├── quotes_tick/
│   └── sec_filings/
│
├── processed_features/       → features numéricas ya derivadas
│   ├── daily_cache/          → agregados diarios (close_d, vol_d, rvol30…)
│   ├── intraday_cache/       → 1-min agregados (VWAP, dollar_vol, etc.)
│   ├── informational_bars/   → DIB/VIB (volumen, dólar o desequilibrio)
│   ├── labeling/             → triple barrier / meta-labeling
│   └── ml_dataset/           → dataset final para entrenamiento
│
├── signals_realtime/         → señales en tiempo real (desde TSIS engine)
│   ├── active_signals.parquet
│   └── signals_history/
│
├── execution/                → operaciones reales (DAS)
│   ├── orders_log.parquet
│   ├── fills_log.parquet
│   ├── positions.parquet
│   └── risk_limits.parquet
│
├── analytics/                → métricas de performance / riesgo / edge
│   ├── pnl_daily.parquet
│   ├── strategy_stats/
│   ├── model_stats/
│   └── execution_latency/
│
└── governance/               → control de versiones, seeds, hashes
    ├── data_version.json
    ├── model_registry.json
    └── pipeline_status.log
```

👉 Este esquema combina tu *pipeline científico (López de Prado)* con la capa operativa de *trading ATS*.

---

## ⚙️ 3️⃣ Detalle por capa funcional

### A. **reference/**

Información estática de referencia.
Ejemplo:

| ticker | name             | exchange | ipo_date   | delisted | sector     | shares_outstanding | float     |
| ------ | ---------------- | -------- | ---------- | -------- | ---------- | ------------------ | --------- |
| CREV   | Creative MedTech | XNAS     | 2021-04-12 | False    | Healthcare | 20_000_000         | 8_000_000 |

Se actualiza 1×/día o por cambios de SEC.

---

### B. **raw_market_data/**

Datos en su forma original:

| ticker | timestamp        | open | high | low  | close | volume | vwap | ... |
| ------ | ---------------- | ---- | ---- | ---- | ----- | ------ | ---- | --- |
| CREV   | 2023-11-10 09:31 | 40.2 | 42.0 | 39.9 | 41.8  | 123000 | 41.1 | ... |

Cada fuente conserva su formato, pero se normaliza en schema Arrow (columnar).
Idealmente dividido por:

```
raw_market_data/ohlcv_intraday_1m/ticker=CREV/year=2023/month=11/
```

---

### C. **processed_features/**

Donde se crean las *variables informacionales*.

Ejemplo de columnas:
| ticker | date | pctchg_d | dollar_vol_d | rvol30 | float_rotation | intraday_volatility | info_score | ... |
|--------|------|-----------|---------------|---------|----------------|---------------------|-------------|
| CREV | 2023-11-10 | +0.57 | 2.43e7 | 3.12 | 0.21 | 0.042 | 3 | ... |

Subniveles:

* `daily_cache/` → features diarios.
* `intraday_cache/` → agregaciones minuto.
* `informational_bars/` → DIB/VIB (basado en desequilibrio).
* `labeling/` → triple barrier con etiquetas `+1 / 0 / -1`.
* `ml_dataset/` → dataset limpio, alineado y con pesos (`sample_weight`).

---

### D. **signals_realtime/**

Salidas del motor TSIS en vivo (lo que se vería en `www.tsis.ia`).

**active_signals.parquet**

| timestamp           | ticker | phase   | setup       | ml_score | action      | entry | stop | tp1  | status  |
| ------------------- | ------ | ------- | ----------- | -------- | ----------- | ----- | ---- | ---- | ------- |
| 2025-10-26 15:32:11 | CREV   | impulso | Gap&Go long | 0.84     | ENTRY_ARMED | 2.37  | 2.19 | 2.65 | waiting |

**signals_history/**
Histórico diario para backtesting de señales reales generadas en sesión.

---

### E. **execution/**

Datos reales enviados/recibidos desde DAS Trader Pro.

| order_id | ticker | side | qty  | price | stop | tp   | status | pnl  | latency_ms |
| -------- | ------ | ---- | ---- | ----- | ---- | ---- | ------ | ---- | ---------- |
| 2342     | CREV   | BUY  | 2000 | 2.37  | 2.19 | 2.65 | FILLED | +580 | 130        |

Aquí se integra con la **DAS API bridge**.

También:

* `positions.parquet` → posiciones vivas.
* `risk_limits.parquet` → límites activos (riesgo diario, drawdown, exposición).

---

### F. **analytics/**

Resumen estadístico y científico del sistema:

1. **pnl_daily.parquet**

   * resultado diario acumulado por estrategia.
2. **strategy_stats/**

   * métricas Sharpe, profit factor, max DD, win rate, expectancy.
3. **model_stats/**

   * métricas ML (precision, recall, F1, AUC) por modelo.
4. **execution_latency/**

   * tiempo entre señal → orden enviada → orden ejecutada (en ms).

👉 Con esto puedes estudiar la degradación real de performance *live vs backtest*.

---

### G. **governance/**

Control de calidad y trazabilidad:

* `data_version.json` → hash SHA de datasets diarios.
* `model_registry.json` → qué modelo (nombre, versión, fecha de entrenamiento) estaba activo.
* `pipeline_status.log` → logs del orquestador Prefect/Airflow (última ejecución correcta).

Esto garantiza **reproducibilidad total**, estilo científico.

---

## 📊 4️⃣ Relaciones principales (modelo relacional simplificado)

```
tickers_dim (1) ───< ohlcv_daily
tickers_dim (1) ───< ohlcv_intraday_1m
ohlcv_intraday_1m (1) ───< features_intraday
features_intraday (1) ───< labels
labels (1) ───< signals
signals (1) ───< orders
orders (1) ───< fills
fills (1) ───< pnl_daily
```

👉 Esto te permite unir cualquier capa (por ejemplo: *ver cómo se comportó un modelo dado según las features de ese día y las ejecuciones reales*).

---

## 🧮 5️⃣ Herramientas recomendadas

| Tipo               | Herramienta sugerida                                         | Motivo                            |
| ------------------ | ------------------------------------------------------------ | --------------------------------- |
| **Data lake**      | `Parquet` + `Polars` + `Arrow`                               | Columnar, rápido, escalable       |
| **Warehouse**      | `DuckDB` o `PostgreSQL`                                      | Consultas interactivas y joins    |
| **Realtime cache** | `Redis` o `SQLite WAL`                                       | Señales instantáneas en memoria   |
| **Orquestador**    | `Prefect` o `Airflow`                                        | Control de DAGs y logs            |
| **Versionado**     | `DVC` o `Git LFS`                                            | Reproducibilidad de modelos/datos |
| **Frontend / API** | `FastAPI` + `React` (para [www.tsis.ia](http://www.tsis.ia)) | Web moderna y rápida              |

---

## 🧠 6️⃣ Lo importante: separación entre *información* y *decisión*

| Capa                      | Contiene                                   | Rol                  |
| ------------------------- | ------------------------------------------ | -------------------- |
| **Datos (reference/raw)** | Hechos objetivos (ticks, precios, filings) | Materia prima        |
| **Features/Labels**       | Información estructurada                   | Base del aprendizaje |
| **Signals**               | Predicciones y decisiones ML               | Output del modelo    |
| **Execution**             | Acciones reales tomadas                    | Lo que hace el bot   |
| **Analytics**             | Evaluación / mejora                        | Cómo de bueno fue    |

Esto garantiza que puedas **auditar cada paso**:

> “¿Por qué entró CREV el 26 Oct 2025 a las 10:31?”
> → Vas a `signals_history`, miras `ml_score`, `phase`, `checklist=True`, `risk_OK=True`, y validas la cadena completa.

---

## ✅ 7️⃣ Resultado: una BBDD científica + operativa

Tu BBDD no será solo un almacenamiento de precios.
Será un **ecosistema trazable** que une:

* *Mercado real* → *Features informacionales* → *Modelo científico* → *Señal ejecutable* → *Orden real* → *Análisis de resultados*.

---


