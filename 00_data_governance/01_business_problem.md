# Definición ejecutiva y técnica de TSIS `(Trading Scientific Intelligence System)`



## 1. Clarify objectives — Qué estamos intentando hacer o mejorar

### Objetivo estratégico

Construir una infraestructura científica-operativa capaz de:

1. Detectar en mercado real comportamientos anómalos en small caps y micro caps (pump, distribución, colapso, rebote).
2. Clasificar el estado actual de cada acción dentro del ciclo pump & dump.
3. Convertir esa interpretación de contexto en señales operativas accionables (long / short) con riesgo definido.
4. Ejecutar esas ideas en el broker (DAS Trader Pro), con control de riesgo y trazabilidad.

Este sistema unifica investigación cuantitativa, modelado estadístico, priorización intradía y ejecución supervisada / automatizada.

### Ángulo de negocio

El objetivo de negocio no es "predecir precio" de forma abstracta. Es construir una ventaja explotable y repetible en un segmento concreto del mercado: tickers low-float, high-momentum, ilíquidos, manipulables, donde hay movimientos extremos en ventanas muy cortas. Estos activos son volátiles, ofrecen R grandes en poco tiempo y suelen estar mal cubiertos por la infraestructura institucional tradicional. Ahí está el edge.

### Lo que queremos mejorar

* **Capacidad de detección temprana:**   identificar que un ticker está entrando en fase de impulso (o de agotamiento) mientras todavía se puede actuar con una relación riesgo/beneficio favorable.

* **Clasificación precisa:**   distinguir entre las fases de un ciclo especulativo:
   - Impulso/markup (entrada de flujo agresivo).
   - Distribución (los insiders descargan).
   - Colapso/flush (venta forzada / halt / SSR / primera vela roja real).
   - Rebote técnico / "dead cat bounce".  

   Esto evita confundir "pullback sano" con "fin de la película".

* **Timing operativo:**   reducir falsos positivos de entrada y no llegar tarde a la rotura clave (por ejemplo, ruptura del máximo premarket con volumen auténtico y liquidez utilizable, no solo print sucio).
* **Fiabilidad de la decisión:**   pasar de intuición discrecional a checklist cuantificable y repetible por máquina.

### Nivel al que se aplica

No es un producto "para el usuario final retail". Es una plataforma interna tipo mesa prop:
- motor de datos,
- motor de señal,
- motor de ejecución,
- motor de riesgo,
- y capa de auditoría.

En otras palabras, es una infraestructura empresa–producto que soporta la operativa intradía en small caps y genera histórico auditable para mejora continua.

### Business case

1. **Reducir ruido humano:** que el sistema priorice dinámicamente en qué tickers merece la pena enfocar la atención, en lugar de vigilar manualmente decenas de pantallas.

2. **Disminuir el coste de decisión errónea:** limitar entradas impulsivas sin contexto (por ejemplo, entrar corto contra un runner sin confirmar distribución real y sin borrow disponible).

3. **Aumentar la consistencia estadística:** aplicar siempre el mismo proceso de lectura de fase → preparación → ejecución → gestión de riesgo.

4. **Capturar edge repetible y medible:** documentar qué patrones realmente producen beneficio ajustado al riesgo y en qué condiciones microestructurales (spread, float, halts, RVOL, etc.).

## 2. Define metrics — Qué vamos a medir y optimizar

El sistema TSIS tiene tres familias de métricas: modelo, trading y calidad de datos. Todas son necesarias.

#### 2.1 Métricas de modelo (machine learning)

Estas métricas miden la capacidad del modelo para leer la fase de mercado y anticipar un comportamiento explotable.

* **Precision:** de todas las señales "esto es un pump en fase de impulso", cuántas eran de verdad impulso tradable y no simplemente ruido. Controla falsos positivos.
* **Recall / Sensitivity:** de todos los eventos de verdad relevantes (movimiento explosivo / flush real), cuántos fuimos capaces de detectar. Controla falsos negativos. En mercados de eventos raros (squeezes violentos, first red day auténtico), el recall es crítico.
* **F1 Score:** balance entre precision y recall en eventos muy desbalanceados. Esta métrica es importante porque los pumps "buenos" son raros. Si solo optimizas accuracy, el modelo puede "decir que no hay señal" siempre y parecer bueno. F1 te obliga a capturar lo raro.
* **AUC-ROC / Precision-Recall Curve:** calidad global del clasificador bajo distintos umbrales.
* **MAE / Error de magnitud:** si el modelo estima duración esperada de la fase o profundidad esperada del dump, se evalúa la desviación absoluta media.
* **Latencia de inferencia:** tiempo entre recibir el dato intradía y producir el score. Esta métrica marca si el modelo es operativo o solo académico.

#### 2.2 Métricas de negocio y trading

Estas métricas determinan si la señal ML más las reglas operativas generan dinero de forma defendible.

* **Profit Factor:** beneficio bruto / pérdida bruta por estrategia concreta (Gap&Go long, First Red Day short, VWAP reclaim long, Late Day Fade short, etc.). Es el primer indicador de edge.
* **Expectancy por trade:** media de (beneficio - pérdida) por operación, en dólares o múltiplos de riesgo. Evalúa si el patrón tiene esperanza matemática positiva.
* **Win rate:** porcentaje de trades positivos. No es suficiente sola, pero es operativamente importante para la psicología y la estabilidad del equity curve.
* **Sharpe Ratio / Sortino:** beneficio ajustado por volatilidad de resultados. Nos dice si el flujo de PnL es explotable a escala.
* **Max Drawdown:** pérdida máxima en la curva de capital simulada/real. Absolutamente crítico para control de riesgo y sizing.
* **Lead time de detección:** cuánto antes TSIS etiqueta “fase de impulso” o “fase de agotamiento” respecto al punto óptimo de entrada o salida. Esto mide la utilidad táctica del sistema, no solo su capacidad de etiquetar bonitamente a posteriori.
* **Latencia señal→ejecución:** tiempo entre “ENTRY_ARMED / ENTRY_CONFIRMED” y la orden realmente enviada a mercado. Mide fricción operativa, y es vital cuando damos el salto a ejecución automática.

#### 2.3 Métricas de calidad de datos / integridad estructural

Sin datos limpios y trazables, no hay sistema reproducible.

* **Cobertura temporal:** cuántos años de histórico hay por ticker para cada capa (diario, 1 minuto, tick-level). En nuestro caso: 20 años diario, 1-min consolidado, y ventana rolling de ~5 años de datos tick/quote profundo para el 10 % de tickers más relevantes.

* **Cobertura de universo:** cuántos tickers small/micro-cap activos actuales y delisted (sin sesgo de supervivencia). Aquí entran fuentes como Norgate/Kibot para delisted y Polygon para activos.

* **Continuidad de sesión:** % de barras informacionales sin huecos en las series DIB/VIB (Dollar/Volume/Imbalance Bars). Huecos = riesgo de etiquetar un falso movimiento.

* **Calidad microestructura disponible en vivo:** spread actual, profundidad de libro, existencia de borrow si la señal es short. Esto es condición previa a ejecución.

* **Sample Weights / unicidad de eventos:** asegurar que no estamos entrenando el modelo diez veces con el mismo patrón duplicado del mismo ticker en el mismo día, inflando el edge.

### 3. Constraints and scope — Limitaciones y alcance

#### 3.1 Tipo de predicción

No es HFT. No es latencia en microsegundos. Se trabaja en la escala de segundos a minutos.

**Ventana temporal:** se generan señales en marcos de 1 a 5 minutos.
**Objetivo:** detectar inicio de squeeze, agotamiento de tendencia parabólica, ruptura de VWAP con volumen real, primer giro bajista tras extensión irracional, etc.
**Tiempo aceptable para inferencia:** menos de 60 segundos por ticker activo, preferible menos.

Esto sitúa TSIS en “detección intradía temprana” y no en “market making / scalping de microsegundos”.

#### 3.2 Recursos computacionales y límites prácticos

**Infraestructura desplegada:** Python, Polars, Prefect/Airflow, almacenamiento columnar Parquet.
**Capacidad histórica:** ~3.000 tickers × 20 años de datos diarios + histórico intradía consolidado; tick-level profundo solo para subset de interés (rolling ~5 años) para controlar tamaño en disco y volumen de I/O.
**Cómputo:** CPU multicore para ingesta paralela, feature engineering y scoring batch frecuente; GPU opcional únicamente si los modelos lo justifican (por ejemplo, ensembles complejos o redes ligeras).
**Rate limits externos:** Polygon, SEC Filings, etc. requieren cacheo y sincronización incremental. No se puede asumir “stream infinito” oficial sin límites.

Conclusión: el modelo debe ser tabular, explicable, rápido de evaluar, y barato de mantener (LightGBM, XGBoost, RandomForest bien calibrado), en lugar de arquitecturas pesadas tipo deep learning secuencial con gran coste de inferencia.

#### 3.3 Frecuencia de predicción y refresco del universo

**Nivel diario:** selección de universo dinámico (qué tickers vigilar hoy). Esto depende de gap %, float bajo, filings de dilución, volatilidad reciente, rotación de float, RVOL.
**Nivel intradía:** priorización y rescoring cada 1–5 minutos sobre ese universo reducido, no sobre todo el mercado completo. Aquí buscamos eventos como Gap&Go temprano, First Red Day, VWAP reclaim, fade de final de día, etc.

Esto permite escalar: en vez de trackear 3.000 símbolos en vivo, se vigilan 30-80 “calientes”; el motor ML ordena cuáles son realmente candidatos de calidad; y solo unos pocos pasan a “ENTRY_ARMED”.

3.4 Latencia operativa y toma de decisión

En la versión de sólo señal (alertas): podemos permitirnos lotes cada 1-5 minutos.
En la versión con disparo automático de orden: la latencia cae a segundos.
Eso obliga a:
**tener un ScannerService en vivo,
– mantener estado en memoria,
– no depender exclusivamente de lectura batch desde disco,
– y precomputar features clave (VWAP actual, high-of-day, spread, volumen minuto actual, etc.) en tiempo real.

3.5 Riesgos de infraestructura (y por tanto alcance realista)

– Storage:** crece a escala de terabytes si guardas tick/quote de todos los tickers durante décadas. Se controla con ventanas móviles y selección de subset relevante.
**Microestructura extrema:** small caps pueden haltear, quedarse sin liquidez, tener spreads enormes de repente, activar SSR (Short Sale Restriction), o volverse inejecutables. Esto obliga a introducir micro-checks antes de lanzar una orden.
**Regulación y responsabilidad:** en el modo totalmente automático (ATS), el sistema asume responsabilidad operativa completa. Necesita kill switch, límites de exposición diaria, y logging/auditoría en tiempo real.

En resumen: el alcance del sistema es detección intradía y ejecución táctica en small/micro caps bajo gestión de riesgo estricta. No es HFT, no es market making y no es un sistema sin control de riesgo.

### 4. High-level design — Arquitectura de alto nivel

#### 4.1 Flujo conceptual completo

1. Data Ingestion
   – Fuentes:
   • Polygon.io (OHLCV diario, 1m, trades/quotes selectivos).
   • DAS Trader Pro (Level 1 / Level 2 en vivo, prints ejecutables).
   • SEC filings (S-1, S-3, 424B, ATM) para detectar riesgo de dilución / float real / presión de oferta.
   • Norgate/Kibot para históricos de delisted y evitar sesgo de supervivencia.
   – Objetivo: tener visión realista del ecosistema small cap, incluyendo tickers ya muertos.

2. Preprocessing / Feature Engineering
   – Construcción de series informacionales:
   • Dollar Bars, Volume Bars, Imbalance Bars (según flujo de agresión de compra/venta).
   – Cálculo de features diarios:
   • RVOL, dollar volume diario, rotación de float, gap %, volatilidad intradía, “info_score”.
   – Cálculo de features intradía:
   • VWAP actual, volumen minuto vs media, distancia a premarket high, spread, halts recientes, SSR activo, disponibilidad de borrow.
   – Etiquetado supervisado (Triple Barrier):
   • Se etiqueta cada situación histórica como “éxito long”, “éxito short” o “fallo”, con horizonte y stop definidos.
   – Meta-labeling:
   • Segundo clasificador que aprende a filtrar cuáles de las señales base tienen probabilidad real de funcionar (reduce falsos positivos).

3. ML Scoring Layer
   – Modelos tipo LightGBM/XGBoost entrenados con esas features y etiquetas históricas.
   – Objetivo:
   • Clasificar la fase actual del ticker (impulso, distribución, colapso, rebote).
   • Estimar probabilidad de que un setup concreto (ej. First Red Day short) tenga edge en ese instante.
   – Salida de esta capa:
   • Ranking priorizado de tickers “interesantes ahora”.
   • Para cada ticker: probabilidad asociada a un playbook concreto.

   Nota importante: esta capa NO ejecuta todavía. Esta capa decide dónde mirar y qué playbook usar.

4. Strategy Evaluator (Reglas duras de ejecución)
   – Para cada ticker priorizado por ML se aplica la checklist estricta del setup operativo:
   Ejemplo Gap&Go long día 1:
   • Gap > +20 % vs. close previo.
   • Float < 20M.
   • Ruptura del premarket high con volumen > 2x media 5 velas 1m.
   • Spread < 0.03 USD.
   • Dollar volume último minuto ≥ umbral mínimo de liquidez.
   • Stop loss claro (VWAP o último pullback).
   • R:R ≥ 2:1 hacia nivel redondo cercano.
   – Si todas las condiciones son verdaderas → señal de “ENTRY_CONFIRMED” con parámetros concretos:
   precio de entrada, stop inicial, TP1, TP2, trailing.

   Aquí sucede la transición entre “esto se parece a un pump interesante” y “esta operación es ejecutable con riesgo definido ahora mismo”.

5. Risk Manager / Position Sizing
   – Controla:
   • tamaño máximo por operación,
   • número de posiciones simultáneas,
   • riesgo total abierto vs límite diario,
   • drawdown intradía,
   • disponibilidad de borrow si es short,
   • prohibición de operar durante o justo tras un halt.
   – Si el trade rompe los límites → se bloquea la ejecución aunque la señal sea perfecta.

   Esta capa es necesaria para pasar de semiautomático a automático. Es también la defensa regulatoria y operativa.

6. Execution Engine (integración con DAS Trader Pro)
   – Envía la orden de entrada (limit/market según el caso).
   – Coloca bracket completo:
   • stop loss inicial,
   • TP1 / TP2,
   • trailing stop que se ajusta al ir moviéndose el precio,
   • lógica de mover el stop al TP1 una vez alcanzado ese TP (heredado de tu planteamiento en futuros: al tocar TP1, mueves el stop al nivel de TP1 y activas trailing hacia TP2).
   – Mantiene y actualiza órdenes en vivo bajo control del Risk Manager.

   En modo totalmente automático, esta capa toma decisiones sin intervención humana. En modo semiautomático, puede esperar confirmación manual (“Send order”).

7. Trade Tracker / Logging / Governance
   – Registra cada paso:
   • timestamp de la señal,
   • features relevantes,
   • decisión del modelo,
   • checklist que justificó la entrada,
   • riesgo aprobado,
   • orden enviada,
   • fills reales,
   • PnL resultante.
   – Esto alimenta:
   • backtesting realista posterior,
   • mejora del clasificador,
   • reporting de métricas de riesgo,
   • documentación regulatory-style si alguna vez se escala capital externo.

#### 4.2 Relación con la web interna [www.tsis.ia](http://www.tsis.ia)

Esta arquitectura se expone operativamente a través de una web privada ([www.tsis.ia](http://www.tsis.ia)) que actúa como tu sala de control:

**Muestra en vivo el universo priorizado (tickers calientes).
– Para cada ticker:** fase actual, setup asignado, score ML, estado operativo (“WAIT”, “ENTRY_ARMED”, “ENTRY_CONFIRMED”).
**Presenta propuesta concreta:** entrada, stop, TP1, TP2, R:R estimado, riesgo en dólares.
**Muestra PnL abierto y riesgo consumido hoy.
– Registra y enseña todas las operaciones pasadas (auditoría).
– Puede ofrecer botones tipo “Ejecutar ahora”, o directamente ejecutar bajo modo ATS completo.

Esto separa visualización de inteligencia (dashboard) de la ejecución técnica (DAS). El cerebro está en TSIS, el brazo en DAS.

#### 4.3 Evolución en madurez operativa

Fase 1 – Científico / alerta
– TSIS identifica patrones y manda alertas con probabilidad y fase (“esto parece First Red Day short”).
– El trader humano decide y ejecuta en DAS manualmente.

Fase 2 – Semiautomático
– TSIS propone entrada completa (precio, stop, TP) y chequea microestructura.
– El dashboard permite “CONFIRMAR EJECUCIÓN”.
– El backend lanza la orden en DAS y coloca los brackets.
– Riesgo aún supervisado por humano.

Fase 3 – ATS completo
– TSIS ejecuta automáticamente cuando:**

1. el modelo prioriza el ticker,
2. la checklist técnica se cumple,
3. el Risk Manager lo aprueba,
4. los límites globales de riesgo diarios no se han roto.
   – Todas las órdenes y gestiones posteriores (mover stop, tomar TP parciales, cerrar al final del día) se gestionan sin intervención humana.
   – Se añaden kill switch y máxima transparencia histórica.

En esta fase, TSIS deja de ser solo un analista cuant y pasa a ser un sistema de trading algorítmico supervisable, con trazabilidad completa y disciplina de riesgo institucional.

5. Conclusión ejecutiva

6. Objetivo
   TSIS busca industrializar el edge en small caps: detectar fases de pump & dump, priorizar oportunidades, validar setups reales y ejecutar con disciplina cuantitativa reproducible.

7. Métrica de éxito
   Éxito no es “tener un modelo con F1 alto” ni “predecir pumps bonitos en retrospectiva”. Éxito es:
   – generar señales con alta precisión/recall,
   – convertir esas señales en entradas explotables con riesgo definido,
   – gestionarlas bajo un régimen de límites cuantificados,
   – registrar todo para poder auditar y mejorar,
   – y sostener beneficio ajustado por riesgo con drawdown controlado.

8. Alcance
   TSIS no es un juguete ni un simple screener. Es una plataforma completa con:
   – Data lake histórico libre de sesgo de supervivencia,
   – Feature engineering y labeling científico (Triple Barrier, Meta-Labeling, Sample Weights),
   – Motor de priorización ML en vivo,
   – Motor de validación táctica con reglas duras por setup,
   – Motor de riesgo,
   – Motor de ejecución conectado a DAS,
   – y capa de reporting y auditoría tipo mesa prop.

9. Arquitectura final
   El sistema se construye para operar en dos modos:
   – modo investigación/backtesting (offline, reproducible, profundo, 20 años),
   – modo operativo intradía (priorización en vivo, decisiones en ventanas de segundos-minutos, ejecución con control de riesgo).

La web interna [www.tsis.ia](http://www.tsis.ia) es la interfaz de mando que unifica todo: universo activo, fase de cada ticker, intención operativa, riesgo consumido, PnL, y justificación de cada acción.

5. Diferenciación clave
   TSIS separa claramente:
   – “detección / priorización” (ML probabilístico),
   de
   – “disparo / tamaño / gestión” (reglas duras + riesgo real).

Esto elimina el punto débil habitual de los traders discrecionales (entradas emocionales sin edge verificable) y también el punto débil habitual de los quants puros (modelos elegantes que no se pueden ejecutar de forma segura en el mercado real de low-float microcaps).

Esa separación es la base para escalar desde alerta cuantitativa, hacia un ATS auditado y defendible.


## BBDD TSIS (diseño integral de datos)

1. La definición completa de la BBDD TSIS (todas las capas, tablas lógicas, relaciones).
2. Dónde encaja exactamente dentro del documento ejecutivo anterior.

### Parte 1. diseño

La BBDD no es una única base. Es un ecosistema de datos con varias capas, cada una con una función clara: histórico científico, estado intradía, ejecución, y auditoría. Esto permite que TSIS sea reproducible, operable en vivo y auditable.

Estructura global propuesta:

```sh
TSIS_DATABASE
├── reference/                     → catálogos y metadatos estáticos
│   ├── tickers_dim.parquet
│   ├── exchanges_dim.parquet
│   └── sectors_dim.parquet
│
├── raw_market_data/               → datos brutos del mercado y regulatorios
│   ├── ohlcv_daily/               (histórico diario por ticker)
│   ├── ohlcv_intraday_1m/         (barras de 1 minuto)
│   ├── trades_tick/               (prints y agresión, subset profundo)
│   ├── quotes_tick/               (book / bid-ask / spread)
│   └── sec_filings/               (S-1, S-3, 424B, ATM, dilution risk)
│
├── processed_features/            → features calculadas y etiquetas
│   ├── daily_cache/               (rvol30, dollar_vol_d, float_rotation…)
│   ├── intraday_cache/            (VWAP vivo, dollar_vol_1m, spread_live…)
│   ├── informational_bars/        (Dollar Bars / Imbalance Bars / Volume Bars)
│   ├── labeling/                  (triple barrier, meta-label targets)
│   └── ml_dataset/                (dataset final entrenable + sample_weight)
│
├── signals_realtime/              → señales generadas en vivo por TSIS
│   ├── active_signals.parquet     (estado actual por ticker)
│   └── signals_history/           (histórico de todas las señales emitidas)
│
├── execution/                     → interacción real con DAS Trader Pro
│   ├── orders_log.parquet         (órdenes enviadas)
│   ├── fills_log.parquet          (ejecuciones reales)
│   ├── positions.parquet          (posiciones abiertas en vivo)
│   └── risk_limits.parquet        (estado de riesgo y límites vigentes)
│
├── analytics/                     → métricas de negocio y evaluación
│   ├── pnl_daily.parquet          (PnL agregado por día y por estrategia)
│   ├── strategy_stats/            (Sharpe, profit factor, DD, expectancy…)
│   ├── model_stats/               (precision, recall, F1, AUC, latencia…)
│   └── execution_latency/         (tiempo señal→orden→fill en ms)
│
└── governance/                    → trazabilidad y reproducibilidad
├── data_version.json          (hashes / integridad de datos usados)
├── model_registry.json        (qué modelo estaba activo cuándo)
└── pipeline_status.log        (estado de las pipelines Prefect/Airflow)
```

Te detallo cada bloque.

#### 1. reference/

Rol: catálogo maestro de entidades relativamente estables.

Ejemplo de tabla tickers_dim:

| ticker | name                  | exchange | ipo_date   | delisted | float_shares | shares_outstanding | sector     | last_filing |
| ------ | --------------------- | -------- | ---------- | -------- | ------------ | ------------------ | ---------- | ----------- |
| CREV   | Creative MedTech Inc. | NASDAQ   | 2021-04-12 | False    | 8,000,000    | 20,000,000         | Healthcare | 2023-11-09  |

Usos:
**Normalizar identificación de tickers a lo largo de todo el sistema.
– Detectar delisted / reverse split / cambios de ticker.
– Guardar float real y riesgo de dilución (SEC filings).
– Evitar sesgo de supervivencia en el backtest.

Actualización:** una vez al día y tras cada filing relevante.

#### 2. raw_market_data/

Rol: verdad cruda del mercado, sin interpretación.

Subcarpetas:
**ohlcv_daily/:** barras diarias históricas por ticker.
**ohlcv_intraday_1m/:** velas de 1 minuto históricas e intradía (Polygon).
**trades_tick/:** prints individuales, órdenes agresivas de compra/venta. Esto permite medir desequilibrio y absorción.
**quotes_tick/:** profundidad bid/ask, spread, liquidez disponible. Crítico para saber si una señal es ejecutable realmente.
**sec_filings/:** presentaciones S-1, S-3, 424B, ATM, etc. Estas filings anticipan dilución y presión de oferta. Influyen en si una subida es “real squeeze” o “pump para descargar papel”.

Esquema típico (ohlcv_intraday_1m):

| ticker | timestamp           | open  | high  | low   | close | volume | vwap  | dollar_vol | halt_flag | ssr_flag |
| ------ | ------------------- | ----- | ----- | ----- | ----- | ------ | ----- | ---------- | --------- | -------- |
| CREV   | 2023-11-10 09:31:00 | 40.20 | 42.00 | 39.90 | 41.80 | 123000 | 41.10 | 5.05e6     | False     | False    |

Notas:
**Esta capa no ejecuta lógica. Es base para reproducibilidad forense (“qué vio el sistema en ese momento?”).

#### 3. processed_features/

Rol:** transformar datos en información utilizable para ML y toma de decisión.

Incluye:

3.1 daily_cache/
Features calculadas a resolución diaria:
**rvol30:** volumen relativo 30 días.
**pctchg_d:** % cambio diario.
**dollar_vol_d:** suma(volume * price).
**float_rotation:** dollar_vol_d comparado con float.
**info_score:** ranking interno de “importancia especulativa” del ticker ese día.

3.2 intraday_cache/
Features intradía agregadas de la sesión en curso:
**VWAP actual.
– volumen_1m_vs_media_5m.
– spread_actual.
– distancia_a_premarket_high.
– distancia_a_VWAP.
– dollar_volume_last_1m.

Esto es lo que el ScannerService necesita en vivo para decir “esta acción está armando un Gap&Go long legítimo”, o “esto está entrando en First Red Day short”.

3.3 informational_bars/
Barras no temporales (Dollar Bars, Volume Bars, Imbalance Bars). En lugar de usar velas por tiempo fijo, se construyen velas cuando ha ocurrido suficiente “información de mercado” (por ejemplo, X dólares negociados, o cierto desequilibrio de agresión buy vs sell).
Esto es directamente la línea de López de Prado:** usar barras informacionales para extraer estructura real del flujo, no ruido por reloj.

3.4 labeling/
Etiquetas supervisadas para entrenamiento:
**Triple Barrier Labeling:** para cada “evento candidato” se define:
• barrera superior (take profit),
• barrera inferior (stop),
• límite temporal.
La etiqueta final es +1 / 0 / -1 según cuál barrera se toca primero.
Esto convierte cada posible entrada histórica en un resultado cuantificable.

**Meta-labeling:** segundo nivel. Se entrena un clasificador que decide cuándo NO tomar incluso una señal que, en bruto, parece buena. Esto sirve para filtrar setups mediocres y concentrarse en los de mayor edge.

3.5 ml_dataset/
Dataset final listo para entrenar modelos (LightGBM, XGBoost, etc.):
**features numéricas ya limpias,
– etiqueta objetivo (triple barrier / meta-label),
– sample_weight para evitar sobre-representar un único ticker con 200 repeticiones del mismo patrón en el mismo día.

#### 4. signals_realtime/

Rol:** la salida de inteligencia del motor TSIS en vivo. Es exactamente lo que alimenta la web interna ([www.tsis.ia](http://www.tsis.ia)).

active_signals.parquet (estado vivo actual del mercado):

| timestamp           | ticker | phase   | setup       | ml_score | status      | entry | stop | tp1  | rr_est | borrow_ok | spread_ok |
| ------------------- | ------ | ------- | ----------- | -------- | ----------- | ----- | ---- | ---- | ------ | --------- | --------- |
| 2025-10-26 15:32:11 | CREV   | impulse | Gap&Go long | 0.84     | ENTRY_ARMED | 2.37  | 2.19 | 2.65 | 2.1    | True      | True      |

Claves:
**phase:** en qué fase del ciclo pumpeo/dump está el ticker (impulso, distribución, colapso, rebote).
**setup:** qué playbook aplica (Gap&Go long día 1, First Red Day short, VWAP reclaim, Late Day Fade).
**ml_score:** probabilidad estimada por el modelo de que ese patrón tiene edge.
**status:** WAIT / ENTRY_ARMED / ENTRY_CONFIRMED / IN_POSITION.
**entry/stop/tp1:** la propuesta operativa concreta.
**rr_est:** ratio riesgo/beneficio directo estimado.

signals_history/
Histórico de todas las señales generadas, con marcas temporales.
Esto sirve como “caja negra”: se puede reconstruir qué sabía TSIS en cada instante de cada sesión.

#### 5. execution/

Rol: capa de realidad operativa. Lo que se ha hecho de verdad en el broker (DAS Trader Pro).

orders_log.parquet:

| order_id | timestamp_sent      | ticker | side | qty  | entry_price | stop_price | tp_price | status | approved_by | risk_profile_id |
| -------- | ------------------- | ------ | ---- | ---- | ----------- | ---------- | -------- | ------ | ----------- | --------------- |
| 98433    | 2025-10-26 15:32:14 | CREV   | BUY  | 2000 | 2.37        | 2.19       | 2.65     | SENT   | AUTO        | DAY_LIMIT_3     |

fills_log.parquet:

| fill_id | order_id | timestamp_fill      | fill_price | qty_filled | slippage |
| ------- | -------- | ------------------- | ---------- | ---------- | -------- |
| 10022   | 98433    | 2025-10-26 15:32:15 | 2.375      | 2000       | 0.005    |

positions.parquet:

| ticker | avg_price | qty_open | unrealized_pnl | stop_active | trailing_active |
| ------ | --------- | -------- | -------------- | ----------- | --------------- |
| CREV   | 2.375     | 2000     | +480           | True        | True            |

risk_limits.parquet:

| date       | max_positions | max_daily_loss | used_positions | used_dd | kill_switch_tripped |
| ---------- | ------------- | -------------- | -------------- | ------- | ------------------- |
| 2025-10-26 | 3             | -1500 USD      | 1              | -200    | False               |

Funciones clave:
**Permite imponer disciplina de riesgo:** “No más de X posiciones simultáneas”, “No más de Y pérdida diaria”, “Desconectar si DD supera límite”.
**Proporciona la trazabilidad necesaria cuando pasas a ejecución semiautomática o automática (ATS).

#### 6. analytics/

Rol:** medir rendimiento y degradación.

pnl_daily.parquet:

| date       | strategy          | gross_pnl | net_pnl | max_dd_day | sharpe_est | profit_factor |
| ---------- | ----------------- | --------- | ------- | ---------- | ---------- | ------------- |
| 2025-10-26 | Gap&Go_long       | +820      | +790    | -140       | 1.9        | 1.7           |
| 2025-10-26 | FirstRedDay_short | +0        | +0      | 0          | 0          | 0             |

Otros subconjuntos:
**strategy_stats/:** métricas agregadas por estrategia en ventanas móviles (win rate, expectancy, profit factor, drawdown).
**model_stats/:** precision, recall, F1, AUC, latencia del modelo en producción vs entrenamiento. Controla drift.
**execution_latency/:** mide cuánto tarda el pipeline desde “ENTRY_CONFIRMED” hasta “orden enviada” y desde “orden enviada” hasta “primer fill”. Esto detecta cuellos de botella y fricción con DAS.

Esta capa valida si el sistema está funcionando como se diseñó en el documento de negocio. Es la base para decidir si escalar capital o pausar una estrategia.

#### 7. governance/

Rol: reproducibilidad científica y defensa legal/auditoría.

**data_version.json
Guarda hashes de los datasets utilizados en cada sesión de trading / de backtest. Permite demostrar que los resultados no fueron manipulados ex post.

– model_registry.json
Indica qué versión concreta de cada modelo ML estaba activa en cada momento, con fecha de despliegue, hiperparámetros principales y objetivo.
Esto es crítico si dos modelos generan señales distintas en días distintos:** necesitas saber quién tomó la decisión.

**pipeline_status.log
Última ejecución correcta de cada pipeline de ingesta, etiquetado, features, scoring en vivo, etc. Esto permite detectar fallos silenciosos (por ejemplo:** “no se actualizaron las features intradía de 10:15 a 10:22, por tanto las señales durante ese intervalo quedan marcadas como potencialmente incompletas”).

Este bloque convierte TSIS en un sistema auditable tipo mesa prop. No es opcional una vez entras en modo ejecución automática.

Relaciones lógicas entre capas

Podemos verlo como una cadena:

tickers_dim
→ raw_market_data (ohlcv_intraday_1m, trades_tick, etc.)
→ processed_features (daily_cache, intraday_cache, informational_bars)
→ labeling (triple barrier, meta-label targets)
→ ml_dataset (dataset entrenable con sample_weight)
→ signals_realtime (active_signals / signals_history)
→ execution (orders_log, fills_log, positions, risk_limits)
→ analytics (pnl_daily, model_stats, execution_latency)
→ governance (model_registry, data_version)

Esto da dos cosas:
**Reproducibilidad forense:** “Por qué entramos en CREV a las 10:32?” → puedes trazarlo hacia atrás hasta las features exactas que vio el modelo.
**Mejora continua:** “Qué setups están muriendo? Cuál mantiene profit factor?”

### Parte 2. En qué punto del documento ejecutivo anterior debe ir la BBDD

La definición de la BBDD TSIS es una capa transversal de toda la arquitectura. Pero a efectos de documento directivo, la debes insertar como una nueva Sección 5 entre “High-level design — Arquitectura de alto nivel” y “Conclusión ejecutiva”.

Tu documento quedaría estructurado así:

1. Clarify objectives
2. Define metrics
3. Constraints and scope
4. High-level design
5. Data Architecture / BBDD TSIS   ← ESTA SECCIÓN
6. Conclusión ejecutiva

Por qué va en la sección 5 y no mezclado antes:

**Las secciones 1–4 definen el “qué” (objetivo, métricas, límites operativos, arquitectura funcional:** ingestión → modelo → ejecución).
**La BBDD define el “cómo lo sostenemos y auditamos en el tiempo”:** dónde viven los datos crudos, cómo se convierten en features, cómo se generan señales, cómo registramos ejecuciones, cómo medimos el edge y cómo garantizamos reproducibilidad científica y responsabilidad operativa.

Después de la sección 5 ya puedes cerrar con la Conclusión ejecutiva (sección 6), porque ahí enlazas todo: visión de negocio + métrica + operativa + stack técnico + gobernanza.

Resumen

**La BBDD TSIS no es solo almacenamiento. Es la base para:**
• entrenamiento (backtesting científico),
• operación intradía (scoring + signals_realtime),
• ejecución en broker (execution/),
• auditoría y mejora (analytics/ y governance/).

**Esta BBDD habilita tanto el modo “alerta semiautomática” como el modo “ATS completamente autónomo”.
Sin esta capa de datos versionada y trazable, el ATS sería inaceptable desde el punto de vista de riesgo y no escalable.

– En el documento final, la BBDD debe aparecer como Sección 5:** “Data Architecture / BBDD TSIS”, inmediatamente después de la arquitectura de alto nivel.
