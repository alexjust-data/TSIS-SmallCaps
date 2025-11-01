# 07_Short_Plays 

## 1. Introducción y contexto general

En esta sesión se explican los *setups* ideales para las **estrategias en corto** del *playbook* de *Latin Day Trading*. A diferencia de las estrategias largas, aquí se detallan los patrones de agotamiento, rechazo y reversión.

Se cubren los siguientes patrones:

* **Green to Red**
* **First Red Day**
* **Swing del primer día rojo**
* **Bull Trap**
* **Overextended Gap Down**
* **Short into Resistance**
* **Gap and Grab**
* **Gap and Extension**
* **VWAP Rejection**
* **Late Day Fade**

> De todos ellos, los más importantes son: **First Red Day**, **Overextended Gap Down** y **Late Day Fade**.

---

## 2. Criterios principales para abrir *shorts*

1. **Sobreextensión:**

   * Si no hay sobreextensión, no se debe abrir un corto.
   * Mínimo 50–60% de subida desde el inicio de la corrida.
   * Cuanto más breve el período de la corrida, mejor.
   * Ejemplo: si la acción sube solo 10–12%, no es ideal.

2. **Dilución activa:**

   * Buscar *filings* como **S-3, 424B, ATM, warrants, convertible notes/debt** o **offerings**.
   * Estas empresas suelen carecer de efectivo y tener **operating cash flow negativo**.

3. **Overhead resistance:**

   * Debe haber resistencias visibles en el gráfico (no estar en *all-time highs*).
   * Idealmente, el gráfico se ve “destruido” con picos de volumen histórico en zonas previas.

4. **Float adecuado:**

   * Evitar *micro-floats* (< 5 millones). Son difíciles de controlar y propensos a *squeezes*.

---

## 3. Patrones principales de *short*

### 3.1 First Red Day (FRD)

Es el primer día de debilidad tras una corrida de varios días verdes. Representa la primera señal de agotamiento.

* **Confirmaciones clave:**

  * Pérdida del control de los compradores.
  * Volumen del día rojo menor al del último día verde.
  * Idealmente, ocurre tras una subida >50% en pocos días.

* **Variantes:**

  1. **FRD con Gap Up** → la acción abre por encima del cierre anterior, hace *green to red* y se destruye.
  2. **FRD sin Gap** → abre al mismo nivel del cierre previo, mantiene estructura lateral, luego gira.
  3. **FRD con Gap Down** → abre 10–15% por debajo del cierre anterior, mostrando pérdida inmediata de atención.

* **Ejemplo operativo:**

  * Entrada en el *green to red*.
  * *Stop loss* unos centavos por encima del *open*.
  * Confirmación: volumen decreciente + pérdida del VWAP.

> “El primer día rojo es esencial para entender el lado corto.”

---

### 3.2 Overextended Gap Down (OGD)

Un patrón de **gap bajista en una acción extremadamente sobreextendida**. El precio abre por debajo del cierre anterior y no logra recuperarlo.

* **Condiciones:**

  * Corrida previa de varios días o subida superior al 100%.
  * Gap Down ≥ 8–10%.
  * Volumen decreciente.

* **Ejecución:**

  * Entrada en los *spikes* hacia el cierre anterior (*previous close*).
  * *Stop loss* unos centavos por encima del *close* anterior.
  * Se puede construir posición (*frontside short*) antes de que se confirme la debilidad.

* **Detalles técnicos:**

  * Puede activarse la regla SSR (Short Sale Restriction) si el precio cae más de 10% respecto al cierre previo.
  * En ese caso, no se puede atacar el *bid*, favoreciendo a los *longs*.

> Es el único patrón donde se puede *shortear el frontside* con convicción, siempre respetando el *previous close* como resistencia crítica.

---

### 3.3 Short into Resistance (SIR)

Patrón donde la acción intenta romper una **resistencia previa** y falla.

* **Condiciones:**

  * Acción sobreextendida, con *high* anterior bien definido.
  * Volumen decreciente al aproximarse al *high*.
  * El *high of day anterior* actúa como techo.

* **Ejecución:**

  * Entrada al primer rechazo del *high*.
  * *Stop loss* justo por encima del *high*.
  * Confirmar que el volumen no supere el del día anterior.

> Es un patrón más avanzado, considerado *frontside short*. Requiere lectura precisa del volumen y paciencia.

---

### 3.4 Late Day Fade (LDF)

Ocurre cuando una acción verde pierde fuerza en la tarde, rompiendo las **J-Lines** o el **VWAP**, con volumen decreciente.

* **Condiciones:**

  * Acción con gran sobreextensión (>100%).
  * Se mantiene alcista toda la mañana respetando VWAP y J-Lines.
  * Entre 13:30–15:00 (hora NY), el precio rompe esos niveles.

* **Ejecución:**

  * Entrada al romper VWAP o J-Lines.
  * *Stop loss* ligeramente por encima del nivel roto.
  * *Take profit* hacia los mínimos del día.

* **Timing:**

  * La caída suele durar de 1 a 3 horas.
  * No operar este patrón después de las 15:30.

> Es uno de los *setups* más difíciles: requiere disciplina y evitar *shortear* acciones fuertes en su primer día verde.

---

## 4. Patrones complementarios / micro-setups

### 4.1 Gap and Crap

Apertura con **gap up fuerte** que se revierte rápidamente durante el día.

* Volumen alto al inicio y drenaje posterior.
* Se trata igual que un *First Red Day*.
* El VWAP y las J-Lines actúan como guías visuales de agotamiento.

### 4.2 Gap and Extension (Stuff Move)

Rechazo violento en el **pre-market high** (Bull Trap del pre-market).

* Aparece una gran vela roja tras intento fallido de romper el máximo.
* Se produce un rechazo fuerte y se forma el *all-day fade*.
* Confirmar con barra de volumen igual o menor que la del impulso previo.

### 4.3 Bull Trap

*Breakout* fallido que atrapa a los compradores.

* Ocurre cuando la acción supera una resistencia y rápidamente se revierte.
* Genera venta masiva de los *longs* atrapados.
* Se detecta por una mecha superior larga y volumen alto de rechazo.

> Es un micro-patrón técnico, útil para evitar entrar *long* en zonas peligrosas.

### 4.4 Green to Red

Cambio intradía de vela verde a roja (del *open* hacia abajo).

* Señal de pérdida de momentum.
* No es un *setup* independiente, sino una **confirmación** dentro de un patrón mayor (FRD o Gap & Crap).

### 4.5 VWAP Rejection

Rechazo repetido del **VWAP** como resistencia dinámica.

* Ideal cuando VWAP está cercano al *open*.
* Si el volumen decrece y no logra superar VWAP, indica control de vendedores.
* Si el volumen crece, puede implicar un *Red to Green reversal*.

---

## 5. Swing del Primer Día Rojo (continuidad del FRD)

Algunos traders mantienen posición *short* desde el cierre del primer día rojo buscando continuidad.

> Sin embargo, esta práctica es extremadamente riesgosa. Un *gap up* posterior puede causar pérdidas totales (>100%).

---

## 6. Discusión y observaciones finales

* Los *shorts* más eficientes ocurren tras sobreextensión y agotamiento del volumen.
* Evitar acciones con *float* bajo o catalizadores activos.
* Observar las reglas SSR y el comportamiento frente al VWAP.
* Cada patrón tiene variantes, pero los principios clave son siempre:

  1. **Sobreextensión previa.**
  2. **Pérdida de interés.**
  3. **Volumen decreciente.**
  4. **Confirmación técnica (VWAP, J-Lines, High/Close previo).**

> La consistencia proviene de respetar el *setup* grande (big picture) y no forzar anticipaciones.

---

## 7. Glosario técnico breve

| Término                 | Significado                                                               |
| ----------------------- | ------------------------------------------------------------------------- |
| **VWAP**                | Volume Weighted Average Price; promedio ponderado del precio por volumen. |
| **J-Lines**             | Medias móviles de corto plazo usadas como guías dinámicas de tendencia.   |
| **SSR**                 | Short Sale Restriction; restricción de venta corta activada al caer >10%. |
| **Overhead resistance** | Zona de resistencia creada por volúmenes previos o *bagholders*.          |
| **Frontside short**     | Venta anticipada antes de que se confirme la debilidad.                   |
| **All Day Fade**        | Pérdida gradual de precio durante toda la sesión tras el impulso inicial. |

---

## 8. Conclusión

Los *short plays* constituyen el núcleo de la operativa profesional en *small caps* sobreextendidas. Los patrones **First Red Day**, **Overextended Gap Down** y **Late Day Fade** ofrecen los escenarios más claros y repetibles.

> “El objetivo no es adivinar el techo, sino reconocer el momento en que el mercado deja de tener interés.”

---
