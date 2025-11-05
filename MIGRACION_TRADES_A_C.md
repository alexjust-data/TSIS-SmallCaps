# Plan de Migración: Trades Tick-Level de D:/ a C:/

**Fecha:** 2025-11-03
**Razón:** Espacio insuficiente en D:/ para completar la descarga

---

## Situación Actual

### Espacio en Disco
- **D:/ (actual)**
  - Total: 931.2 GB
  - Usado: 878.8 GB
  - **Libre: 52.3 GB** ⚠️ INSUFICIENTE

- **C:/ (destino)**
  - Total: 1,861.9 GB
  - Usado: 682.0 GB
  - **Libre: 1,179.9 GB** ✅ SUFICIENTE

### Datos de Descarga
- Tickers completados: ~921 / 6,405
- Espacio usado actual: ~16 GB
- **Espacio total estimado: ~109 GB**
- **Espacio pendiente: ~93 GB**
- **Conclusión: D:/ se quedará sin espacio (faltarían ~40 GB)**

---

## Estrategia: Enlace Simbólico (Junction)

### Ventajas
1. ✅ **Transparente**: Todo el código sigue funcionando sin cambios
2. ✅ **Sin modificar configuración**: Los scripts siguen apuntando a `D:\TSIS_SmallCaps\raw\polygon\trades_ticks`
3. ✅ **Reanudable**: La descarga continúa con `--resume` sin redescargar nada
4. ✅ **Verificable**: Fácil de probar que funciona correctamente

### Cómo Funciona
Un enlace simbólico (junction en Windows) es como un "atajo transparente":
```
D:\TSIS_SmallCaps\raw\polygon\trades_ticks  -->  C:\TSIS_Data\trades_ticks
         (enlace simbólico)                            (datos reales)
```

Cuando cualquier programa accede a `D:\...\trades_ticks`, Windows automáticamente redirige a `C:\TSIS_Data\trades_ticks`.

---

## Plan de Migración Paso a Paso

### PASO 1: Preparación
```powershell
# Verificar espacio disponible en C:/
Get-PSDrive C | Select-Object Name, @{Name="FreeGB";Expression={[math]::Round($_.Free/1GB,2)}}

# Verificar que el proceso de descarga está corriendo
Get-Process python | Where-Object {$_.CommandLine -like "*batch_trades_wrapper*"}
```

**Checkpoint:** Confirmar que C:/ tiene >100 GB libres

---

### PASO 2: Detener la Descarga Actual
```powershell
# Obtener todos los procesos wrapper de Python
$wrappers = Get-Process python -ErrorAction SilentlyContinue | Where-Object {
    $_.CommandLine -like "*batch_trades_wrapper*"
}

# Mostrar cuántos procesos se van a detener
Write-Host "Procesos a detener: $($wrappers.Count)" -ForegroundColor Yellow

# Detener los procesos
$wrappers | Stop-Process -Force

# Esperar a que terminen
Start-Sleep -Seconds 5

# Verificar que ya no hay procesos corriendo
Get-Process python -ErrorAction SilentlyContinue | Where-Object {
    $_.CommandLine -like "*batch_trades_wrapper*"
}
```

**Checkpoint:** No debe haber procesos wrapper corriendo

---

### PASO 3: Crear Directorio en C:/
```powershell
# Crear el directorio destino
New-Item -ItemType Directory -Path "C:\TSIS_Data" -Force

# Verificar que se creó
Test-Path "C:\TSIS_Data"
```

**Checkpoint:** Debe retornar `True`

---

### PASO 4: Mover los Datos (TOMA TIEMPO)
```powershell
# Definir rutas
$source = "D:\TSIS_SmallCaps\raw\polygon\trades_ticks"
$target = "C:\TSIS_Data\trades_ticks"

Write-Host "Iniciando movimiento de datos..." -ForegroundColor Cyan
Write-Host "Origen: $source" -ForegroundColor Yellow
Write-Host "Destino: $target" -ForegroundColor Yellow
Write-Host ""
Write-Host "NOTA: Este proceso tomará varios minutos (~16 GB, ~1M archivos)" -ForegroundColor Yellow
Write-Host ""

# Mover el directorio completo
Move-Item $source $target -Force

Write-Host "Movimiento completado" -ForegroundColor Green
```

**Tiempo estimado:** 10-15 minutos
**Checkpoint:** Verificar que `C:\TSIS_Data\trades_ticks` existe y contiene datos

```powershell
# Verificar que el destino existe
Test-Path "C:\TSIS_Data\trades_ticks"

# Contar algunos subdirectorios (debe haber ~921 tickers)
(Get-ChildItem "C:\TSIS_Data\trades_ticks" -Directory |
    Where-Object {$_.Name -ne "_batch_temp"}).Count
```

---

### PASO 5: Crear Enlace Simbólico
```powershell
# Crear el junction (enlace simbólico de directorio)
New-Item -ItemType Junction -Path $source -Target $target -Force

Write-Host "Enlace simbólico creado:" -ForegroundColor Green
Write-Host "  $source --> $target" -ForegroundColor Cyan
```

**Checkpoint:** Verificar que el enlace funciona

```powershell
# Probar que el enlace funciona
Test-Path "D:\TSIS_SmallCaps\raw\polygon\trades_ticks\_batch_temp"

# Debe retornar True
# Los datos están en C:/ pero se acceden desde D:/
```

---

### PASO 6: Verificar Integridad
```powershell
# Contar tickers en ambas rutas (deben ser iguales)
$count_d = (Get-ChildItem "D:\TSIS_SmallCaps\raw\polygon\trades_ticks" -Directory |
    Where-Object {$_.Name -ne "_batch_temp"}).Count

$count_c = (Get-ChildItem "C:\TSIS_Data\trades_ticks" -Directory |
    Where-Object {$_.Name -ne "_batch_temp"}).Count

Write-Host "Tickers en D:/ (enlace): $count_d" -ForegroundColor Cyan
Write-Host "Tickers en C:/ (real):   $count_c" -ForegroundColor Cyan

if ($count_d -eq $count_c) {
    Write-Host "✓ Verificación exitosa" -ForegroundColor Green
} else {
    Write-Host "✗ ERROR: Conteos no coinciden" -ForegroundColor Red
}
```

**Checkpoint:** Los conteos deben ser idénticos (~921)

---

### PASO 7: Reiniciar la Descarga
```powershell
# Navegar al directorio del proyecto
cd "D:\TSIS_SmallCaps"

# Reiniciar el launcher (usará --resume automáticamente)
.\scripts\01_agregation_OHLCV\launch_trades_wrapper.ps1
```

**Resultado esperado:**
- El wrapper detectará que ya hay ~921 tickers descargados
- Continuará desde el ticker 922 en adelante
- Los nuevos datos se escribirán en C:/ (vía el enlace en D:/)

---

## Verificación Post-Migración

### 1. Verificar que la descarga está corriendo
```powershell
Get-Process python | Where-Object {$_.CommandLine -like "*batch_trades_wrapper*"}
```

### 2. Verificar que los datos se escriben en C:/
```powershell
# Ver los archivos más recientes en C:/
Get-ChildItem "C:\TSIS_Data\trades_ticks" -Recurse -File -Filter "*.parquet" |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 10 FullName, LastWriteTime
```

### 3. Monitorear espacio en C:/
```powershell
# Ejecutar cada 5 minutos para ver el crecimiento
while ($true) {
    $drive = Get-PSDrive C
    $free_gb = [math]::Round($drive.Free/1GB, 2)
    $time = Get-Date -Format "HH:mm:ss"
    Write-Host "[$time] C:/ libre: $free_gb GB" -ForegroundColor Cyan
    Start-Sleep -Seconds 300  # 5 minutos
}
```

---

## Script Completo Automatizado

Guarda esto como `migrate_to_c.ps1`:

```powershell
# migrate_to_c.ps1
# Migración de trades_ticks de D:/ a C:/ con enlace simbólico

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  MIGRACION TRADES_TICKS: D:/ --> C:/" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

# PASO 1: Verificar espacio
Write-Host "PASO 1: Verificando espacio disponible..." -ForegroundColor Yellow
$drive_c = Get-PSDrive C
$free_gb = [math]::Round($drive_c.Free/1GB, 2)
Write-Host "  C:/ Espacio libre: $free_gb GB" -ForegroundColor Cyan

if ($free_gb -lt 100) {
    Write-Host "ERROR: Espacio insuficiente en C:/ (se necesitan >100 GB)" -ForegroundColor Red
    exit 1
}
Write-Host "  ✓ Espacio suficiente" -ForegroundColor Green
Write-Host ""

# PASO 2: Detener wrapper
Write-Host "PASO 2: Deteniendo procesos de descarga..." -ForegroundColor Yellow
$wrappers = Get-Process python -ErrorAction SilentlyContinue | Where-Object {
    $_.CommandLine -like "*batch_trades_wrapper*"
}

if ($wrappers) {
    Write-Host "  Deteniendo $($wrappers.Count) proceso(s)..." -ForegroundColor Yellow
    $wrappers | Stop-Process -Force
    Start-Sleep -Seconds 5
    Write-Host "  ✓ Procesos detenidos" -ForegroundColor Green
} else {
    Write-Host "  No hay procesos corriendo" -ForegroundColor Yellow
}
Write-Host ""

# PASO 3: Crear directorio destino
Write-Host "PASO 3: Creando directorio en C:/..." -ForegroundColor Yellow
New-Item -ItemType Directory -Path "C:\TSIS_Data" -Force | Out-Null
Write-Host "  ✓ Directorio creado: C:\TSIS_Data" -ForegroundColor Green
Write-Host ""

# PASO 4: Mover datos
Write-Host "PASO 4: Moviendo datos (esto tomará varios minutos)..." -ForegroundColor Yellow
$source = "D:\TSIS_SmallCaps\raw\polygon\trades_ticks"
$target = "C:\TSIS_Data\trades_ticks"

if (Test-Path $source) {
    Write-Host "  Moviendo: $source" -ForegroundColor Cyan
    Write-Host "  Destino:  $target" -ForegroundColor Cyan
    Write-Host "  Estimado: 10-15 minutos (~16 GB, ~1M archivos)" -ForegroundColor Yellow
    Write-Host ""

    Move-Item $source $target -Force
    Write-Host "  ✓ Datos movidos exitosamente" -ForegroundColor Green
} else {
    Write-Host "  ERROR: No se encontró $source" -ForegroundColor Red
    exit 1
}
Write-Host ""

# PASO 5: Crear enlace simbólico
Write-Host "PASO 5: Creando enlace simbólico..." -ForegroundColor Yellow
New-Item -ItemType Junction -Path $source -Target $target -Force | Out-Null
Write-Host "  ✓ Enlace creado: $source --> $target" -ForegroundColor Green
Write-Host ""

# PASO 6: Verificar
Write-Host "PASO 6: Verificando integridad..." -ForegroundColor Yellow
if (Test-Path "$source\_batch_temp") {
    $count = (Get-ChildItem $source -Directory |
        Where-Object {$_.Name -ne "_batch_temp"}).Count
    Write-Host "  ✓ Enlace funciona correctamente" -ForegroundColor Green
    Write-Host "  Tickers encontrados: $count" -ForegroundColor Cyan
} else {
    Write-Host "  ERROR: Verificación falló" -ForegroundColor Red
    exit 1
}
Write-Host ""

# PASO 7: Resumen final
Write-Host "============================================================" -ForegroundColor Green
Write-Host "  MIGRACION COMPLETA" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Datos migrados a: C:\TSIS_Data\trades_ticks" -ForegroundColor Cyan
Write-Host "Enlace simbólico: D:\TSIS_SmallCaps\raw\polygon\trades_ticks" -ForegroundColor Cyan
Write-Host ""
Write-Host "Próximos pasos:" -ForegroundColor Yellow
Write-Host "  1. Reiniciar la descarga: .\scripts\01_agregation_OHLCV\launch_trades_wrapper.ps1" -ForegroundColor White
Write-Host "  2. La descarga continuará automáticamente con --resume" -ForegroundColor White
Write-Host "  3. Los datos se escribirán en C:/ de forma transparente" -ForegroundColor White
Write-Host ""
```

---

## Rollback (Si algo sale mal)

Si necesitas revertir la migración:

```powershell
# 1. Detener descarga
Get-Process python | Where-Object {$_.CommandLine -like "*batch_trades_wrapper*"} | Stop-Process -Force

# 2. Eliminar enlace simbólico (NO elimina los datos)
Remove-Item "D:\TSIS_SmallCaps\raw\polygon\trades_ticks" -Force

# 3. Mover datos de vuelta
Move-Item "C:\TSIS_Data\trades_ticks" "D:\TSIS_SmallCaps\raw\polygon\trades_ticks" -Force

# 4. Reiniciar descarga
.\scripts\01_agregation_OHLCV\launch_trades_wrapper.ps1
```

---

## Preguntas Frecuentes

### ¿Se perderán datos durante la migración?
No. `Move-Item` mueve los archivos físicamente, no los copia y elimina. Es una operación atómica y segura.

### ¿Cuánto tiempo toma la migración?
- Detener procesos: ~5 segundos
- Mover datos: ~10-15 minutos (depende de la velocidad del disco)
- Crear enlace: <1 segundo
- Total: ~15-20 minutos

### ¿El código necesita modificarse?
No. El enlace simbólico hace que todo sea transparente. Los scripts siguen usando la misma ruta `D:\TSIS_SmallCaps\raw\polygon\trades_ticks`.

### ¿Qué pasa si D:/ y C:/ están en discos físicos diferentes?
Funciona igual. Los enlaces simbólicos funcionan entre particiones y discos físicos diferentes en Windows.

### ¿Puedo hacer esto sin detener la descarga?
No es recomendable. Si mueves datos mientras se están escribiendo, podrías corromper archivos. Es mejor detener, migrar, y reiniciar con `--resume`.

### ¿Cómo verifico que el enlace está funcionando?
```powershell
# Ver las propiedades del enlace
Get-Item "D:\TSIS_SmallCaps\raw\polygon\trades_ticks" | Select-Object *

# El campo "Target" debe mostrar: C:\TSIS_Data\trades_ticks
```

---

## Contacto y Soporte

Si encuentras algún problema durante la migración:
1. NO borres ningún dato
2. Verifica que los datos estén en `C:\TSIS_Data\trades_ticks`
3. Revisa los logs en `raw/polygon/trades_ticks/_batch_temp/`
4. Si necesitas ayuda, guarda una copia de los logs

---

**Documento creado:** 2025-11-03
**Última actualización:** 2025-11-03
**Versión:** 1.0
