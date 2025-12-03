"""
Script para identificar y matar el proceso viejo de batch_trades_wrapper
"""
import psutil
import sys
from datetime import datetime

def find_and_kill_old_wrapper():
    wrapper_processes = []

    # Buscar todos los procesos Python
    for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'create_time']):
        try:
            if proc.info['name'] == 'python.exe' and proc.info['cmdline']:
                cmdline = ' '.join(proc.info['cmdline'])

                # Buscar procesos que sean batch_trades_wrapper
                if 'batch_trades_wrapper.py' in cmdline:
                    create_time = datetime.fromtimestamp(proc.info['create_time'])

                    # Determinar el rango basado en la línea de comando
                    if '--from 2004-01-01' in cmdline or '--from 2004' in cmdline:
                        range_type = "VIEJO (2004-2018)"
                    elif '--from 2007-01-01' in cmdline or '--from 2007' in cmdline:
                        range_type = "NUEVO (2007-2025)"
                    else:
                        range_type = "DESCONOCIDO"

                    wrapper_processes.append({
                        'pid': proc.info['pid'],
                        'create_time': create_time,
                        'range_type': range_type,
                        'cmdline': cmdline[:200]  # Primeros 200 chars
                    })
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    if not wrapper_processes:
        print("No se encontraron procesos batch_trades_wrapper")
        return

    # Ordenar por tiempo de creación (más viejo primero)
    wrapper_processes.sort(key=lambda x: x['create_time'])

    print(f"\nSe encontraron {len(wrapper_processes)} procesos wrapper:")
    print("=" * 100)
    for i, proc in enumerate(wrapper_processes, 1):
        print(f"\n{i}. PID: {proc['pid']}")
        print(f"   Tipo: {proc['range_type']}")
        print(f"   Creado: {proc['create_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   Comando: {proc['cmdline'][:150]}...")

    # Encontrar el proceso VIEJO
    old_processes = [p for p in wrapper_processes if "VIEJO" in p['range_type']]

    if not old_processes:
        print("\n❌ No se encontró proceso VIEJO (2004-2018)")
        return

    if len(old_processes) > 1:
        print(f"\n⚠️  Se encontraron {len(old_processes)} procesos VIEJOS. Matando todos...")

    # Matar proceso(s) VIEJO(S)
    for old_proc in old_processes:
        try:
            pid = old_proc['pid']
            proc = psutil.Process(pid)

            # Matar el proceso principal
            print(f"\n🔪 Matando proceso PID {pid} ({old_proc['range_type']})...")
            proc.kill()

            # Esperar a que termine
            proc.wait(timeout=5)
            print(f"✅ Proceso {pid} terminado exitosamente")

        except psutil.NoSuchProcess:
            print(f"⚠️  Proceso {pid} ya no existe")
        except psutil.TimeoutExpired:
            print(f"⚠️  Proceso {pid} no terminó en 5 segundos")
        except Exception as e:
            print(f"❌ Error matando proceso {pid}: {e}")

if __name__ == "__main__":
    find_and_kill_old_wrapper()
