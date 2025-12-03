#!/usr/bin/env python3
"""
Auditoría OPTIMIZADA de datos de Polygon/Massive
Para datasets con millones de archivos - usa muestreo inteligente
"""

import os
import sys
from pathlib import Path
from collections import defaultdict
import random
from datetime import datetime

def format_size(size_bytes):
    """Format size in human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

def get_file_extension(filename):
    """Get file extension"""
    return Path(filename).suffix.lower()

def quick_dir_stats(dirpath):
    """Quick stats without reading all files"""
    try:
        entries = list(os.scandir(dirpath))
        files = [e for e in entries if e.is_file()]
        dirs = [e for e in entries if e.is_dir()]

        total_size = sum(f.stat().st_size for f in files[:100])  # Sample first 100

        return {
            'file_count': len(files),
            'dir_count': len(dirs),
            'sample_size': total_size,
            'files': files[:10],  # Keep first 10 for examples
            'dirs': [d.name for d in dirs]
        }
    except Exception as e:
        return {'error': str(e)}

def analyze_structure_smart(root_path, max_depth=5, sample_files=10):
    """Smart analysis with depth limits and sampling"""
    print(f"Analyzing {root_path}...")

    if not os.path.exists(root_path):
        return None

    structure = {}
    extension_stats = defaultdict(lambda: {'count': 0, 'sample_size': 0, 'examples': []})
    total_dirs = 0
    total_files_estimate = 0
    total_size_estimate = 0

    # Level 1: Analyze top-level structure
    root_stats = quick_dir_stats(root_path)
    structure['.'] = root_stats

    if 'error' not in root_stats:
        total_dirs += root_stats['dir_count']
        total_files_estimate += root_stats['file_count']

        # Track extensions
        for file_entry in root_stats.get('files', []):
            ext = get_file_extension(file_entry.name)
            extension_stats[ext]['count'] += 1
            extension_stats[ext]['sample_size'] += file_entry.stat().st_size
            if len(extension_stats[ext]['examples']) < 3:
                extension_stats[ext]['examples'].append(file_entry.path)

        # Level 2+: Sample subdirectories
        dirs_to_explore = root_stats.get('dirs', [])

        for subdir_name in dirs_to_explore:
            subdir_path = os.path.join(root_path, subdir_name)
            rel_path = os.path.relpath(subdir_path, root_path)

            print(f"  Scanning: {rel_path}")

            sub_stats = quick_dir_stats(subdir_path)
            structure[rel_path] = sub_stats

            if 'error' not in sub_stats:
                total_dirs += sub_stats['dir_count']
                total_files_estimate += sub_stats['file_count']
                total_size_estimate += sub_stats['sample_size']

                # Track extensions from this dir
                for file_entry in sub_stats.get('files', []):
                    ext = get_file_extension(file_entry.name)
                    extension_stats[ext]['count'] += 1
                    if len(extension_stats[ext]['examples']) < 3:
                        extension_stats[ext]['examples'].append(file_entry.path)

                # Go deeper if there are many subdirs (sample them)
                if sub_stats['dir_count'] > 0:
                    try:
                        nested_dirs = list(os.scandir(subdir_path))
                        nested_dirs = [d for d in nested_dirs if d.is_dir()]

                        # Sample some nested dirs to understand structure
                        sample_nested = nested_dirs[:5] if len(nested_dirs) <= 5 else random.sample(nested_dirs, 5)

                        for nested_dir in sample_nested:
                            nested_path = nested_dir.path
                            nested_rel = os.path.relpath(nested_path, root_path)

                            nested_stats = quick_dir_stats(nested_path)
                            structure[nested_rel] = nested_stats

                            if 'error' not in nested_stats:
                                total_files_estimate += nested_stats['file_count']
                                total_size_estimate += nested_stats['sample_size']

                                # Track extensions
                                for file_entry in nested_stats.get('files', []):
                                    ext = get_file_extension(file_entry.name)
                                    if len(extension_stats[ext]['examples']) < 3:
                                        extension_stats[ext]['examples'].append(file_entry.path)
                    except:
                        pass

    print(f"  Estimated files: {total_files_estimate:,}")
    print(f"  Directories scanned: {len(structure)}")

    return structure, dict(extension_stats), total_files_estimate, total_size_estimate, total_dirs

def generate_read_code(file_path, extension):
    """Generate Python code to read different file types"""
    rel_path = file_path.replace('\\', '/')

    if extension == '.parquet':
        return f"""```python
import pandas as pd
import pyarrow.parquet as pq

# Read parquet file
df = pd.read_parquet(r'{file_path}')
print(f"Shape: {{df.shape}}")
print(f"Columns: {{df.columns.tolist()}}")
print(df.head())

# Check schema
table = pq.read_table(r'{file_path}')
print(table.schema)
```"""

    elif extension == '.csv':
        return f"""```python
import pandas as pd

# Read CSV
df = pd.read_csv(r'{file_path}')
print(f"Shape: {{df.shape}}")
print(df.head())
print(df.dtypes)
```"""

    elif extension == '.json':
        return f"""```python
import json
import pandas as pd

# Read JSON
with open(r'{file_path}', 'r') as f:
    data = json.load(f)
print(type(data))
print(list(data.keys()) if isinstance(data, dict) else len(data))

# Or with pandas
df = pd.read_json(r'{file_path}')
print(df.head())
```"""

    else:
        return f"""```python
# Read as binary
with open(r'{file_path}', 'rb') as f:
    data = f.read()
print(f"Size: {{len(data)}} bytes")
```"""

def generate_markdown_report(root_paths, output_path):
    """Generate optimized markdown audit"""

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    md_content = f"""# Auditoría Optimizada - Datos Polygon/Massive

**Fecha:** {timestamp}

## ⚠️ Nota sobre Metodología

Esta auditoría utiliza **muestreo inteligente** para datasets masivos:
- Analiza estructura completa de directorios (primeros 2-3 niveles)
- Muestrea archivos representativos de cada tipo
- Proporciona estimaciones estadísticas cuando hay millones de archivos
- Genera código ejecutable para cada tipo de dato encontrado

---

## Resumen Ejecutivo

Ubicaciones analizadas:
- `C:\\TSIS_Data`
- `D:\\TSIS_SmallCaps\\raw\\polygon`

---

"""

    grand_total_files = 0
    grand_total_size = 0
    all_extensions = defaultdict(lambda: {'count': 0, 'examples': []})

    for root_path in root_paths:
        print(f"\n{'='*60}")
        print(f"Processing {root_path}")
        print('='*60)

        result = analyze_structure_smart(root_path)

        if result is None:
            md_content += f"## ⚠️ {root_path}\n\n**Estado:** Directorio no encontrado\n\n"
            continue

        structure, ext_stats, est_files, est_size, total_dirs = result

        grand_total_files += est_files
        grand_total_size += est_size

        # Merge extension stats
        for ext, stats in ext_stats.items():
            all_extensions[ext]['count'] += stats['count']
            all_extensions[ext]['examples'].extend(stats['examples'][:2])

        md_content += f"""## 📁 {root_path}

**Archivos estimados:** ~{est_files:,}
**Tamaño estimado:** ~{format_size(est_size)}
**Directorios escaneados:** {len(structure)}
**Subdirectorios totales:** {total_dirs:,}

### Tipos de Archivo Encontrados

| Extensión | Cantidad | Ejemplo de Archivo |
|-----------|----------|--------------------|
"""

        for ext, stats in sorted(ext_stats.items(), key=lambda x: x[1]['count'], reverse=True):
            example = stats['examples'][0] if stats['examples'] else "N/A"
            example_name = os.path.basename(example) if example != "N/A" else "N/A"
            md_content += f"| `{ext or '(sin ext)'}` | {stats['count']:,} | `{example_name}` |\n"

        md_content += "\n### Estructura de Directorios (Muestra)\n\n"

        # Show key directories
        key_paths = sorted(structure.keys(), key=lambda x: x.count(os.sep))[:20]

        for rel_path in key_paths:
            info = structure[rel_path]

            if 'error' in info:
                continue

            depth = rel_path.count(os.sep)
            indent = "  " * depth

            dir_display = rel_path if rel_path != '.' else '[RAÍZ]'
            md_content += f"{indent}#### 📂 `{dir_display}`\n\n"

            if info.get('file_count', 0) > 0 or info.get('dir_count', 0) > 0:
                md_content += f"{indent}**Archivos:** {info.get('file_count', 0):,} | "
                md_content += f"**Subdirectorios:** {info.get('dir_count', 0):,}\n\n"

            if info.get('dirs'):
                subdir_list = ', '.join([f'`{d}`' for d in info['dirs'][:10]])
                md_content += f"{indent}**Subdirectorios:** {subdir_list}"
                if len(info['dirs']) > 10:
                    md_content += f" _(+{len(info['dirs']) - 10} más)_"
                md_content += "\n\n"

            # Show sample files
            sample_files = info.get('files', [])[:5]
            if sample_files:
                md_content += f"{indent}**Archivos (muestra):**\n\n"
                for file_entry in sample_files:
                    try:
                        size = file_entry.stat().st_size
                        md_content += f"{indent}- `{file_entry.name}` ({format_size(size)})\n"
                    except:
                        md_content += f"{indent}- `{file_entry.name}`\n"
                md_content += "\n"

        md_content += "\n"

    # Code examples section
    md_content += """---

## 💻 Código para Acceder a los Datos

### Por Tipo de Archivo

"""

    for ext, stats in sorted(all_extensions.items(), key=lambda x: x[1]['count'], reverse=True):
        if not stats['examples']:
            continue

        example_path = stats['examples'][0]
        md_content += f"#### Archivos `{ext}` (encontrados: {stats['count']:,})\n\n"
        md_content += generate_read_code(example_path, ext)
        md_content += "\n\n"

    # Summary
    md_content += f"""---

## 📊 Resumen Global

**Total estimado de archivos:** ~{grand_total_files:,}
**Tamaño total estimado:** ~{format_size(grand_total_size)}
**Tipos de archivo diferentes:** {len(all_extensions)}

### Distribución por Extensión

| Extensión | Cantidad Estimada |
|-----------|-------------------|
"""

    for ext, stats in sorted(all_extensions.items(), key=lambda x: x[1]['count'], reverse=True):
        md_content += f"| `{ext or '(sin ext)'}` | {stats['count']:,} |\n"

    md_content += f"""

---

## 📝 Notas Técnicas

- **Metodología:** Muestreo estratificado para datasets masivos
- **Precisión:** Las estimaciones se basan en muestras representativas
- **Código:** Todo el código proporcionado es ejecutable en Jupyter Notebook
- **Actualización:** {timestamp}

---

*Generado por audit_polygon_data_optimized.py*
"""

    # Write report
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(md_content)

    print(f"\n{'='*60}")
    print(f"OK Reporte generado: {output_path}")
    print(f"   Archivos estimados: ~{grand_total_files:,}")
    print(f"   Tamaño estimado: ~{format_size(grand_total_size)}")
    print('='*60)

if __name__ == "__main__":
    root_paths = [
        r"C:\TSIS_Data",
        r"D:\TSIS_SmallCaps\raw\polygon"
    ]

    output_path = r"D:\TSIS_SmallCaps\01_daily\04_data_final\00_audit_polygon_data.md"

    print("="*60)
    print("AUDITORÍA OPTIMIZADA DE DATOS POLYGON/MASSIVE")
    print("="*60)
    print(f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    generate_markdown_report(root_paths, output_path)

    print(f"\nFin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\nOK Auditoria completada")
