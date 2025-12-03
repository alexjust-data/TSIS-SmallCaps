#!/usr/bin/env python3
"""
Auditoría completa de datos descargados de Polygon/Massive
Genera un reporte detallado en Markdown con:
- Estructura de directorios
- Contenido de archivos (con muestras)
- Código para acceder a cada tipo de dato
- Detección de duplicados
"""

import os
import sys
from pathlib import Path
from collections import defaultdict
import hashlib
from datetime import datetime

def get_file_hash(filepath, sample_size=1024*1024):
    """Calculate hash of first MB of file for duplicate detection"""
    try:
        hasher = hashlib.md5()
        with open(filepath, 'rb') as f:
            hasher.update(f.read(sample_size))
        return hasher.hexdigest()
    except:
        return None

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

def analyze_directory_structure(root_path):
    """Scan and analyze complete directory structure"""
    print(f"Scanning {root_path}...")

    structure = {}
    file_hashes = defaultdict(list)  # For duplicate detection
    extension_stats = defaultdict(lambda: {'count': 0, 'total_size': 0})
    total_files = 0
    total_size = 0

    if not os.path.exists(root_path):
        return None, None, None, 0, 0

    for dirpath, dirnames, filenames in os.walk(root_path):
        rel_path = os.path.relpath(dirpath, root_path)

        # Sort for consistent output
        dirnames.sort()
        filenames.sort()

        dir_info = {
            'subdirs': dirnames.copy(),
            'files': [],
            'total_size': 0,
            'file_count': len(filenames)
        }

        for filename in filenames:
            full_path = os.path.join(dirpath, filename)
            try:
                size = os.path.getsize(full_path)
                ext = get_file_extension(filename)
                file_hash = get_file_hash(full_path)

                file_info = {
                    'name': filename,
                    'size': size,
                    'extension': ext,
                    'hash': file_hash
                }

                dir_info['files'].append(file_info)
                dir_info['total_size'] += size
                total_size += size
                total_files += 1

                # Track by extension
                extension_stats[ext]['count'] += 1
                extension_stats[ext]['total_size'] += size

                # Track for duplicates
                if file_hash:
                    file_hashes[file_hash].append(full_path)

            except Exception as e:
                print(f"  Error processing {full_path}: {e}")
                continue

        structure[rel_path] = dir_info

    # Find duplicates
    duplicates = {h: paths for h, paths in file_hashes.items() if len(paths) > 1}

    print(f"  Scanned: {total_files:,} files, {format_size(total_size)}")
    print(f"  Found {len(duplicates)} potential duplicates")

    return structure, extension_stats, duplicates, total_files, total_size

def generate_read_code(file_path, extension):
    """Generate Python code to read different file types"""
    rel_path = file_path.replace('\\', '/')

    if extension == '.parquet':
        return f"""```python
import pandas as pd
import pyarrow.parquet as pq

# Read parquet file
df = pd.read_parquet(r'{file_path}')
print(df.shape)
print(df.head())

# Or with pyarrow for metadata
table = pq.read_table(r'{file_path}')
print(table.schema)
```"""

    elif extension == '.csv':
        return f"""```python
import pandas as pd

# Read CSV file
df = pd.read_csv(r'{file_path}')
print(df.shape)
print(df.head())
print(df.dtypes)
```"""

    elif extension == '.json':
        return f"""```python
import json
import pandas as pd

# Read JSON file
with open(r'{file_path}', 'r') as f:
    data = json.load(f)

# Or with pandas
df = pd.read_json(r'{file_path}')
print(df.head())
```"""

    elif extension == '.txt':
        return f"""```python
# Read text file
with open(r'{file_path}', 'r') as f:
    content = f.read()
print(content[:500])  # First 500 characters
```"""

    else:
        return f"""```python
# Binary file - check type
with open(r'{file_path}', 'rb') as f:
    data = f.read()
print(f"Size: {{len(data)}} bytes")
```"""

def generate_markdown_report(root_paths, output_path):
    """Generate comprehensive markdown audit report"""

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    md_content = f"""# Auditoría Completa de Datos Polygon/Massive

**Fecha de auditoría:** {timestamp}

## Resumen Ejecutivo

Esta auditoría analiza el 100% de los datos descargados desde Polygon (Massive API) en las siguientes ubicaciones:
- `C:\\TSIS_Data`
- `D:\\TSIS_SmallCaps\\raw\\polygon`

---

"""

    all_duplicates = {}
    total_files_all = 0
    total_size_all = 0

    for root_path in root_paths:
        print(f"\n{'='*60}")
        print(f"Analyzing {root_path}")
        print('='*60)

        structure, ext_stats, duplicates, total_files, total_size = analyze_directory_structure(root_path)

        if structure is None:
            md_content += f"## ⚠️ {root_path}\n\n**Estado:** Directorio no encontrado\n\n"
            continue

        total_files_all += total_files
        total_size_all += total_size
        all_duplicates.update(duplicates)

        md_content += f"""## 📁 {root_path}

**Total de archivos:** {total_files:,}
**Tamaño total:** {format_size(total_size)}
**Número de directorios:** {len(structure)}

### Estadísticas por Tipo de Archivo

| Extensión | Cantidad | Tamaño Total |
|-----------|----------|--------------|
"""

        for ext, stats in sorted(ext_stats.items(), key=lambda x: x[1]['total_size'], reverse=True):
            md_content += f"| `{ext or '(sin extensión)'}` | {stats['count']:,} | {format_size(stats['total_size'])} |\n"

        md_content += "\n### Estructura de Directorios y Contenido\n\n"

        # Organize by depth and path
        for rel_path in sorted(structure.keys(), key=lambda x: (x.count(os.sep), x)):
            info = structure[rel_path]

            depth = rel_path.count(os.sep)
            indent = "  " * depth

            # Directory header
            dir_display = rel_path if rel_path != '.' else '[RAÍZ]'
            md_content += f"{indent}#### 📂 `{dir_display}`\n\n"
            md_content += f"{indent}**Archivos:** {info['file_count']} | **Tamaño:** {format_size(info['total_size'])}\n\n"

            if info['subdirs']:
                md_content += f"{indent}**Subdirectorios:** {', '.join([f'`{d}`' for d in info['subdirs'][:10]])}"
                if len(info['subdirs']) > 10:
                    md_content += f" _(+{len(info['subdirs']) - 10} más)_"
                md_content += "\n\n"

            # List files
            if info['files']:
                md_content += f"{indent}**Archivos:**\n\n"

                # Show first 10 files
                for file_info in info['files'][:10]:
                    full_path = os.path.join(root_path, rel_path, file_info['name'])
                    md_content += f"{indent}- `{file_info['name']}` ({format_size(file_info['size'])}) {file_info['extension']}\n"

                if len(info['files']) > 10:
                    md_content += f"{indent}  _... y {len(info['files']) - 10} archivos más_\n"

                md_content += "\n"

                # Add code example for first file of each extension type
                shown_extensions = set()
                for file_info in info['files']:
                    ext = file_info['extension']
                    if ext not in shown_extensions and ext in ['.parquet', '.csv', '.json']:
                        shown_extensions.add(ext)
                        full_path = os.path.join(root_path, rel_path, file_info['name'])
                        md_content += f"{indent}**Ejemplo de lectura ({ext}):**\n\n"
                        md_content += generate_read_code(full_path, ext) + "\n\n"

            md_content += "\n"

    # Duplicates section
    md_content += f"""---

## 🔍 Detección de Duplicados

**Total de archivos potencialmente duplicados:** {len(all_duplicates)}

"""

    if all_duplicates:
        md_content += "| Hash | Ubicaciones | Tamaño |\n"
        md_content += "|------|-------------|--------|\n"

        for file_hash, paths in list(all_duplicates.items())[:20]:  # Show first 20
            if paths:
                try:
                    size = os.path.getsize(paths[0])
                    size_str = format_size(size)
                except:
                    size_str = "?"

                paths_str = "<br>".join([f"`{p}`" for p in paths[:3]])
                if len(paths) > 3:
                    paths_str += f"<br>_... y {len(paths) - 3} más_"

                md_content += f"| `{file_hash[:8]}...` | {paths_str} | {size_str} |\n"

        if len(all_duplicates) > 20:
            md_content += f"\n_... y {len(all_duplicates) - 20} grupos de duplicados más_\n"
    else:
        md_content += "✅ No se encontraron archivos duplicados.\n"

    md_content += f"""

---

## 📊 Resumen Global

**Total de archivos analizados:** {total_files_all:,}
**Tamaño total:** {format_size(total_size_all)}
**Grupos de duplicados:** {len(all_duplicates)}

---

## 📝 Notas

- Esta auditoría analiza el 100% de los archivos en las rutas especificadas
- Los duplicados se detectan comparando el hash MD5 del primer MB de cada archivo
- El código de ejemplo proporcionado es funcional y puede ejecutarse en Jupyter Notebook
- Todas las rutas mostradas son absolutas y listas para usar

**Generado el:** {timestamp}
"""

    # Write report
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(md_content)

    print(f"\n{'='*60}")
    print(f"✅ Reporte generado: {output_path}")
    print(f"   Total de archivos: {total_files_all:,}")
    print(f"   Tamaño total: {format_size(total_size_all)}")
    print('='*60)

if __name__ == "__main__":
    root_paths = [
        r"C:\TSIS_Data",
        r"D:\TSIS_SmallCaps\raw\polygon"
    ]

    output_path = r"D:\TSIS_SmallCaps\01_daily\04_data_final\00_audit_polygon_data.md"

    generate_markdown_report(root_paths, output_path)

    print("\n✅ Auditoría completada")
