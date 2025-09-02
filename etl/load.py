"""
Objetivo del módulo:
====================
Cargar los datos transformados en la base de datos SQL (SQLite por defecto) 
y exportarlos opcionalmente a CSV y Parquet como respaldo.

Flujo:
1. Conectar con la base de datos usando SQLAlchemy.
2. Crear la tabla si no existe.
3. Insertar los datos transformados.
4. Exportar a CSV/Parquet en data/processed/ si se requiere.
"""

# === Importaciones necesarias ===
import os                           # Manejo de rutas y directorios
import pandas as pd                 # Manipulación de DataFrames
from sqlalchemy import create_engine # Crear conexión (engine) con la base de datos
from etl.config import DB_URL, PROCESSED_PATH  # Configuración centralizada del proyecto

# === Función para inicializar conexión a la base de datos ===
def init_db():
    """
    Inicializa la conexión a la base de datos usando SQLAlchemy.
    Retorna un engine para interactuar con SQL.
    """
    engine = create_engine(DB_URL)  # Crea un "engine" con la URL de la base de datos
    return engine                   # Devuelve el engine para que otros lo usen

# === Función para insertar datos en SQL ===
def guardar_en_sql(df: pd.DataFrame, tabla: str = "calidad_aire"):
    """
    Inserta los datos transformados en una tabla SQL.
    Parámetros:
        df (pd.DataFrame): DataFrame con datos limpios.
        tabla (str): Nombre de la tabla donde insertar.
    """
    engine = init_db()  # Obtiene el engine de conexión
    try:
        # Inserta el DataFrame en la tabla (append = agrega sin borrar lo anterior)
        df.to_sql(tabla, engine, if_exists="append", index=False)
        print(f"✅ Datos insertados en la tabla '{tabla}'")
    except Exception as e:
        # Si hay error (ej. tabla no accesible o DB bloqueada), lo muestra
        print(f"❌ Error al guardar en SQL: {e}")

# === Función para exportar DataFrame a CSV y Parquet ===
def exportar_csv_parquet(df: pd.DataFrame, nombre: str = "air_quality_processed"):
    """
    Exporta los datos procesados a CSV y Parquet en data/processed/.
    Parámetros:
        df (pd.DataFrame): DataFrame con datos limpios.
        nombre (str): Prefijo de los archivos de salida.
    """
    # Crea la carpeta processed si no existe
    os.makedirs(PROCESSED_PATH, exist_ok=True)
    
    # Define rutas de salida para CSV y Parquet
    ruta_csv = os.path.join(PROCESSED_PATH, f"{nombre}.csv")
    ruta_parquet = os.path.join(PROCESSED_PATH, f"{nombre}.parquet")

    try:
        df.to_csv(ruta_csv, index=False)       # Exporta a CSV
        df.to_parquet(ruta_parquet, index=False)  # Exporta a Parquet
        print(f"📂 Datos exportados a: {ruta_csv} y {ruta_parquet}")
    except Exception as e:
        # Si ocurre error (ej. permisos, disco lleno, etc.), se muestra
        print(f"❌ Error al exportar: {e}")


