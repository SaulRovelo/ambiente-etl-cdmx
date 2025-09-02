"""
Objetivo del módulo:
====================
Cargar los datos transformados en la base de datos SQL (SQLite por defecto) 
y exportarlos opcionalmente a CSV y Parquet como respaldo.
"""

import os
import pandas as pd
from sqlalchemy import create_engine
from etl.config import DB_URL, PROCESSED_PATH


def inicializar_bd():
    """Inicializa la conexión a la base de datos usando SQLAlchemy."""
    return create_engine(DB_URL)


def guardar_en_sql(df: pd.DataFrame, tabla: str = "calidad_aire"):
    """Inserta un DataFrame en la tabla SQL indicada."""
    engine = inicializar_bd()
    try:
        df.to_sql(tabla, engine, if_exists="append", index=False)
        print(f"✅ Datos insertados en la tabla '{tabla}'")
    except Exception as e:
        print(f"❌ Error al guardar en SQL: {e}")


def exportar_csv_parquet(df: pd.DataFrame, nombre: str = "calidad_aire"):
    """Exporta DataFrame a CSV y Parquet en data/processed/."""
    os.makedirs(PROCESSED_PATH, exist_ok=True)

    ruta_csv = os.path.join(PROCESSED_PATH, f"{nombre}.csv")
    ruta_parquet = os.path.join(PROCESSED_PATH, f"{nombre}.parquet")

    try:
        df.to_csv(ruta_csv, index=False)
        df.to_parquet(ruta_parquet, index=False)
        print(f"📂 Datos exportados a: {ruta_csv} y {ruta_parquet}")
    except Exception as e:
        print(f"❌ Error al exportar: {e}")
