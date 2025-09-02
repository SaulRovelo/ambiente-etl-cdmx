"""
Objetivo:
=========
Orquestar el pipeline ETL con Prefect, coordinando la ejecución
de extracción, transformación y carga en un flujo automatizable.
"""

from prefect import flow, task
from etl.extract import obtener_datos_calidad_aire, guardar_datos_crudos
from etl.transform import transformar_datos
from etl.load import guardar_en_sql, exportar_csv_parquet


@task
def tarea_extraer():
    """Descarga datos de la API y guarda el JSON en data/raw."""
    datos = obtener_datos_calidad_aire()
    if datos and datos.get("status") == "success":
        return guardar_datos_crudos(datos)
    print("⚠️ No se recibieron datos válidos de la API.")
    return None


@task
def tarea_transformar(ruta_archivo: str):
    """Transforma el JSON crudo en un DataFrame limpio."""
    if ruta_archivo:
        return transformar_datos(ruta_archivo)
    return None


@task
def tarea_cargar(df):
    """Inserta los datos en la base de datos y exporta a CSV/Parquet."""
    if df is not None and not df.empty:
        guardar_en_sql(df, tabla="calidad_aire")
        exportar_csv_parquet(df, nombre="calidad_aire")
        return True
    print("⚠️ DataFrame vacío: no se cargaron datos.")
    return False


@flow(name="ETL-CalidadAire-CDMX")
def flujo_etl():
    """Flujo principal ETL orquestado con Prefect."""
    print("🚀 Iniciando flujo ETL con Prefect...")
    ruta = tarea_extraer()
    df = tarea_transformar(ruta)
    tarea_cargar(df)
    print("✅ Flujo ETL completado.")


if __name__ == "__main__":
    flujo_etl()
