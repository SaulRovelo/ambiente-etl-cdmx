"""
Objetivo del módulo:
====================
Transformar los datos crudos descargados desde la API de IQAir (calidad del aire y clima),
aplicando reglas de limpieza, normalización y validación antes de cargarlos a la base de datos
o exportarlos como CSV/Parquet.

Flujo:
1) Leer archivo JSON crudo desde data/raw/
2) Convertir a DataFrame de pandas con columnas normalizadas
3) Limpiar y validar valores críticos (AQI, temperatura, humedad, etc.)
4) Guardar los resultados procesados en data/processed/
"""

# -----------------------
# Importaciones
# -----------------------
import os                                   # Manejo de rutas y utilidades de sistema operativo
import json                                 # Cargar/parsear archivos JSON
import pandas as pd                         # Manipulación de datos tabulares
from datetime import datetime               # Timestamps para versionar archivos de salida
from etl.config import RAW_PATH, PROCESSED_PATH  # Rutas absolutas definidas en config.py


def cargar_datos_crudos(ruta_archivo: str) -> dict:
    """
    Cargar un archivo JSON crudo desde la carpeta data/raw.

    Parámetros:
        ruta_archivo (str): ruta absoluta del archivo JSON crudo.

    Retorna:
        dict: contenido del archivo interpretado como diccionario Python.
    """
    # Abrimos el archivo en modo lectura con codificación UTF-8 → garantiza caracteres correctos
    with open(ruta_archivo, "r", encoding="utf-8") as f:
        # Leemos el contenido y lo convertimos de JSON a dict → estructura navegable en Python
        return json.load(f)


def transformar_datos(data: dict) -> pd.DataFrame:
    """
    Transformar los datos crudos en un DataFrame estructurado.

    Extrae:
      - Ciudad y país
      - Fecha/hora de la medición
      - Indicadores de contaminación (AQI US, AQI CN, contaminante principal)
      - Variables meteorológicas (temperatura, humedad, velocidad del viento)
    """
    try:
        # Navegamos el JSON hasta metadatos generales
        ciudad = data["data"]["city"]                           # Nombre de la ciudad
        pais = data["data"]["country"]                          # Nombre del país
        ts = data["data"]["current"]["pollution"]["ts"]         # Timestamp ISO de la medición

        # Navegamos a los nodos específicos de contaminación y clima
        contaminacion = data["data"]["current"]["pollution"]    # Nodo con métricas de AQI
        clima = data["data"]["current"]["weather"]              # Nodo con métricas meteorológicas

        # Construimos un registro normalizado con nombres de columnas consistentes
        registro = {
            "ciudad": ciudad,                                   # Columna: ciudad
            "pais": pais,                                       # Columna: país
            "fecha_hora": pd.to_datetime(ts),                   # Convertimos string ISO → datetime
            "aqi_us": contaminacion["aqius"],                   # AQI estándar US
            "aqi_cn": contaminacion["aqicn"],                   # AQI estándar China
            "contaminante_principal": contaminacion["mainus"],  # Contaminante dominante (US)
            "temperatura_c": clima["tp"],                       # Temperatura en °C
            "humedad_pct": clima["hu"],                         # Humedad relativa en %
            "viento_mps": clima["ws"],                          # Velocidad del viento en m/s
        }

        # Convertimos ese registro (dict) en un DataFrame de 1 fila → interfaz uniforme para la T y la L
        return pd.DataFrame([registro])

    except KeyError as e:
        # Si alguna clave esperada no existe en el JSON, avisamos y devolvemos DF vacío para no romper el pipeline
        print(f"❌ Error en transformación: falta la clave {e}")
        return pd.DataFrame()


def limpiar_y_validar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplicar reglas de limpieza y validación:
      - Eliminar duplicados (por fecha y ciudad)
      - Validar que no haya valores nulos en campos críticos (AQI y temperatura)
    """
    # Si por alguna razón recibimos un DataFrame vacío, lo devolvemos tal cual (evita errores aguas arriba)
    if df.empty:
        return df

    # Eliminamos registros duplicados considerando la combinación fecha_hora + ciudad (clave natural)
    df = df.drop_duplicates(subset=["fecha_hora", "ciudad"])

    # Validamos que los campos críticos no vengan nulos; si lo están, preferimos fallar temprano (fail-fast)
    if df["aqi_us"].isnull().any() or df["temperatura_c"].isnull().any():
        raise ValueError("⚠️ Datos incompletos detectados en AQI o temperatura")

    # Retornamos el DataFrame limpio y validado
    return df


def guardar_datos_procesados(df: pd.DataFrame, prefijo: str = "calidad_aire"):
    """
    Guardar los datos limpios en la carpeta data/processed en dos formatos:
      - CSV  : formato legible y portable
      - Parquet : formato binario eficiente (columnar) para análisis
    """
    # Aseguramos que la carpeta de salida exista (idempotente)
    os.makedirs(PROCESSED_PATH, exist_ok=True)

    # Generamos un timestamp YYYYMMDD_HHMMSS para versionar los archivos de salida
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Construimos rutas absolutas de salida para CSV y Parquet
    ruta_csv = os.path.join(PROCESSED_PATH, f"{prefijo}_{timestamp}.csv")
    ruta_parquet = os.path.join(PROCESSED_PATH, f"{prefijo}_{timestamp}.parquet")

    # Escribimos el DataFrame a CSV (sin índice) → fácil de inspeccionar y compartir
    df.to_csv(ruta_csv, index=False)

    # Intentamos guardar Parquet usando pyarrow (motor recomendado)
    try:
        df.to_parquet(ruta_parquet, engine="pyarrow", index=False)
    except Exception as e:
        # Si pyarrow falla (no instalado / conflicto), probamos automáticamente con fastparquet
        print(f"⚠️ Error con pyarrow: {e}. Intentando con fastparquet...")
        df.to_parquet(ruta_parquet, engine="fastparquet", index=False)

    # Mensaje de confirmación con las rutas donde se guardaron los archivos
    print(f"✅ Datos procesados guardados en:\n- {ruta_csv}\n- {ruta_parquet}")
