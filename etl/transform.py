"""
Objetivo del módulo:
====================
Transformar los datos crudos descargados desde la API de IQAir (calidad del aire y clima),
aplicando reglas de limpieza, normalización y validación antes de cargarlos a la base de datos
o exportarlos como CSV/Parquet.
"""

import os
import json
import pandas as pd
from datetime import datetime
from etl.config import PROCESSED_PATH


def cargar_datos_crudos(ruta_archivo: str) -> dict:
    """Lee un JSON desde data/raw y lo devuelve como diccionario."""
    with open(ruta_archivo, "r", encoding="utf-8") as f:
        return json.load(f)


def transformar_datos(ruta_archivo: str) -> pd.DataFrame:
    """
    Toma un JSON crudo desde la ruta, lo convierte en DataFrame estructurado y lo guarda en processed/.
    """
    if not os.path.exists(ruta_archivo):
        print(f"❌ Archivo no encontrado: {ruta_archivo}")
        return pd.DataFrame()

    try:
        datos = cargar_datos_crudos(ruta_archivo)

        ciudad = datos["data"]["city"]
        if ciudad.lower() in ["cdmx", "mexico city", "méxico city"]:
            ciudad = "CDMX"

        pais = datos["data"]["country"]
        contaminacion = datos["data"]["current"]["pollution"]
        clima = datos["data"]["current"]["weather"]

        registro = {
            "ciudad": ciudad,
            "pais": pais,
            "fecha_hora": pd.to_datetime(contaminacion["ts"], utc=True),
            "aqi_us": contaminacion["aqius"],
            "aqi_cn": contaminacion["aqicn"],
            "contaminante_principal": contaminacion["mainus"],
            "temperatura_c": clima["tp"],
            "humedad_pct": clima["hu"],
            "viento_mps": clima["ws"],
        }

        df = pd.DataFrame([registro]).dropna()

        # Guardar versión procesada
        os.makedirs(PROCESSED_PATH, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        ruta_csv = os.path.join(PROCESSED_PATH, f"calidad_aire_{timestamp}.csv")
        df.to_csv(ruta_csv, index=False, encoding="utf-8")

        print(f"✅ Datos transformados y guardados en {ruta_csv}")
        return df

    except KeyError as e:
        print(f"❌ Error en estructura de datos: campo faltante {e}")
        return pd.DataFrame()
