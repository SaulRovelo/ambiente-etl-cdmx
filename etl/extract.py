# Objetivo:
# Este script conecta con la API de IQAir (AirVisual) para descargar datos actuales
# de calidad del aire y clima básico en la Ciudad de México.
# Los resultados se guardan como archivos JSON en la carpeta data/raw/
# con un nombre que incluye timestamp para asegurar trazabilidad histórica.

import os
import requests
import json
from datetime import datetime
from etl.config import API_KEY, RAW_PATH


def obtener_datos_calidad_aire() -> dict:
    """
    Conecta con la API de IQAir para obtener datos de calidad del aire y clima
    de la Ciudad de México.
    Retorna la respuesta como un diccionario (JSON).
    """
    url = "https://api.airvisual.com/v2/city"
    params = {
        "city": "Mexico City",
        "state": "Mexico City",
        "country": "Mexico",
        "key": API_KEY
    }

    try:
        respuesta = requests.get(url, params=params, timeout=10)
        respuesta.raise_for_status()
        return respuesta.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Error al descargar datos: {e}")
        return {}


def guardar_datos_crudos(data: dict, prefijo: str = "calidad_aire"):
    """
    Guarda los datos descargados en un archivo JSON dentro de data/raw/.
    Usa timestamp en el nombre para no sobrescribir descargas previas.
    """
    os.makedirs(RAW_PATH, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    ruta_archivo = os.path.join(RAW_PATH, f"{prefijo}_{timestamp}.json")

    with open(ruta_archivo, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"✅ Datos guardados en {ruta_archivo}")
    return ruta_archivo
