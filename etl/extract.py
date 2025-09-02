# Objetivo:
# Este script conecta con la API de IQAir (AirVisual) para descargar datos actuales
# de calidad del aire y clima básico en la Ciudad de México.
# Los resultados se guardan como archivos JSON en la carpeta data/raw/
# con un nombre que incluye timestamp para asegurar trazabilidad histórica.

import os                # Manejo de rutas y directorios
import requests          # Librería para hacer solicitudes HTTP (API REST)
import json              # Para guardar la respuesta en formato JSON
from datetime import datetime  # Para generar timestamp en los nombres de archivo
from etl.config import API_KEY, RAW_PATH  # Variables centralizadas en config.py

def fetch_air_quality_data() -> dict:
    """
    Conecta con la API de IQAir para obtener datos de calidad del aire y clima
    de la Ciudad de México.
    Retorna la respuesta como un diccionario (JSON).
    """
    # URL base de la API de IQAir
    url = "https://api.airvisual.com/v2/city"

    # Parámetros que definen la consulta: ciudad, estado, país y clave API
    params = {
        "city": "Mexico City",
        "state": "Mexico City",
        "country": "Mexico",
        "key": API_KEY
    }

    try:
        # Se hace la llamada HTTP GET con un tiempo máximo de espera de 10s
        response = requests.get(url, params=params, timeout=10)

        # Si la respuesta no es 200 (OK), lanza excepción
        response.raise_for_status()

        # Convierte la respuesta en formato JSON y la retorna
        return response.json()
    except requests.exceptions.RequestException as e:
        # Captura cualquier error de red o API y muestra mensaje en consola
        print(f"❌ Error al descargar datos: {e}")
        return {}

def save_raw_data(data: dict, prefix: str = "air_quality"):
    """
    Guarda los datos descargados en un archivo JSON dentro de data/raw/.
    Usa timestamp en el nombre para no sobrescribir descargas previas.
    """
    # Crear la carpeta data/raw si no existe
    os.makedirs(RAW_PATH, exist_ok=True)

    # Generar un timestamp con formato YYYYMMDD_HHMMSS
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Construir el nombre del archivo de salida
    file_path = os.path.join(RAW_PATH, f"{prefix}_{timestamp}.json")

    # Guardar el diccionario en formato JSON legible (indentado)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"✅ Datos guardados en {file_path}")
    return file_path

if __name__ == "__main__":
    # Punto de entrada del script: se ejecuta solo si llamamos python -m etl.extract

    # Paso 1: Llamar a la API y obtener los datos
    data = fetch_air_quality_data()

    # Paso 2: Validar que la respuesta sea correcta (status=success)
    if data and data.get("status") == "success":
        # Paso 3: Guardar los datos en un archivo JSON
        save_raw_data(data)
    else:
        # Mensaje en caso de que no haya datos válidos
        print("⚠️ No se recibieron datos válidos de la API.")
