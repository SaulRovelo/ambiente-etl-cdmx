import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Detectar BASE_DIR (raíz del proyecto)
BASE_DIR = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

# Rutas absolutas
RAW_PATH = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_PATH = os.path.join(BASE_DIR, "data", "processed")
DB_PATH = os.path.join(BASE_DIR, "data", "db", "ambiente.db")

# Asegurar que existan las carpetas necesarias
os.makedirs(RAW_PATH, exist_ok=True)
os.makedirs(PROCESSED_PATH, exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Variables de entorno
API_KEY = os.getenv("API_KEY")

# Forzar que DB_URL siempre use la ruta absoluta en formato universal
DB_URL = os.getenv("DB_URL")
if not DB_URL:
    DB_URL = f"sqlite:///{DB_PATH.replace(os.sep, '/')}"


# Debug opcional
print("RAW_PATH:", RAW_PATH)
print("PROCESSED_PATH:", PROCESSED_PATH)
print("DB_PATH:", DB_PATH)
print("DB_URL:", DB_URL)
