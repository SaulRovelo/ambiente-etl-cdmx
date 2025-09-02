import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Si __file__ existe (scripts), usarlo. Si no (notebooks), usar cwd y retroceder a la raíz.
if "__file__" in globals():
    BASE_DIR = os.path.dirname(os.path.dirname(__file__))
else:
    BASE_DIR = os.path.abspath(os.path.join(os.getcwd(), ".."))

# Variables de entorno
API_KEY = os.getenv("API_KEY")
DB_URL = os.getenv(
    "DB_URL",
    f"sqlite:///{os.path.join(BASE_DIR, 'data', 'db', 'ambiente.db')}"
)

# Rutas absolutas
RAW_PATH = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_PATH = os.path.join(BASE_DIR, "data", "processed")
DB_PATH = os.path.join(BASE_DIR, "data", "db", "ambiente.db")

# Debug (puedes quitarlo luego)
print("RAW_PATH:", RAW_PATH)
print("PROCESSED_PATH:", PROCESSED_PATH)
print("DB_PATH:", DB_PATH)
