import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

API_KEY = os.getenv("API_KEY")
DB_URL = os.getenv("DB_URL", "sqlite:///data/db/ambiente.db")

# Rutas base
RAW_PATH = "data/raw"
PROCESSED_PATH = "data/processed"
DB_PATH = "data/db/ambiente.db"
