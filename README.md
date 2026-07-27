# Pipeline ETL de Calidad del Aire en Ciudad de México

Pipeline en Python para consultar datos de calidad del aire y clima de IQAir/AirVisual, conservar la respuesta original y mantener un historial local validado en SQLite, CSV y Parquet.

## Estado

Extract, Transform, validación, carga, exportaciones y orquestación con Prefect están implementados y probados. El proyecto continúa en desarrollo antes de definir automatización o una frecuencia de ejecución.

## Tecnologías

- Python 3.11
- Requests
- Pandas
- SQLAlchemy y SQLite
- PyArrow
- Prefect
- Pytest
- python-dotenv

## Organización

- `etl/`: código del pipeline.
- `tests/`: pruebas automatizadas.
- `data/`: datos crudos, procesados y base SQLite.
- `notebooks/`: exploración y evidencias.
- `assets/`: recursos de documentación.

## Configuración

`.env.example` documenta las variables previstas. Las credenciales reales deberán guardarse en un archivo local `.env`, excluido de Git.

La suite ordinaria no lee el `.env` real ni requiere una API key.

## Entorno de desarrollo

Crear y activar el entorno virtual:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
```

Actualizar `pip` e instalar las dependencias:

```bash
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## Pruebas

Ejecutar la suite ordinaria:

```bash
python -m pytest
```

Ejecutar únicamente las pruebas integrales:

```bash
python -m pytest tests/test_integration.py -vv
```

Estas pruebas utilizan respuestas simuladas, directorios y bases SQLite temporales y un servidor Prefect local mediante loopback. No realizan conexiones a internet ni modifican los archivos de `data/`.

## Contrato real opcional

La prueba marcada como `real_api` está desactivada por defecto. Para ejecutarla explícitamente:

```bash
python -m pytest tests/test_integration.py -m real_api --real-api -vv
```

Esta ejecución requiere un `.env` válido, realiza una sola solicitud a IQAir y guarda cualquier resultado únicamente en un directorio temporal. Se omite automáticamente si falta la configuración.
