# Pipeline ETL de Calidad del Aire en Ciudad de México

Proyecto en desarrollo para consultar datos de calidad del aire y clima de IQAir/AirVisual, conservar la respuesta original y preparar un historial local validado en SQLite, CSV y Parquet.

## Estado

La estructura inicial y el entorno de desarrollo están preparados. La configuración y los componentes del pipeline todavía no están implementados.

## Tecnologías previstas

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
