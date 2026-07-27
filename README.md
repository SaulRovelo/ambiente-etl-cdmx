# Pipeline ETL de Calidad del Aire en Ciudad de México

Pipeline en Python para consultar datos de calidad del aire y clima de IQAir/AirVisual, conservar la respuesta original y mantener un historial local validado en SQLite, CSV y Parquet.

## Estado

Extract, Transform, validación, carga, exportaciones y orquestación con Prefect están implementados y probados. El proyecto incluye un deployment local con ejecución horaria y continúa en desarrollo.

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

## Automatización local con Prefect

La [documentación de IQAir](https://api-docs.iqair.com/) indica que las estaciones admitidas actualizan sus datos una vez por hora. El deployment `calidad-aire-cdmx-horario` se ejecuta en el minuto 10 de cada hora, en la zona `America/Mexico_City`:

```text
10 * * * *
```

Esta frecuencia realiza 24 consultas diarias y hasta 744 en un mes de 31 días. Se mantiene por debajo de los [límites publicados por IQAir](https://www.iqair.com/commercial-air-quality-monitors/api) para el plan Community —500 consultas diarias y 10 000 mensuales— y evita repetir consultas dentro del mismo ciclo de actualización. El nivel contratado y el consumo disponible deben comprobarse en el panel de la cuenta antes de mantener el servicio activo.

El deployment usa un work pool local de tipo `process`. Su límite de concurrencia es uno y cancela una ejecución nueva si la anterior sigue activa, por lo que no acumula ejecuciones solapadas.

El archivo `.env` con la API key debe permanecer únicamente en la raíz local del proyecto. `prefect.yaml` no contiene credenciales. Todos los comandos siguientes deben ejecutarse desde la raíz del repositorio con `.venv` activo.

Iniciar el servidor local y apuntar la CLI a su API:

```bash
source .venv/bin/activate
prefect server start --background
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```

Crear el work pool la primera vez y registrar o actualizar el deployment:

```bash
prefect work-pool create ambiente-etl-local --type process
prefect deploy --name calidad-aire-cdmx-horario
```

Iniciar el worker en otra terminal, también desde la raíz del repositorio:

```bash
source .venv/bin/activate
prefect worker start --pool ambiente-etl-local --type process --limit 1
```

El worker atiende la agenda y las ejecuciones manuales. Para solicitar una ejecución inmediata sin alterar el cron:

```bash
prefect deployment run pipeline-calidad-aire-cdmx/calidad-aire-cdmx-horario --watch
```

La interfaz local queda disponible en `http://127.0.0.1:4200`. También se pueden consultar el deployment y las ejecuciones recientes desde la terminal:

```bash
prefect deployment inspect pipeline-calidad-aire-cdmx/calidad-aire-cdmx-horario
prefect flow-run ls --flow-name pipeline-calidad-aire-cdmx
```

Para detener los servicios, interrumpir primero el worker con `Ctrl+C` en su terminal y después detener el servidor:

```bash
prefect server stop
```
