"""Configuración tipada y rutas del pipeline ETL."""

from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping
from urllib.parse import urlsplit, urlunsplit

from dotenv import dotenv_values


DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parents[1]

_ENVIRONMENT_VARIABLES = (
    "IQAIR_API_KEY",
    "IQAIR_CITY",
    "IQAIR_STATE",
    "IQAIR_COUNTRY",
    "IQAIR_BASE_URL",
    "IQAIR_TIMEOUT_SECONDS",
)
_PLACEHOLDER_VALUES = frozenset(
    {
        "changeme",
        "confirm_with_iqair",
        "replace_with_your_api_key",
        "your_api_key",
        "your_api_key_here",
    }
)


class ConfigurationError(ValueError):
    """Indica que la configuración requerida es inexistente o inválida."""


@dataclass(frozen=True, slots=True)
class ProjectPaths:
    """Rutas absolutas utilizadas por el pipeline."""

    project_root: Path
    data_dir: Path
    raw_dir: Path
    processed_dir: Path
    db_dir: Path
    database_path: Path
    processed_csv_path: Path
    processed_parquet_path: Path
    rejected_csv_path: Path

    @property
    def data_directories(self) -> tuple[Path, Path, Path]:
        """Devuelve los directorios de datos que el pipeline puede crear."""

        return (self.raw_dir, self.processed_dir, self.db_dir)


@dataclass(frozen=True, slots=True)
class Settings:
    """Configuración validada para consultar y persistir datos de IQAir."""

    api_key: str = field(repr=False)
    city: str
    state: str
    country: str
    base_url: str
    timeout_seconds: float
    paths: ProjectPaths


def build_project_paths(project_root: Path | None = None) -> ProjectPaths:
    """Construye rutas absolutas sin depender del directorio de ejecución."""

    root = (project_root or DEFAULT_PROJECT_ROOT).expanduser().resolve()
    data_dir = root / "data"
    raw_dir = data_dir / "raw"
    processed_dir = data_dir / "processed"
    db_dir = data_dir / "db"

    return ProjectPaths(
        project_root=root,
        data_dir=data_dir,
        raw_dir=raw_dir,
        processed_dir=processed_dir,
        db_dir=db_dir,
        database_path=db_dir / "ambiente.db",
        processed_csv_path=processed_dir / "calidad_aire.csv",
        processed_parquet_path=processed_dir / "calidad_aire.parquet",
        rejected_csv_path=processed_dir / "registros_rechazados.csv",
    )


def load_settings(
    *,
    env_file: Path | None = None,
    environ: Mapping[str, str] | None = None,
    project_root: Path | None = None,
) -> Settings:
    """Carga y valida la configuración desde ``.env`` y el entorno.

    Las variables del entorno tienen prioridad sobre los valores del archivo.
    Pasar ``environ`` permite aislar la configuración durante las pruebas.
    """

    paths = build_project_paths(project_root)
    dotenv_path = env_file or paths.project_root / ".env"
    file_values = _read_env_file(dotenv_path)
    environment = os.environ if environ is None else environ

    values = {
        variable: file_values.get(variable)
        for variable in _ENVIRONMENT_VARIABLES
    }
    values.update(
        {
            variable: environment[variable]
            for variable in _ENVIRONMENT_VARIABLES
            if variable in environment
        }
    )

    missing = [
        variable
        for variable, value in values.items()
        if value is None or not str(value).strip()
    ]
    if missing:
        names = ", ".join(sorted(missing))
        raise ConfigurationError(
            f"Faltan variables de entorno obligatorias: {names}"
        )

    api_key = _validate_text(values["IQAIR_API_KEY"], "IQAIR_API_KEY")
    city = _validate_text(values["IQAIR_CITY"], "IQAIR_CITY")
    state = _validate_text(values["IQAIR_STATE"], "IQAIR_STATE")
    country = _validate_text(values["IQAIR_COUNTRY"], "IQAIR_COUNTRY")

    return Settings(
        api_key=api_key,
        city=city,
        state=state,
        country=country,
        base_url=_validate_base_url(values["IQAIR_BASE_URL"]),
        timeout_seconds=_validate_timeout(values["IQAIR_TIMEOUT_SECONDS"]),
        paths=paths,
    )


def _read_env_file(env_file: Path) -> dict[str, str | None]:
    """Lee un archivo dotenv sin modificar globalmente ``os.environ``."""

    try:
        return dict(dotenv_values(env_file, interpolate=False))
    except OSError as exc:
        raise ConfigurationError(
            f"No fue posible leer el archivo de configuración: {env_file}"
        ) from exc


def _validate_text(value: str | None, variable: str) -> str:
    """Valida un valor textual obligatorio y descarta placeholders."""

    normalized = str(value).strip()
    if (
        not normalized
        or normalized.casefold() in _PLACEHOLDER_VALUES
        or normalized.casefold().startswith("confirm_")
    ):
        raise ConfigurationError(
            f"La variable {variable} debe contener un valor real no vacío"
        )
    return normalized


def _validate_base_url(value: str | None) -> str:
    """Valida y normaliza la URL HTTPS base del proveedor."""

    normalized = str(value).strip()
    if any(character.isspace() for character in normalized):
        raise ConfigurationError(
            "IQAIR_BASE_URL debe ser una URL HTTPS válida sin espacios"
        )

    try:
        parsed = urlsplit(normalized)
        hostname = parsed.hostname
        parsed.port
    except ValueError as exc:
        raise ConfigurationError(
            "IQAIR_BASE_URL debe ser una URL HTTPS válida"
        ) from exc

    if parsed.scheme.casefold() != "https" or not hostname:
        raise ConfigurationError(
            "IQAIR_BASE_URL debe ser una URL HTTPS válida con un host"
        )
    if parsed.username or parsed.password:
        raise ConfigurationError(
            "IQAIR_BASE_URL no debe incluir credenciales"
        )
    if parsed.query or parsed.fragment:
        raise ConfigurationError(
            "IQAIR_BASE_URL no debe incluir query ni fragmento"
        )

    path = parsed.path.rstrip("/")
    return urlunsplit(("https", parsed.netloc, path, "", ""))


def _validate_timeout(value: str | None) -> float:
    """Valida que el timeout sea numérico, finito y mayor que cero."""

    try:
        timeout = float(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(
            "IQAIR_TIMEOUT_SECONDS debe ser un número mayor que cero"
        ) from exc

    if not math.isfinite(timeout) or timeout <= 0:
        raise ConfigurationError(
            "IQAIR_TIMEOUT_SECONDS debe ser un número finito mayor que cero"
        )
    return timeout
