"""Utilidades compartidas para fechas, directorios y logging seguro."""

from __future__ import annotations

import logging
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable, TextIO

from etl.config import ProjectPaths


REDACTED = "<redacted>"

_SENSITIVE_ASSIGNMENT = re.compile(
    r"(?i)(?P<label>\b(?:iqair[_-]?api[_-]?key|api[_-]?key|apikey|key)"
    r"\b\s*(?:=|:)\s*[\"']?)(?P<value>[^&\s,;\"'}]+)"
)


def utc_now() -> datetime:
    """Devuelve la fecha y hora actual como ``datetime`` consciente en UTC."""

    return datetime.now(UTC)


def format_utc_timestamp(
    value: datetime | None = None,
    *,
    timespec: str = "seconds",
) -> str:
    """Normaliza una fecha consciente a ISO 8601 UTC con sufijo ``Z``."""

    timestamp = utc_now() if value is None else value
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("El timestamp debe incluir información de zona horaria")

    normalized = timestamp.astimezone(UTC)
    return normalized.isoformat(timespec=timespec).replace("+00:00", "Z")


def ensure_data_directories(paths: ProjectPaths) -> tuple[Path, ...]:
    """Crea de forma idempotente únicamente los directorios de datos."""

    directories = paths.data_directories
    for directory in directories:
        if not directory.is_relative_to(paths.project_root):
            raise ValueError(
                f"El directorio de datos está fuera del proyecto: {directory}"
            )
        directory.mkdir(parents=True, exist_ok=True)
    return directories


def redact_sensitive_text(
    value: object,
    *,
    secrets: Iterable[str] = (),
) -> str:
    """Oculta secretos conocidos y parámetros comunes de API keys."""

    redacted = str(value)
    normalized_secrets = sorted(
        {secret for secret in secrets if secret},
        key=len,
        reverse=True,
    )
    for secret in normalized_secrets:
        redacted = redacted.replace(secret, REDACTED)

    return _SENSITIVE_ASSIGNMENT.sub(
        lambda match: f"{match.group('label')}{REDACTED}",
        redacted,
    )


class RedactingFormatter(logging.Formatter):
    """Formatter que elimina secretos del mensaje y de las excepciones."""

    converter = time.gmtime

    def __init__(self, *, secrets: Iterable[str] = ()) -> None:
        super().__init__(
            fmt="%(asctime)sZ | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
        self._secrets = tuple(secret for secret in secrets if secret)

    def format(self, record: logging.LogRecord) -> str:
        """Formatea el registro y redacta su representación completa."""

        rendered = super().format(record)
        return redact_sensitive_text(rendered, secrets=self._secrets)


def configure_safe_logger(
    name: str,
    *,
    api_key: str,
    level: int = logging.INFO,
    stream: TextIO | None = None,
) -> logging.Logger:
    """Configura un logger dedicado que no propaga mensajes sin redactar."""

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False
    logger.handlers.clear()

    handler = logging.StreamHandler(stream)
    handler.setFormatter(RedactingFormatter(secrets=(api_key,)))
    logger.addHandler(handler)
    return logger
