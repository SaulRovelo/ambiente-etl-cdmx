"""Extracción controlada de datos actuales de IQAir."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

import requests

from etl.config import ProjectPaths, Settings
from etl.utils import (
    configure_safe_logger,
    ensure_data_directories,
    redact_sensitive_text,
    utc_now,
)


_CITY_ENDPOINT = "city"
_MAX_FILENAME_ATTEMPTS = 10
_SAFE_TOKEN = re.compile(r"[^a-zA-Z0-9]")


class ResponseLike(Protocol):
    """Contrato mínimo de una respuesta HTTP utilizado por la extracción."""

    status_code: int
    headers: Mapping[str, str]
    content: bytes

    def json(self) -> Any:
        """Decodifica el cuerpo como JSON."""


class HttpGet(Protocol):
    """Firma inyectable de una solicitud HTTP GET."""

    def __call__(
        self,
        url: str,
        *,
        params: Mapping[str, str],
        timeout: float,
    ) -> ResponseLike:
        """Ejecuta una solicitud GET."""


class ExtractionError(RuntimeError):
    """Error base de la etapa de extracción."""


class ExtractionTimeoutError(ExtractionError):
    """La solicitud superó el timeout configurado."""


class ExtractionConnectionError(ExtractionError):
    """No fue posible establecer conexión con el proveedor."""


class ExtractionRequestError(ExtractionError):
    """La solicitud falló antes de obtener una respuesta HTTP válida."""


class ExtractionHTTPError(ExtractionError):
    """El proveedor respondió con un código HTTP de error."""

    def __init__(self, status_code: int, endpoint: str) -> None:
        self.status_code = status_code
        self.endpoint = endpoint
        super().__init__(
            f"IQAir respondió HTTP {status_code} en el endpoint {endpoint}"
        )


class InvalidJSONError(ExtractionError):
    """La respuesta del proveedor no contiene JSON válido."""


class ProviderResponseError(ExtractionError):
    """IQAir rechazó la solicitud dentro de una respuesta JSON válida."""

    def __init__(
        self,
        provider_status: str,
        provider_message: str | None = None,
    ) -> None:
        self.provider_status = provider_status
        self.provider_message = provider_message
        detail = (
            f": {provider_message}"
            if provider_message
            else ""
        )
        super().__init__(
            f"IQAir rechazó la solicitud con estado {provider_status}{detail}"
        )


class UnexpectedResponseStructureError(ExtractionError):
    """La respuesta no cumple la estructura mínima esperada."""

    def __init__(self, missing_fields: tuple[str, ...]) -> None:
        self.missing_fields = missing_fields
        fields = ", ".join(missing_fields)
        super().__init__(
            f"La respuesta de IQAir no contiene la estructura mínima: {fields}"
        )


class SensitiveResponseError(ExtractionError):
    """La respuesta contiene la API key y no puede conservarse."""


class RawDataWriteError(ExtractionError):
    """No fue posible conservar el JSON crudo."""


@dataclass(frozen=True, slots=True)
class ExtractionMetadata:
    """Metadatos no sensibles obtenidos durante una extracción."""

    provider_status: str
    city: str
    state: str
    country: str
    content_type: str | None
    response_size_bytes: int
    data_fields: tuple[str, ...]
    pollution_fields: tuple[str, ...]
    weather_fields: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ExtractionResult:
    """Resultado público de una extracción validada y persistida."""

    payload: dict[str, Any] = field(repr=False)
    raw_path: Path
    extracted_at: datetime
    endpoint: str
    status_code: int
    metadata: ExtractionMetadata


def extract_air_quality(
    settings: Settings,
    *,
    http_get: HttpGet = requests.get,
    timestamp_factory: Callable[[], datetime] = utc_now,
    token_factory: Callable[[], str] = lambda: uuid4().hex,
    logger: logging.Logger | None = None,
) -> ExtractionResult:
    """Consulta IQAir, valida la respuesta y conserva el JSON original."""

    endpoint = f"{settings.base_url.rstrip('/')}/{_CITY_ENDPOINT}"
    active_logger = logger or configure_safe_logger(
        __name__,
        api_key=settings.api_key,
    )
    active_logger.info(
        "Iniciando extracción de %s, %s, %s mediante %s",
        settings.city,
        settings.state,
        settings.country,
        endpoint,
    )

    response = _request_city_data(
        settings,
        endpoint=endpoint,
        http_get=http_get,
    )
    payload = _decode_json(response, endpoint=endpoint)
    data, pollution, weather = _validate_payload(
        payload,
        api_key=settings.api_key,
    )
    extracted_at = _normalize_utc_timestamp(timestamp_factory())
    raw_path = _save_raw_payload(
        payload,
        paths=settings.paths,
        extracted_at=extracted_at,
        token_factory=token_factory,
    )

    metadata = ExtractionMetadata(
        provider_status=str(payload["status"]),
        city=str(data["city"]),
        state=str(data["state"]),
        country=str(data["country"]),
        content_type=response.headers.get("Content-Type"),
        response_size_bytes=len(response.content),
        data_fields=tuple(sorted(data)),
        pollution_fields=tuple(sorted(pollution)),
        weather_fields=tuple(sorted(weather)),
    )
    active_logger.info(
        "Extracción completada con HTTP %s; archivo=%s",
        response.status_code,
        raw_path.name,
    )

    return ExtractionResult(
        payload=payload,
        raw_path=raw_path,
        extracted_at=extracted_at,
        endpoint=endpoint,
        status_code=response.status_code,
        metadata=metadata,
    )


def _request_city_data(
    settings: Settings,
    *,
    endpoint: str,
    http_get: HttpGet,
) -> ResponseLike:
    """Ejecuta la solicitud y traduce fallos de transporte o HTTP."""

    params = {
        "city": settings.city,
        "state": settings.state,
        "country": settings.country,
        "key": settings.api_key,
    }
    try:
        response = http_get(
            endpoint,
            params=params,
            timeout=settings.timeout_seconds,
        )
    except requests.Timeout:
        raise ExtractionTimeoutError(
            f"Timeout al consultar el endpoint {endpoint}"
        ) from None
    except requests.ConnectionError:
        raise ExtractionConnectionError(
            f"No fue posible conectar con el endpoint {endpoint}"
        ) from None
    except requests.RequestException:
        raise ExtractionRequestError(
            f"Falló la solicitud al endpoint {endpoint}"
        ) from None

    if response.status_code >= 400:
        raise ExtractionHTTPError(response.status_code, endpoint)
    return response


def _decode_json(
    response: ResponseLike,
    *,
    endpoint: str,
) -> dict[str, Any]:
    """Decodifica una respuesta JSON sin propagar detalles sensibles."""

    try:
        payload = response.json()
    except (TypeError, ValueError):
        raise InvalidJSONError(
            f"IQAir devolvió JSON inválido en el endpoint {endpoint}"
        ) from None

    if not isinstance(payload, dict):
        raise UnexpectedResponseStructureError(("objeto JSON raíz",))
    return payload


def _validate_payload(
    payload: dict[str, Any],
    *,
    api_key: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Valida estado del proveedor, datos sensibles y estructura mínima."""

    provider_status = payload.get("status")
    if provider_status != "success":
        safe_status = redact_sensitive_text(
            provider_status or "desconocido",
            secrets=(api_key,),
        )
        safe_message = _provider_error_message(payload, api_key=api_key)
        raise ProviderResponseError(safe_status, safe_message)

    if _contains_secret(payload, api_key):
        raise SensitiveResponseError(
            "IQAir devolvió una respuesta con información sensible; "
            "no se guardó el contenido"
        )

    missing: list[str] = []
    data = payload.get("data")
    if not isinstance(data, dict):
        raise UnexpectedResponseStructureError(("data",))

    for field_name in ("city", "state", "country"):
        value = data.get(field_name)
        if not isinstance(value, str) or not value.strip():
            missing.append(f"data.{field_name}")

    current = data.get("current")
    if not isinstance(current, dict):
        missing.append("data.current")
        current = {}

    pollution = current.get("pollution")
    if not isinstance(pollution, dict):
        missing.append("data.current.pollution")
        pollution = {}
    else:
        for field_name in ("ts", "aqius"):
            if field_name not in pollution:
                missing.append(f"data.current.pollution.{field_name}")

    weather = current.get("weather")
    if not isinstance(weather, dict):
        missing.append("data.current.weather")
        weather = {}

    if missing:
        raise UnexpectedResponseStructureError(tuple(missing))
    return data, pollution, weather


def _provider_error_message(
    payload: dict[str, Any],
    *,
    api_key: str,
) -> str | None:
    """Extrae un diagnóstico del proveedor sin conservar secretos."""

    data = payload.get("data")
    if isinstance(data, dict):
        message = data.get("message")
    else:
        message = data
    if message is None:
        return None
    return redact_sensitive_text(message, secrets=(api_key,))


def _contains_secret(value: Any, secret: str) -> bool:
    """Detecta si un objeto JSON contiene la API key."""

    if isinstance(value, str):
        return secret in value
    if isinstance(value, Mapping):
        return any(
            _contains_secret(key, secret)
            or _contains_secret(item, secret)
            for key, item in value.items()
        )
    if isinstance(value, list):
        return any(_contains_secret(item, secret) for item in value)
    return False


def _normalize_utc_timestamp(timestamp: datetime) -> datetime:
    """Exige un timestamp consciente y lo normaliza a UTC."""

    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ExtractionError(
            "El timestamp de extracción debe incluir zona horaria"
        )
    return timestamp.astimezone(UTC)


def _save_raw_payload(
    payload: dict[str, Any],
    *,
    paths: ProjectPaths,
    extracted_at: datetime,
    token_factory: Callable[[], str],
) -> Path:
    """Guarda el JSON con creación exclusiva y elimina archivos parciales."""

    ensure_data_directories(paths)
    timestamp_text = extracted_at.strftime("%Y%m%dT%H%M%S%fZ")

    for _ in range(_MAX_FILENAME_ATTEMPTS):
        token = _safe_filename_token(token_factory())
        raw_path = (
            paths.raw_dir
            / f"air_quality_{timestamp_text}_{token}.json"
        )
        try:
            with raw_path.open("x", encoding="utf-8") as file_handle:
                json.dump(
                    payload,
                    file_handle,
                    ensure_ascii=False,
                    indent=2,
                )
                file_handle.write("\n")
            return raw_path
        except FileExistsError:
            continue
        except (OSError, TypeError, ValueError):
            raw_path.unlink(missing_ok=True)
            raise RawDataWriteError(
                f"No fue posible guardar el JSON crudo: {raw_path.name}"
            ) from None

    raise RawDataWriteError(
        "No fue posible generar un nombre único para el JSON crudo"
    )


def _safe_filename_token(token: str) -> str:
    """Normaliza el token interno utilizado para evitar colisiones."""

    safe_token = _SAFE_TOKEN.sub("", str(token))[:16]
    if not safe_token:
        raise RawDataWriteError(
            "No fue posible generar un token seguro para el archivo"
        )
    return safe_token
