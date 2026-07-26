"""Pruebas unitarias de la extracción controlada de IQAir."""

from __future__ import annotations

import io
import json
import logging
from collections.abc import Callable, Mapping
from copy import deepcopy
from dataclasses import fields
from datetime import UTC, datetime
from inspect import signature
from pathlib import Path
from typing import Any

import pytest
import requests

from etl.config import Settings, load_settings
from etl.extract import (
    ExtractionConnectionError,
    ExtractionHTTPError,
    ExtractionResult,
    ExtractionTimeoutError,
    InvalidJSONError,
    ProviderResponseError,
    RawDataWriteError,
    SensitiveResponseError,
    UnexpectedResponseStructureError,
    extract_air_quality,
)
from etl.utils import REDACTED, configure_safe_logger


FIXED_TIMESTAMP = datetime(2026, 7, 24, 18, 30, 15, 123456, tzinfo=UTC)
FAKE_API_KEY = "fake-iqair-key-for-tests"


class FakeResponse:
    """Respuesta HTTP mínima para pruebas sin red."""

    def __init__(
        self,
        payload: Any,
        *,
        status_code: int = 200,
        json_error: Exception | None = None,
    ) -> None:
        self._payload = payload
        self._json_error = json_error
        self.status_code = status_code
        self.headers = {"Content-Type": "application/json"}
        self.content = json.dumps(payload).encode("utf-8")

    def json(self) -> Any:
        if self._json_error is not None:
            raise self._json_error
        return deepcopy(self._payload)


class RecordingGet:
    """GET simulado que conserva los argumentos de la solicitud."""

    def __init__(
        self,
        response: FakeResponse | None = None,
        *,
        error: Exception | None = None,
    ) -> None:
        self.response = response
        self.error = error
        self.calls: list[dict[str, Any]] = []

    def __call__(
        self,
        url: str,
        *,
        params: Mapping[str, str],
        timeout: float,
    ) -> FakeResponse:
        self.calls.append(
            {
                "url": url,
                "params": dict(params),
                "timeout": timeout,
            }
        )
        if self.error is not None:
            raise self.error
        assert self.response is not None
        return self.response


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    """Crea configuración aislada con credenciales ficticias."""

    return load_settings(
        env_file=tmp_path / "missing.env",
        environ={
            "IQAIR_API_KEY": FAKE_API_KEY,
            "IQAIR_CITY": "Mexico City",
            "IQAIR_STATE": "Mexico City",
            "IQAIR_COUNTRY": "Mexico",
            "IQAIR_BASE_URL": "https://api.airvisual.com/v2",
            "IQAIR_TIMEOUT_SECONDS": "12.5",
        },
        project_root=tmp_path,
    )


def run_extract(
    settings: Settings,
    payload: dict[str, Any],
    *,
    http_get: RecordingGet | None = None,
    token: str = "unique-token",
    logger: logging.Logger | None = None,
) -> ExtractionResult:
    """Ejecuta una extracción determinista para pruebas."""

    get = http_get or RecordingGet(FakeResponse(payload))
    return extract_air_quality(
        settings,
        http_get=get,
        timestamp_factory=lambda: FIXED_TIMESTAMP,
        token_factory=lambda: token,
        logger=logger,
    )


def test_public_extraction_contract_is_stable() -> None:
    assert tuple(signature(extract_air_quality).parameters) == (
        "settings",
        "http_get",
        "timestamp_factory",
        "token_factory",
        "logger",
    )
    assert tuple(field.name for field in fields(ExtractionResult)) == (
        "payload",
        "raw_path",
        "extracted_at",
        "endpoint",
        "status_code",
        "metadata",
    )


def test_successful_extraction_returns_typed_result(
    settings: Settings,
    sample_payload: dict[str, Any],
) -> None:
    result = run_extract(settings, sample_payload)

    assert isinstance(result, ExtractionResult)
    assert result.payload == sample_payload
    assert result.status_code == 200
    assert result.extracted_at == FIXED_TIMESTAMP
    assert result.endpoint == "https://api.airvisual.com/v2/city"
    assert FAKE_API_KEY not in repr(result)


def test_request_uses_city_params_and_configured_timeout(
    settings: Settings,
    sample_payload: dict[str, Any],
) -> None:
    http_get = RecordingGet(FakeResponse(sample_payload))

    run_extract(settings, sample_payload, http_get=http_get)

    assert http_get.calls == [
        {
            "url": "https://api.airvisual.com/v2/city",
            "params": {
                "city": "Mexico City",
                "state": "Mexico City",
                "country": "Mexico",
                "key": FAKE_API_KEY,
            },
            "timeout": 12.5,
        }
    ]


def test_valid_json_is_saved_with_same_logical_content(
    settings: Settings,
    sample_payload: dict[str, Any],
) -> None:
    result = run_extract(settings, sample_payload)

    assert result.raw_path.is_file()
    assert result.raw_path.parent == settings.paths.raw_dir
    assert result.raw_path.name.startswith(
        "air_quality_20260724T183015123456Z_"
    )
    assert json.loads(result.raw_path.read_text(encoding="utf-8")) == sample_payload
    assert FAKE_API_KEY not in result.raw_path.read_text(encoding="utf-8")


def test_metadata_contains_only_expected_non_sensitive_values(
    settings: Settings,
    sample_payload: dict[str, Any],
) -> None:
    result = run_extract(settings, sample_payload)

    assert result.metadata.provider_status == "success"
    assert result.metadata.city == "Mexico City"
    assert result.metadata.state == "Mexico City"
    assert result.metadata.country == "Mexico"
    assert result.metadata.content_type == "application/json"
    assert result.metadata.response_size_bytes > 0
    assert result.metadata.pollution_fields == ("aqius", "mainus", "ts")
    assert result.metadata.weather_fields == ("hu", "pr", "tp", "ts", "wd", "ws")
    assert FAKE_API_KEY not in repr(result.metadata)


def test_close_executions_generate_unique_filenames(
    settings: Settings,
    sample_payload: dict[str, Any],
) -> None:
    tokens = iter(("first-token", "second-token"))

    first = extract_air_quality(
        settings,
        http_get=RecordingGet(FakeResponse(sample_payload)),
        timestamp_factory=lambda: FIXED_TIMESTAMP,
        token_factory=lambda: next(tokens),
    )
    second = extract_air_quality(
        settings,
        http_get=RecordingGet(FakeResponse(sample_payload)),
        timestamp_factory=lambda: FIXED_TIMESTAMP,
        token_factory=lambda: next(tokens),
    )

    assert first.raw_path != second.raw_path
    assert first.raw_path.is_file()
    assert second.raw_path.is_file()


def test_timeout_is_reported_without_api_key(settings: Settings) -> None:
    http_get = RecordingGet(error=requests.Timeout("sensitive request"))

    with pytest.raises(ExtractionTimeoutError) as captured:
        extract_air_quality(settings, http_get=http_get)

    assert FAKE_API_KEY not in str(captured.value)


def test_connection_error_is_reported_without_api_key(
    settings: Settings,
) -> None:
    http_get = RecordingGet(
        error=requests.ConnectionError(
            f"https://example.test?key={FAKE_API_KEY}"
        )
    )

    with pytest.raises(ExtractionConnectionError) as captured:
        extract_air_quality(settings, http_get=http_get)

    assert FAKE_API_KEY not in str(captured.value)


@pytest.mark.parametrize("status_code", [401, 429, 500, 503])
def test_http_errors_are_distinguished_by_status(
    status_code: int,
    settings: Settings,
    sample_payload: dict[str, Any],
) -> None:
    http_get = RecordingGet(
        FakeResponse(sample_payload, status_code=status_code)
    )

    with pytest.raises(ExtractionHTTPError) as captured:
        extract_air_quality(settings, http_get=http_get)

    assert captured.value.status_code == status_code
    assert FAKE_API_KEY not in str(captured.value)
    assert not settings.paths.raw_dir.exists()


def test_invalid_json_is_rejected(
    settings: Settings,
    sample_payload: dict[str, Any],
) -> None:
    response = FakeResponse(
        sample_payload,
        json_error=ValueError("invalid json"),
    )

    with pytest.raises(InvalidJSONError):
        extract_air_quality(settings, http_get=RecordingGet(response))

    assert not settings.paths.raw_dir.exists()


def test_provider_error_is_sanitized(
    settings: Settings,
) -> None:
    payload = {
        "status": "fail",
        "data": {
            "message": f"incorrect_api_key: {FAKE_API_KEY}",
        },
    }

    with pytest.raises(ProviderResponseError) as captured:
        extract_air_quality(
            settings,
            http_get=RecordingGet(FakeResponse(payload)),
        )

    assert captured.value.provider_status == "fail"
    assert FAKE_API_KEY not in str(captured.value)
    assert REDACTED in str(captured.value)
    assert not settings.paths.raw_dir.exists()


@pytest.mark.parametrize(
    "missing_path",
    [
        ("data",),
        ("data", "city"),
        ("data", "current"),
        ("data", "current", "pollution"),
        ("data", "current", "pollution", "ts"),
        ("data", "current", "pollution", "aqius"),
        ("data", "current", "weather"),
    ],
)
def test_incomplete_minimum_structure_is_rejected(
    missing_path: tuple[str, ...],
    settings: Settings,
    sample_payload: dict[str, Any],
) -> None:
    payload = deepcopy(sample_payload)
    target: dict[str, Any] = payload
    for key in missing_path[:-1]:
        target = target[key]
    target.pop(missing_path[-1])

    with pytest.raises(UnexpectedResponseStructureError):
        run_extract(settings, payload)

    assert not settings.paths.raw_dir.exists()


def test_write_failure_removes_partial_file(
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
    sample_payload: dict[str, Any],
) -> None:
    def fail_open(
        self: Path,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        raise PermissionError("simulated write failure")

    monkeypatch.setattr(Path, "open", fail_open)

    with pytest.raises(RawDataWriteError) as captured:
        run_extract(settings, sample_payload)

    assert FAKE_API_KEY not in str(captured.value)
    assert list(settings.paths.raw_dir.glob("*.json")) == []


def test_api_key_is_absent_from_logs_exceptions_results_and_files(
    settings: Settings,
    sample_payload: dict[str, Any],
    isolated_logger: Callable[[str], logging.Logger],
) -> None:
    stream = io.StringIO()
    logger_name = "etl.tests.extract.security"
    isolated_logger(logger_name)
    logger = configure_safe_logger(
        logger_name,
        api_key=FAKE_API_KEY,
        stream=stream,
    )

    result = run_extract(
        settings,
        sample_payload,
        logger=logger,
    )

    assert FAKE_API_KEY not in stream.getvalue()
    assert FAKE_API_KEY not in result.endpoint
    assert FAKE_API_KEY not in repr(result)
    assert FAKE_API_KEY not in result.raw_path.read_text(encoding="utf-8")


def test_response_echoing_api_key_is_never_saved_or_returned(
    settings: Settings,
    sample_payload: dict[str, Any],
) -> None:
    payload = deepcopy(sample_payload)
    payload["data"]["debug"] = f"key={FAKE_API_KEY}"

    with pytest.raises(SensitiveResponseError) as captured:
        run_extract(settings, payload)

    assert FAKE_API_KEY not in str(captured.value)
    assert not settings.paths.raw_dir.exists()
