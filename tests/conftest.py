"""Fixtures compartidos y barreras de aislamiento para la suite."""

from __future__ import annotations

import json
import logging
import socket
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
import requests

import etl.config as config_module


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "iqair_success.json"
_IQAIR_ENVIRONMENT_VARIABLES = (
    "IQAIR_API_KEY",
    "IQAIR_CITY",
    "IQAIR_STATE",
    "IQAIR_COUNTRY",
    "IQAIR_BASE_URL",
    "IQAIR_TIMEOUT_SECONDS",
)


@pytest.fixture
def sample_payload() -> dict[str, Any]:
    """Carga una copia nueva del fixture público y sin secretos."""

    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


@pytest.fixture
def isolated_logger() -> Iterator[Callable[[str], logging.Logger]]:
    """Restaura el estado de los loggers configurados durante una prueba."""

    snapshots: dict[
        str,
        tuple[int, bool, bool, tuple[logging.Handler, ...]],
    ] = {}

    def remember(logger_name: str) -> logging.Logger:
        logger = logging.getLogger(logger_name)
        snapshots.setdefault(
            logger_name,
            (
                logger.level,
                logger.propagate,
                logger.disabled,
                tuple(logger.handlers),
            ),
        )
        return logger

    yield remember

    for logger_name, state in snapshots.items():
        level, propagate, disabled, previous_handlers = state
        logger = logging.getLogger(logger_name)
        for handler in tuple(logger.handlers):
            if handler not in previous_handlers:
                handler.close()
        logger.handlers[:] = list(previous_handlers)
        logger.setLevel(level)
        logger.propagate = propagate
        logger.disabled = disabled


@pytest.fixture(autouse=True)
def isolate_network_and_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Impide red y evita que las pruebas hereden credenciales IQAir."""

    def block_network(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("Las pruebas no pueden acceder a la red")

    monkeypatch.setattr(requests.sessions.Session, "request", block_network)
    monkeypatch.setattr(socket.socket, "connect", block_network)
    monkeypatch.setattr(socket, "create_connection", block_network)
    for variable in _IQAIR_ENVIRONMENT_VARIABLES:
        monkeypatch.delenv(variable, raising=False)


@pytest.fixture(autouse=True)
def prevent_real_dotenv_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Permite dotenv temporales, pero prohíbe leer el archivo real."""

    project_env = (
        config_module.DEFAULT_PROJECT_ROOT / ".env"
    ).resolve()
    original_dotenv_values = config_module.dotenv_values

    def guarded_dotenv_values(
        dotenv_path: str | Path,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        if Path(dotenv_path).expanduser().resolve() == project_env:
            raise AssertionError(
                "La suite no puede leer el archivo .env del proyecto"
            )
        return original_dotenv_values(dotenv_path, *args, **kwargs)

    monkeypatch.setattr(
        config_module,
        "dotenv_values",
        guarded_dotenv_values,
    )


@pytest.fixture(autouse=True)
def preserve_project_data_directory() -> Iterator[None]:
    """Comprueba que cada prueba deje intactos los datos del proyecto."""

    before = _data_snapshot()
    yield
    assert _data_snapshot() == before


def _data_snapshot() -> tuple[tuple[str, int, int], ...]:
    """Describe archivos de datos sin leer su contenido."""

    data_directory = config_module.DEFAULT_PROJECT_ROOT / "data"
    if not data_directory.exists():
        return ()
    return tuple(
        sorted(
            (
                str(path.relative_to(data_directory)),
                path.stat().st_size,
                path.stat().st_mtime_ns,
            )
            for path in data_directory.rglob("*")
            if path.is_file()
        )
    )
