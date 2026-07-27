"""Fixtures compartidos y barreras de aislamiento para la suite."""

from __future__ import annotations

import json
import logging
import socket
from collections.abc import Callable, Iterator
from ipaddress import ip_address
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


def pytest_addoption(parser: pytest.Parser) -> None:
    """Registra la habilitación explícita de la prueba contra IQAir."""

    parser.addoption(
        "--real-api",
        action="store_true",
        default=False,
        help="habilita la prueba opcional de contrato contra IQAir",
    )


def pytest_configure(config: pytest.Config) -> None:
    """Documenta la marca de la prueba que puede usar red y credenciales."""

    config.addinivalue_line(
        "markers",
        "real_api: prueba opcional que realiza una única consulta real a IQAir",
    )


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    """Omite pruebas reales salvo que se soliciten expresamente."""

    if config.getoption("--real-api"):
        return
    skip_real_api = pytest.mark.skip(
        reason="requiere habilitación explícita con --real-api",
    )
    for item in items:
        if item.get_closest_marker("real_api") is not None:
            item.add_marker(skip_real_api)


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
    request: pytest.FixtureRequest,
) -> None:
    """Bloquea internet y permite únicamente loopback para Prefect local."""

    if _real_api_enabled(request):
        return

    def block_requests(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("Las pruebas ordinarias no pueden usar Requests")

    original_connect = socket.socket.connect
    original_create_connection = socket.create_connection

    def guarded_connect(
        active_socket: socket.socket,
        address: Any,
    ) -> Any:
        if _is_loopback_address(address):
            return original_connect(active_socket, address)
        raise AssertionError(
            "Las pruebas ordinarias no pueden conectarse a internet"
        )

    def guarded_create_connection(
        address: Any,
        *args: Any,
        **kwargs: Any,
    ) -> socket.socket:
        if _is_loopback_address(address):
            return original_create_connection(address, *args, **kwargs)
        raise AssertionError(
            "Las pruebas ordinarias no pueden conectarse a internet"
        )

    monkeypatch.setattr(requests.sessions.Session, "request", block_requests)
    monkeypatch.setattr(socket.socket, "connect", guarded_connect)
    monkeypatch.setattr(
        socket,
        "create_connection",
        guarded_create_connection,
    )
    for variable in _IQAIR_ENVIRONMENT_VARIABLES:
        monkeypatch.delenv(variable, raising=False)


@pytest.fixture(autouse=True)
def prevent_real_dotenv_access(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    """Permite dotenv temporales, pero prohíbe leer el archivo real."""

    if _real_api_enabled(request):
        return

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


def _real_api_enabled(request: pytest.FixtureRequest) -> bool:
    """Indica si una prueba real fue marcada y habilitada por CLI."""

    return (
        request.node.get_closest_marker("real_api") is not None
        and bool(request.config.getoption("--real-api"))
    )


def _is_loopback_address(address: Any) -> bool:
    """Acepta loopback TCP y sockets Unix, pero rechaza destinos externos."""

    if isinstance(address, (str, bytes)):
        return True
    if not isinstance(address, tuple) or not address:
        return False
    host = str(address[0]).split("%", maxsplit=1)[0]
    if host.casefold() == "localhost":
        return True
    try:
        return ip_address(host).is_loopback
    except ValueError:
        return False
