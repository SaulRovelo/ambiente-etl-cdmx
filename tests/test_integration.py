"""Pruebas integrales y contrato real opcional del pipeline."""

from __future__ import annotations

import io
import json
import os
import sqlite3
from collections.abc import Callable, Iterator, Mapping
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from prefect import get_client
from prefect.client.schemas.filters import (
    TaskRunFilter,
    TaskRunFilterFlowRunId,
)
from prefect.client.schemas.objects import State, TaskRun
from prefect.testing.utilities import prefect_test_harness

import etl.flow as flow_module
import etl.load as load_module
from etl.config import (
    DEFAULT_PROJECT_ROOT,
    ConfigurationError,
    Settings,
    load_settings,
)
from etl.extract import (
    ExtractionResult,
    UnexpectedResponseStructureError,
    extract_air_quality,
)
from etl.flow import PipelineRunSummary, air_quality_flow
from etl.transform import REJECTED_COLUMNS, SCHEMA_COLUMNS
from etl.utils import configure_safe_logger


FAKE_API_KEY = "fake-integration-key-for-tests"
FIXED_EXTRACTION_TIME = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)


class SimulatedIQAirResponse:
    """Respuesta HTTP mínima y determinista para el flujo integral."""

    def __init__(
        self,
        payload: dict[str, Any],
        *,
        status_code: int = 200,
    ) -> None:
        self._payload = deepcopy(payload)
        self.status_code = status_code
        self.headers: Mapping[str, str] = {
            "Content-Type": "application/json",
        }
        self.content = json.dumps(
            payload,
            ensure_ascii=False,
        ).encode("utf-8")

    def json(self) -> dict[str, Any]:
        """Devuelve una copia para evitar estado compartido entre ejecuciones."""

        return deepcopy(self._payload)


@pytest.fixture(scope="module")
def prefect_runtime() -> Iterator[None]:
    """Inicia Prefect localmente con base temporal y telemetría desactivada."""

    environment = pytest.MonkeyPatch()
    environment.setenv("PREFECT_SERVER_ANALYTICS_ENABLED", "false")
    environment.setenv("PREFECT_TELEMETRY_ENABLE_RESOURCE_METRICS", "false")
    environment.setenv("PREFECT_LOGGING_LEVEL", "WARNING")
    try:
        with prefect_test_harness():
            yield
    finally:
        environment.undo()


def make_settings(tmp_path: Path) -> Settings:
    """Construye configuración ficticia con todas las rutas bajo tmp_path."""

    return load_settings(
        env_file=tmp_path / "missing.env",
        environ={
            "IQAIR_API_KEY": FAKE_API_KEY,
            "IQAIR_CITY": "Mexico City",
            "IQAIR_STATE": "Mexico City",
            "IQAIR_COUNTRY": "Mexico",
            "IQAIR_BASE_URL": "https://api.airvisual.com/v2",
            "IQAIR_TIMEOUT_SECONDS": "10",
        },
        project_root=tmp_path,
    )


def configure_simulated_api(
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
    payload: dict[str, Any],
    isolated_logger: Callable[[str], Any],
) -> tuple[list[dict[str, Any]], io.StringIO]:
    """Inyecta configuración y transporte HTTP simulado en Extract."""

    calls: list[dict[str, Any]] = []
    stream = io.StringIO()
    logger_name = f"etl.tests.integration.{id(calls)}"
    isolated_logger(logger_name)
    logger = configure_safe_logger(
        logger_name,
        api_key=settings.api_key,
        stream=stream,
    )

    def fake_get(
        url: str,
        *,
        params: Mapping[str, str],
        timeout: float,
    ) -> SimulatedIQAirResponse:
        calls.append(
            {
                "url": url,
                "params": dict(params),
                "timeout": timeout,
            }
        )
        return SimulatedIQAirResponse(payload)

    def simulated_extract(active_settings: Settings) -> ExtractionResult:
        return extract_air_quality(
            active_settings,
            http_get=fake_get,
            timestamp_factory=lambda: FIXED_EXTRACTION_TIME,
            token_factory=lambda: f"integration-{len(calls)}",
            logger=logger,
        )

    monkeypatch.setattr(flow_module, "load_settings", lambda: settings)
    monkeypatch.setattr(
        flow_module,
        "extract_air_quality",
        simulated_extract,
    )
    return calls, stream


def run_flow_with_state() -> tuple[State[PipelineRunSummary], PipelineRunSummary]:
    """Ejecuta el flow decorado y devuelve estado Prefect y resumen."""

    state = air_quality_flow(return_state=True)
    assert isinstance(state, State)
    summary = state.result()
    assert isinstance(summary, PipelineRunSummary)
    return state, summary


def read_task_runs(flow_state: State[Any]) -> list[TaskRun]:
    """Consulta los estados de tareas asociados a una ejecución Prefect."""

    flow_run_id = flow_state.state_details.flow_run_id
    assert flow_run_id is not None
    with get_client(sync_client=True) as client:
        return client.read_task_runs(
            task_run_filter=TaskRunFilter(
                flow_run_id=TaskRunFilterFlowRunId(
                    any_=[flow_run_id],
                )
            )
        )


def read_sqlite_history(database_path: Path) -> pd.DataFrame:
    """Lee el historial temporal sin depender de SQLAlchemy interno."""

    with sqlite3.connect(database_path) as connection:
        return pd.read_sql_query(
            "SELECT * FROM calidad_aire "
            "ORDER BY timestamp_api, record_id",
            connection,
        )


def assert_successful_prefect_states(flow_state: State[Any]) -> None:
    """Comprueba un flow completo y sus cinco tareas terminadas."""

    assert flow_state.is_completed()
    task_runs = read_task_runs(flow_state)
    assert len(task_runs) == 5
    assert all(task_run.state.is_completed() for task_run in task_runs)


def test_end_to_end_flow_is_reproducible_and_idempotent(
    prefect_runtime: None,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    sample_payload: dict[str, Any],
    isolated_logger: Callable[[str], Any],
) -> None:
    settings = make_settings(tmp_path)
    calls, log_stream = configure_simulated_api(
        monkeypatch,
        settings,
        sample_payload,
        isolated_logger,
    )

    first_state, first = run_flow_with_state()
    second_state, second = run_flow_with_state()

    assert_successful_prefect_states(first_state)
    assert_successful_prefect_states(second_state)
    assert len(calls) == 2
    assert first.status == "completed"
    assert first.records_transformed == 1
    assert first.records_valid == 1
    assert first.records_rejected == 0
    assert first.rows_inserted == 1
    assert first.rows_duplicated == 0
    assert first.transaction_status == "committed"
    assert second.rows_inserted == 0
    assert second.rows_duplicated == 1
    assert second.transaction_status == "no_changes"

    assert first.raw_json_path.parent == settings.paths.raw_dir
    assert json.loads(
        first.raw_json_path.read_text(encoding="utf-8")
    ) == sample_payload
    assert len(list(settings.paths.raw_dir.glob("*.json"))) == 2
    assert first.csv_path == settings.paths.processed_csv_path
    assert first.parquet_path == settings.paths.processed_parquet_path
    assert first.rejected_csv_path == settings.paths.rejected_csv_path
    assert first.csv_status == "exported"
    assert first.parquet_status == "exported"
    assert first.rejected_csv_status == "exported"

    sqlite_history = read_sqlite_history(settings.paths.database_path)
    csv_history = pd.read_csv(first.csv_path)
    parquet_history = pd.read_parquet(first.parquet_path)
    assert tuple(sqlite_history.columns) == SCHEMA_COLUMNS
    pd.testing.assert_frame_equal(
        sqlite_history,
        csv_history,
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        sqlite_history,
        parquet_history,
        check_dtype=False,
    )
    assert len(sqlite_history) == 1
    assert FAKE_API_KEY not in repr(first)
    assert FAKE_API_KEY not in log_stream.getvalue()


def test_rejected_record_is_exported_without_sqlite_insertion(
    prefect_runtime: None,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    sample_payload: dict[str, Any],
    isolated_logger: Callable[[str], Any],
) -> None:
    settings = make_settings(tmp_path)
    rejected_payload = deepcopy(sample_payload)
    rejected_payload["data"]["current"]["pollution"]["aqius"] = -1
    configure_simulated_api(
        monkeypatch,
        settings,
        rejected_payload,
        isolated_logger,
    )

    flow_state, summary = run_flow_with_state()

    assert_successful_prefect_states(flow_state)
    assert summary.records_valid == 0
    assert summary.records_rejected == 1
    assert summary.rows_inserted == 0
    assert summary.transaction_status == "no_changes"
    assert read_sqlite_history(settings.paths.database_path).empty
    rejected = pd.read_csv(summary.rejected_csv_path)
    assert tuple(rejected.columns) == REJECTED_COLUMNS
    assert len(rejected) == 1
    assert "aqius" in rejected.loc[0, "rejection_reason"]
    assert pd.read_csv(summary.csv_path).empty
    assert pd.read_parquet(summary.parquet_path).empty


def test_critical_extract_failure_stops_prefect_dependencies(
    prefect_runtime: None,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    isolated_logger: Callable[[str], Any],
) -> None:
    settings = make_settings(tmp_path)
    incomplete_payload = {"status": "success", "data": {}}
    calls, _ = configure_simulated_api(
        monkeypatch,
        settings,
        incomplete_payload,
        isolated_logger,
    )

    flow_state = air_quality_flow(return_state=True)

    assert flow_state.is_failed()
    with pytest.raises(UnexpectedResponseStructureError):
        flow_state.result()
    task_runs = read_task_runs(flow_state)
    assert len(task_runs) == 1
    assert task_runs[0].state.is_failed()
    assert task_runs[0].run_count == 1
    assert len(calls) == 1
    assert not settings.paths.database_path.exists()
    assert not settings.paths.processed_dir.exists()
    assert not settings.paths.raw_dir.exists()


def test_export_failure_preserves_committed_sqlite_data(
    prefect_runtime: None,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    sample_payload: dict[str, Any],
    isolated_logger: Callable[[str], Any],
) -> None:
    settings = make_settings(tmp_path)
    configure_simulated_api(
        monkeypatch,
        settings,
        sample_payload,
        isolated_logger,
    )

    def fail_parquet(dataframe: pd.DataFrame, destination: Path) -> None:
        del dataframe, destination
        raise OSError("fallo simulado de Parquet")

    monkeypatch.setattr(
        load_module,
        "_write_parquet_file",
        fail_parquet,
    )

    flow_state, summary = run_flow_with_state()

    assert_successful_prefect_states(flow_state)
    assert summary.status == "completed_with_export_errors"
    assert summary.rows_inserted == 1
    assert summary.transaction_status == "committed"
    assert summary.csv_status == "exported"
    assert summary.parquet_status == "failed"
    assert summary.rejected_csv_status == "exported"
    assert len(summary.errors) == 1
    assert summary.errors[0].target == "parquet"
    assert len(read_sqlite_history(settings.paths.database_path)) == 1
    assert summary.csv_path.is_file()
    assert not summary.parquet_path.exists()
    assert summary.rejected_csv_path.is_file()


@pytest.mark.real_api
def test_real_iqair_contract_once(tmp_path: Path) -> None:
    """Realiza una única consulta real solo bajo habilitación explícita."""

    try:
        settings = load_settings(
            env_file=DEFAULT_PROJECT_ROOT / ".env",
            environ=os.environ,
            project_root=tmp_path,
        )
    except ConfigurationError as exc:
        pytest.skip(f"configuración real no disponible: {type(exc).__name__}")

    result = extract_air_quality(settings)

    assert result.status_code == 200
    assert result.metadata.provider_status == "success"
    assert result.raw_path.parent == tmp_path / "data" / "raw"
    assert settings.api_key not in repr(result)
