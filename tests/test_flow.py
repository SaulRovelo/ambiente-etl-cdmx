"""Pruebas unitarias de la orquestación Prefect del pipeline."""

from __future__ import annotations

import json
from collections.abc import Callable
from copy import deepcopy
from dataclasses import fields
from datetime import UTC, datetime, timedelta
from inspect import signature
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from prefect.flows import Flow
from prefect.states import Failed
from prefect.tasks import Task

import etl.flow as flow_module
from etl.config import ConfigurationError, Settings, load_settings
from etl.extract import (
    ExtractionConnectionError,
    ExtractionHTTPError,
    ExtractionMetadata,
    ExtractionResult,
    ExtractionTimeoutError,
    InvalidJSONError,
    ProviderResponseError,
    RawDataWriteError,
    UnexpectedResponseStructureError,
)
from etl.flow import (
    EXTRACTION_RETRIES,
    EXTRACTION_RETRY_DELAYS_SECONDS,
    PipelineRunSummary,
    air_quality_flow,
    export_air_quality_task,
    extract_air_quality_task,
    load_air_quality_task,
    transform_air_quality_task,
    validate_air_quality_task,
)
from etl.load import (
    ExportIssue,
    ExportResult,
    LoadResult,
    LoadTransactionError,
    load_air_quality,
)
from etl.transform import (
    QualityValidationError,
    REJECTED_COLUMNS,
    TransformationError,
)


FAKE_API_KEY = "fake-flow-key-for-tests"
STARTED_AT = datetime(2026, 7, 25, 14, 0, tzinfo=UTC)
FINISHED_AT = STARTED_AT + timedelta(seconds=2)
_TASK_ATTRIBUTES = (
    "extract_air_quality_task",
    "transform_air_quality_task",
    "validate_air_quality_task",
    "load_air_quality_task",
    "export_air_quality_task",
)
_TASK_ORDER = ("extract", "transform", "validate", "load", "export")


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    """Crea configuración aislada con rutas temporales."""

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


def make_extraction_result(
    payload: dict[str, Any],
    settings: Settings,
) -> ExtractionResult:
    """Construye una extracción simulada dentro del directorio temporal."""

    raw_path = settings.paths.raw_dir / "air_quality_simulated.json"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_text(
        json.dumps(payload, ensure_ascii=False),
        encoding="utf-8",
    )
    data = payload["data"]
    pollution = data["current"]["pollution"]
    weather = data["current"]["weather"]
    return ExtractionResult(
        payload=deepcopy(payload),
        raw_path=raw_path,
        extracted_at=datetime(2026, 7, 25, 13, 59, tzinfo=UTC),
        endpoint="https://api.airvisual.com/v2/city",
        status_code=200,
        metadata=ExtractionMetadata(
            provider_status="success",
            city=data["city"],
            state=data["state"],
            country=data["country"],
            content_type="application/json",
            response_size_bytes=raw_path.stat().st_size,
            data_fields=tuple(sorted(data)),
            pollution_fields=tuple(sorted(pollution)),
            weather_fields=tuple(sorted(weather)),
        ),
    )


def make_export_result(
    settings: Settings,
    *,
    failed_target: str | None = None,
) -> ExportResult:
    """Construye un resultado de exportación seguro y determinista."""

    errors = (
        (
            ExportIssue(
                target=failed_target,
                error_type="OSError",
                message=f"No fue posible completar la exportación {failed_target}",
            ),
        )
        if failed_target is not None
        else ()
    )
    return ExportResult(
        csv_path=settings.paths.processed_csv_path,
        parquet_path=settings.paths.processed_parquet_path,
        rejected_csv_path=settings.paths.rejected_csv_path,
        csv_rows_exported=0 if failed_target == "csv" else 1,
        parquet_rows_exported=0 if failed_target == "parquet" else 1,
        valid_rows_exported=0 if failed_target in {"csv", "parquet"} else 1,
        rejected_rows_exported=(
            0 if failed_target == "rejected_csv" else 0
        ),
        csv_status="failed" if failed_target == "csv" else "exported",
        parquet_status=(
            "failed" if failed_target == "parquet" else "exported"
        ),
        rejected_csv_status=(
            "failed" if failed_target == "rejected_csv" else "exported"
        ),
        errors=errors,
    )


def install_direct_task_calls(
    monkeypatch: pytest.MonkeyPatch,
    calls: list[str],
) -> None:
    """Ejecuta el cuerpo de las tareas sin iniciar el motor o servidor Prefect."""

    for attribute, label in zip(
        _TASK_ATTRIBUTES,
        _TASK_ORDER,
        strict=True,
    ):
        prefect_task = getattr(flow_module, attribute)

        def direct_call(
            *args: Any,
            _function: Callable[..., Any] = prefect_task.fn,
            _label: str = label,
            **kwargs: Any,
        ) -> Any:
            calls.append(_label)
            return _function(*args, **kwargs)

        monkeypatch.setattr(flow_module, attribute, direct_call)


def configure_simulated_flow(
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
    extraction_result: ExtractionResult,
    calls: list[str],
) -> None:
    """Inyecta configuración y Extract simulados; conserva T/V/L/E reales."""

    monkeypatch.setattr(flow_module, "load_settings", lambda: settings)
    monkeypatch.setattr(
        flow_module,
        "extract_air_quality",
        lambda received_settings: extraction_result,
    )
    install_direct_task_calls(monkeypatch, calls)


def test_public_flow_contract_and_prefect_objects_are_stable() -> None:
    assert isinstance(air_quality_flow, Flow)
    assert air_quality_flow.name == "pipeline-calidad-aire-cdmx"
    assert tuple(signature(air_quality_flow.fn).parameters) == ()
    assert tuple(field.name for field in fields(PipelineRunSummary)) == (
        "started_at",
        "finished_at",
        "status",
        "raw_json_path",
        "records_transformed",
        "records_valid",
        "records_rejected",
        "rows_inserted",
        "rows_duplicated",
        "transaction_status",
        "csv_path",
        "parquet_path",
        "rejected_csv_path",
        "csv_status",
        "parquet_status",
        "rejected_csv_status",
        "warnings",
        "errors",
    )

    prefect_tasks = (
        extract_air_quality_task,
        transform_air_quality_task,
        validate_air_quality_task,
        load_air_quality_task,
        export_air_quality_task,
    )
    assert all(isinstance(prefect_task, Task) for prefect_task in prefect_tasks)
    assert extract_air_quality_task.retries == EXTRACTION_RETRIES
    assert extract_air_quality_task.retry_delay_seconds == list(
        EXTRACTION_RETRY_DELAYS_SECONDS
    )
    assert all(
        prefect_task.retries == 0
        for prefect_task in prefect_tasks[1:]
    )
    assert all(
        prefect_task.persist_result is False
        for prefect_task in prefect_tasks
    )
    assert air_quality_flow.retries == 0
    assert air_quality_flow.persist_result is False


def test_tasks_delegate_directly_to_approved_components(
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
) -> None:
    extraction_result = object()
    transform_result = object()
    quality_result = object()
    load_result = object()
    export_result = object()
    calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def delegate(name: str, result: object) -> Callable[..., object]:
        def call(*args: Any, **kwargs: Any) -> object:
            calls.append((name, args, kwargs))
            return result

        return call

    monkeypatch.setattr(
        flow_module,
        "extract_air_quality",
        delegate("extract", extraction_result),
    )
    monkeypatch.setattr(
        flow_module,
        "transform_air_quality",
        delegate("transform", transform_result),
    )
    monkeypatch.setattr(
        flow_module,
        "validate_air_quality",
        delegate("validate", quality_result),
    )
    monkeypatch.setattr(
        flow_module,
        "load_air_quality",
        delegate("load", load_result),
    )
    monkeypatch.setattr(
        flow_module,
        "export_air_quality",
        delegate("export", export_result),
    )

    assert extract_air_quality_task.fn(settings) is extraction_result
    assert transform_air_quality_task.fn(extraction_result) is transform_result
    assert validate_air_quality_task.fn(transform_result) is quality_result
    assert (
        load_air_quality_task.fn(
            quality_result,
            database_path=settings.paths.database_path,
        )
        is load_result
    )
    assert (
        export_air_quality_task.fn(
            quality_result,
            database_path=settings.paths.database_path,
            output_directory=settings.paths.processed_dir,
        )
        is export_result
    )

    assert [name for name, _, _ in calls] == list(_TASK_ORDER)
    assert calls[3][2] == {
        "database_path": settings.paths.database_path,
    }
    assert calls[4][2] == {
        "database_path": settings.paths.database_path,
        "output_directory": settings.paths.processed_dir,
    }


@pytest.mark.parametrize(
    "error",
    [
        ExtractionTimeoutError("timeout"),
        ExtractionConnectionError("connection"),
        InvalidJSONError("invalid JSON"),
        ExtractionHTTPError(429, "https://api.example.test/v2/city"),
        ExtractionHTTPError(500, "https://api.example.test/v2/city"),
        ExtractionHTTPError(503, "https://api.example.test/v2/city"),
    ],
)
def test_transient_extraction_errors_are_retryable(
    error: Exception,
) -> None:
    retry_condition = extract_air_quality_task.retry_condition_fn

    assert retry_condition is not None
    assert retry_condition(None, None, Failed(data=error)) is True


@pytest.mark.parametrize(
    "error",
    [
        ExtractionHTTPError(400, "https://api.example.test/v2/city"),
        ExtractionHTTPError(401, "https://api.example.test/v2/city"),
        ProviderResponseError("fail", "invalid key"),
        UnexpectedResponseStructureError(("data",)),
        RawDataWriteError("write failed"),
        TransformationError("transform failed"),
        QualityValidationError("quality failed"),
    ],
)
def test_permanent_errors_are_not_retryable(error: Exception) -> None:
    retry_condition = extract_air_quality_task.retry_condition_fn

    assert retry_condition is not None
    assert retry_condition(None, None, Failed(data=error)) is False


def test_complete_simulated_execution_is_ordered_and_idempotent(
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
    sample_payload: dict[str, Any],
) -> None:
    extraction_result = make_extraction_result(sample_payload, settings)
    calls: list[str] = []
    timestamps = iter(
        (
            STARTED_AT,
            FINISHED_AT,
            STARTED_AT + timedelta(minutes=1),
            FINISHED_AT + timedelta(minutes=1),
        )
    )
    configure_simulated_flow(
        monkeypatch,
        settings,
        extraction_result,
        calls,
    )
    monkeypatch.setattr(flow_module, "utc_now", lambda: next(timestamps))

    first = air_quality_flow.fn()
    second = air_quality_flow.fn()

    assert calls == list(_TASK_ORDER) * 2
    assert first.started_at == STARTED_AT
    assert first.finished_at == FINISHED_AT
    assert first.status == "completed"
    assert first.raw_json_path == extraction_result.raw_path
    assert first.records_transformed == 1
    assert first.records_valid == 1
    assert first.records_rejected == 0
    assert first.rows_inserted == 1
    assert first.rows_duplicated == 0
    assert first.transaction_status == "committed"
    assert first.warnings == ()
    assert first.errors == ()
    assert second.rows_inserted == 0
    assert second.rows_duplicated == 1
    assert second.transaction_status == "no_changes"
    assert first.csv_path.is_file()
    assert first.parquet_path.is_file()
    assert first.rejected_csv_path.is_file()


def test_warnings_are_propagated_to_summary(
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
    sample_payload: dict[str, Any],
) -> None:
    payload = deepcopy(sample_payload)
    payload["data"]["current"]["weather"]["ts"] = (
        "2026-07-24T19:00:00.000Z"
    )
    extraction_result = make_extraction_result(payload, settings)
    configure_simulated_flow(
        monkeypatch,
        settings,
        extraction_result,
        [],
    )

    summary = air_quality_flow.fn()

    assert [warning.code for warning in summary.warnings] == [
        "weather_timestamp_mismatch"
    ]


def test_zero_valid_records_still_loads_and_exports_rejections(
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
    sample_payload: dict[str, Any],
) -> None:
    payload = deepcopy(sample_payload)
    payload["data"]["current"]["pollution"]["aqius"] = -1
    extraction_result = make_extraction_result(payload, settings)
    calls: list[str] = []
    configure_simulated_flow(
        monkeypatch,
        settings,
        extraction_result,
        calls,
    )

    summary = air_quality_flow.fn()

    assert calls == list(_TASK_ORDER)
    assert summary.records_transformed == 1
    assert summary.records_valid == 0
    assert summary.records_rejected == 1
    assert summary.rows_inserted == 0
    assert summary.rows_duplicated == 0
    assert summary.transaction_status == "no_changes"
    rejected = pd.read_csv(summary.rejected_csv_path)
    assert tuple(rejected.columns) == REJECTED_COLUMNS
    assert len(rejected) == 1
    assert "aqius" in rejected.loc[0, "rejection_reason"]


def test_invalid_configuration_stops_before_extract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        flow_module,
        "load_settings",
        lambda: (_ for _ in ()).throw(
            ConfigurationError("configuración inválida")
        ),
    )
    install_direct_task_calls(monkeypatch, calls)

    with pytest.raises(ConfigurationError, match="configuración inválida"):
        air_quality_flow.fn()

    assert calls == []


@pytest.mark.parametrize(
    ("failing_stage", "expected_calls"),
    [
        ("extract", ["extract"]),
        ("transform", ["extract", "transform"]),
        ("validate", ["extract", "transform", "validate"]),
        ("load", ["extract", "transform", "validate", "load"]),
    ],
)
def test_critical_failure_stops_dependent_tasks(
    failing_stage: str,
    expected_calls: list[str],
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
) -> None:
    calls: list[str] = []
    results: dict[str, Any] = {
        "extract": object(),
        "transform": object(),
        "validate": object(),
        "load": object(),
        "export": object(),
    }
    errors: dict[str, Exception] = {
        "extract": ExtractionTimeoutError("timeout definitivo"),
        "transform": TransformationError("transformación inválida"),
        "validate": QualityValidationError("calidad inválida"),
        "load": LoadTransactionError(
            LoadResult(
                database_path=settings.paths.database_path,
                table_name="calidad_aire",
                rows_received=1,
                rows_inserted=0,
                rows_duplicated=0,
                transaction_status="rolled_back",
            )
        ),
    }
    monkeypatch.setattr(flow_module, "load_settings", lambda: settings)

    for attribute, label in zip(
        _TASK_ATTRIBUTES,
        _TASK_ORDER,
        strict=True,
    ):
        def stage(
            *args: Any,
            _label: str = label,
            **kwargs: Any,
        ) -> Any:
            del args, kwargs
            calls.append(_label)
            if _label == failing_stage:
                raise errors[_label]
            return results[_label]

        monkeypatch.setattr(flow_module, attribute, stage)

    with pytest.raises(type(errors[failing_stage])):
        air_quality_flow.fn()

    assert calls == expected_calls


def test_export_failure_does_not_revert_committed_sqlite_data(
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
    sample_payload: dict[str, Any],
) -> None:
    extraction_result = make_extraction_result(sample_payload, settings)
    failed_export = make_export_result(
        settings,
        failed_target="parquet",
    )
    configure_simulated_flow(
        monkeypatch,
        settings,
        extraction_result,
        [],
    )
    monkeypatch.setattr(
        flow_module,
        "export_air_quality",
        lambda *args, **kwargs: failed_export,
    )

    summary = air_quality_flow.fn()

    assert summary.status == "completed_with_export_errors"
    assert summary.rows_inserted == 1
    assert summary.transaction_status == "committed"
    assert summary.parquet_status == "failed"
    assert len(summary.errors) == 1

    duplicate_check = load_air_quality(
        flow_module.validate_air_quality(
            flow_module.transform_air_quality(extraction_result)
        ),
        database_path=settings.paths.database_path,
    )
    assert duplicate_check.rows_inserted == 0
    assert duplicate_check.rows_duplicated == 1
    assert duplicate_check.transaction_status == "no_changes"


def test_summary_tasks_and_errors_do_not_expose_api_key(
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
    sample_payload: dict[str, Any],
) -> None:
    extraction_result = make_extraction_result(sample_payload, settings)
    configure_simulated_flow(
        monkeypatch,
        settings,
        extraction_result,
        [],
    )
    monkeypatch.setattr(
        flow_module,
        "export_air_quality",
        lambda *args, **kwargs: make_export_result(
            settings,
            failed_target="csv",
        ),
    )

    summary = air_quality_flow.fn()

    assert FAKE_API_KEY not in repr(settings)
    assert FAKE_API_KEY not in repr(summary)
    assert all(
        FAKE_API_KEY not in issue.message
        for issue in summary.errors
    )
    assert extract_air_quality_task.persist_result is False
    assert air_quality_flow.persist_result is False
