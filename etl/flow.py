"""Orquestación Prefect del pipeline ETL de calidad del aire."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from prefect import flow, task
from prefect.states import State

from etl.config import Settings, load_settings
from etl.extract import (
    ExtractionConnectionError,
    ExtractionHTTPError,
    ExtractionResult,
    ExtractionTimeoutError,
    InvalidJSONError,
    extract_air_quality,
)
from etl.load import (
    ExportIssue,
    ExportResult,
    LoadResult,
    export_air_quality,
    load_air_quality,
)
from etl.transform import (
    QualityResult,
    TransformResult,
    TransformWarning,
    transform_air_quality,
    validate_air_quality,
)
from etl.utils import utc_now


EXTRACTION_RETRIES = 2
EXTRACTION_RETRY_DELAYS_SECONDS = (1.0, 5.0)

PipelineStatus = Literal["completed", "completed_with_export_errors"]
TransactionStatus = Literal["committed", "rolled_back", "no_changes"]
ExportStatus = Literal["exported", "failed"]


@dataclass(frozen=True, slots=True)
class PipelineRunSummary:
    """Resumen no sensible de una ejecución completa del pipeline."""

    started_at: datetime
    finished_at: datetime
    status: PipelineStatus
    raw_json_path: Path
    records_transformed: int
    records_valid: int
    records_rejected: int
    rows_inserted: int
    rows_duplicated: int
    transaction_status: TransactionStatus
    csv_path: Path
    parquet_path: Path
    rejected_csv_path: Path
    csv_status: ExportStatus
    parquet_status: ExportStatus
    rejected_csv_status: ExportStatus
    warnings: tuple[TransformWarning, ...]
    errors: tuple[ExportIssue, ...]


def _retry_transient_extraction(
    prefect_task: Any,
    task_run: Any,
    state: State[Any],
) -> bool:
    """Permite reintentos únicamente para fallos transitorios de Extract."""

    del prefect_task, task_run
    failure = state.data
    if isinstance(failure, State):
        failure = failure.data
    if isinstance(
        failure,
        (
            ExtractionTimeoutError,
            ExtractionConnectionError,
            InvalidJSONError,
        ),
    ):
        return True
    return (
        isinstance(failure, ExtractionHTTPError)
        and (
            failure.status_code == 429
            or 500 <= failure.status_code < 600
        )
    )


@task(
    name="extract-air-quality",
    retries=EXTRACTION_RETRIES,
    retry_delay_seconds=list(EXTRACTION_RETRY_DELAYS_SECONDS),
    retry_condition_fn=_retry_transient_extraction,
    cache_policy=None,
    persist_result=False,
)
def extract_air_quality_task(settings: Settings) -> ExtractionResult:
    """Ejecuta el componente Extract aprobado."""

    return extract_air_quality(settings)


@task(
    name="transform-air-quality",
    retries=0,
    cache_policy=None,
    persist_result=False,
)
def transform_air_quality_task(
    extraction_result: ExtractionResult,
) -> TransformResult:
    """Ejecuta el componente Transform aprobado."""

    return transform_air_quality(extraction_result)


@task(
    name="validate-air-quality",
    retries=0,
    cache_policy=None,
    persist_result=False,
)
def validate_air_quality_task(
    transform_result: TransformResult,
) -> QualityResult:
    """Ejecuta la validación y clasificación aprobadas."""

    return validate_air_quality(transform_result)


@task(
    name="load-air-quality",
    retries=0,
    cache_policy=None,
    persist_result=False,
)
def load_air_quality_task(
    quality_result: QualityResult,
    *,
    database_path: Path,
) -> LoadResult:
    """Ejecuta el componente Load aprobado."""

    return load_air_quality(
        quality_result,
        database_path=database_path,
    )


@task(
    name="export-air-quality",
    retries=0,
    cache_policy=None,
    persist_result=False,
)
def export_air_quality_task(
    quality_result: QualityResult,
    *,
    database_path: Path,
    output_directory: Path,
) -> ExportResult:
    """Ejecuta las exportaciones aprobadas."""

    return export_air_quality(
        quality_result,
        database_path=database_path,
        output_directory=output_directory,
    )


@flow(
    name="pipeline-calidad-aire-cdmx",
    retries=0,
    persist_result=False,
    log_prints=False,
)
def air_quality_flow() -> PipelineRunSummary:
    """Ejecuta Extract, Transform, Validate, Load y Export en orden."""

    started_at = utc_now()
    settings = load_settings()
    extraction_result = extract_air_quality_task(settings)
    transform_result = transform_air_quality_task(extraction_result)
    quality_result = validate_air_quality_task(transform_result)
    load_result = load_air_quality_task(
        quality_result,
        database_path=settings.paths.database_path,
    )
    export_result = export_air_quality_task(
        quality_result,
        database_path=load_result.database_path,
        output_directory=settings.paths.processed_dir,
    )
    export_failed = bool(export_result.errors) or "failed" in (
        export_result.csv_status,
        export_result.parquet_status,
        export_result.rejected_csv_status,
    )

    return PipelineRunSummary(
        started_at=started_at,
        finished_at=utc_now(),
        status=(
            "completed_with_export_errors"
            if export_failed
            else "completed"
        ),
        raw_json_path=extraction_result.raw_path,
        records_transformed=transform_result.records_transformed,
        records_valid=quality_result.total_valid,
        records_rejected=quality_result.total_rejected,
        rows_inserted=load_result.rows_inserted,
        rows_duplicated=load_result.rows_duplicated,
        transaction_status=load_result.transaction_status,
        csv_path=export_result.csv_path,
        parquet_path=export_result.parquet_path,
        rejected_csv_path=export_result.rejected_csv_path,
        csv_status=export_result.csv_status,
        parquet_status=export_result.parquet_status,
        rejected_csv_status=export_result.rejected_csv_status,
        warnings=quality_result.warnings,
        errors=export_result.errors,
    )
