"""Persistencia transaccional de registros válidos en SQLite."""

from __future__ import annotations

import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Literal

import pandas as pd
from sqlalchemy import (
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    URL,
    create_engine,
    insert,
    select,
)
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError

from etl.config import build_project_paths
from etl.transform import (
    REJECTED_COLUMNS,
    QualityResult,
    SCHEMA_COLUMNS,
)


TABLE_NAME = "calidad_aire"
_SHA256_HEX = re.compile(r"^[0-9a-fA-F]{64}$")
_TransactionStatus = Literal["committed", "rolled_back", "no_changes"]
_ExportStatus = Literal["exported", "failed"]
_ExportTarget = Literal["csv", "parquet", "rejected_csv"]

METADATA = MetaData()
AIR_QUALITY_TABLE = Table(
    TABLE_NAME,
    METADATA,
    Column("record_id", String(64), primary_key=True, nullable=False),
    Column("city", String(255), nullable=False),
    Column("state", String(255), nullable=False),
    Column("country", String(255), nullable=False),
    Column("latitude", Float, nullable=True),
    Column("longitude", Float, nullable=True),
    Column("timestamp_api", String(27), nullable=False),
    Column("timestamp_extraction", String(27), nullable=False),
    Column("aqius", Integer, nullable=False),
    Column("main_pollutant", String(32), nullable=True),
    Column("temperature_c", Float, nullable=True),
    Column("humidity_pct", Float, nullable=True),
    Column("pressure_hpa", Float, nullable=True),
    Column("wind_speed_ms", Float, nullable=True),
    Column("wind_direction_deg", Float, nullable=True),
)


class LoadError(RuntimeError):
    """Error base de la persistencia de calidad del aire."""


class LoadSchemaError(LoadError):
    """El DataFrame no cumple el esquema confirmado."""

    def __init__(self, missing_columns: tuple[str, ...]) -> None:
        self.missing_columns = missing_columns
        columns = ", ".join(missing_columns)
        super().__init__(
            f"El DataFrame no contiene las columnas requeridas: {columns}"
        )


class InvalidRecordIdError(LoadError):
    """Un registro no contiene un SHA-256 utilizable como clave primaria."""

    def __init__(self, row_number: int) -> None:
        self.row_number = row_number
        super().__init__(
            f"record_id inválido en la fila {row_number}: "
            "se requiere un SHA-256 hexadecimal de 64 caracteres"
        )


class InvalidLoadTimestampError(LoadError):
    """Un timestamp no puede serializarse de forma inequívoca en UTC."""

    def __init__(self, field_name: str, row_number: int) -> None:
        self.field_name = field_name
        self.row_number = row_number
        super().__init__(
            f"{field_name} inválido en la fila {row_number}: "
            "se requiere un timestamp con zona horaria"
        )


@dataclass(frozen=True, slots=True)
class LoadResult:
    """Resultado tipado de una operación de persistencia."""

    database_path: Path
    table_name: str
    rows_received: int
    rows_inserted: int
    rows_duplicated: int
    transaction_status: _TransactionStatus


class LoadTransactionError(LoadError):
    """La transacción falló y todos sus cambios fueron revertidos."""

    def __init__(self, result: LoadResult) -> None:
        self.result = result
        super().__init__(
            "La transacción de carga falló y fue revertida completamente"
        )


class ExportError(LoadError):
    """Error base de las exportaciones de archivos procesados."""


class ExportSchemaError(ExportError):
    """Los registros rechazados no cumplen el esquema esperado."""

    def __init__(self, missing_columns: tuple[str, ...]) -> None:
        self.missing_columns = missing_columns
        columns = ", ".join(missing_columns)
        super().__init__(
            f"Los registros rechazados no contienen las columnas: {columns}"
        )


class ExportReadError(ExportError):
    """No fue posible leer el historial consolidado desde SQLite."""


@dataclass(frozen=True, slots=True)
class ExportIssue:
    """Descripción no sensible de una exportación fallida."""

    target: _ExportTarget
    error_type: str
    message: str


@dataclass(frozen=True, slots=True)
class ExportResult:
    """Resultado tipado de exportar archivos válidos y rechazados."""

    csv_path: Path
    parquet_path: Path
    rejected_csv_path: Path
    csv_rows_exported: int
    parquet_rows_exported: int
    valid_rows_exported: int
    rejected_rows_exported: int
    csv_status: _ExportStatus
    parquet_status: _ExportStatus
    rejected_csv_status: _ExportStatus
    errors: tuple[ExportIssue, ...]


def load_air_quality(
    source: QualityResult | pd.DataFrame,
    *,
    database_path: Path | None = None,
) -> LoadResult:
    """Inserta registros válidos nuevos en una transacción por lote."""

    dataframe = _resolve_valid_records(source)
    missing_columns = tuple(
        column
        for column in SCHEMA_COLUMNS
        if column not in dataframe.columns
    )
    if missing_columns:
        raise LoadSchemaError(missing_columns)

    resolved_path = (
        database_path or build_project_paths().database_path
    ).expanduser().resolve()
    resolved_path.parent.mkdir(parents=True, exist_ok=True)

    source_copy = dataframe.loc[:, SCHEMA_COLUMNS].copy(deep=True)
    records = _prepare_records(source_copy)
    unique_records, internal_duplicates = _deduplicate_batch(records)
    rows_received = len(source_copy)
    known_duplicates = internal_duplicates
    engine = create_engine(
        URL.create("sqlite", database=str(resolved_path)),
        future=True,
    )

    try:
        METADATA.create_all(engine, tables=(AIR_QUALITY_TABLE,))
        with engine.begin() as connection:
            existing_ids = _existing_record_ids(
                connection,
                tuple(unique_records),
            )
            new_records = [
                record
                for record_id, record in unique_records.items()
                if record_id not in existing_ids
            ]
            known_duplicates += len(existing_ids)
            if new_records:
                connection.execute(insert(AIR_QUALITY_TABLE), new_records)

        rows_inserted = len(new_records)
        status: _TransactionStatus = (
            "committed" if rows_inserted else "no_changes"
        )
        return LoadResult(
            database_path=resolved_path,
            table_name=TABLE_NAME,
            rows_received=rows_received,
            rows_inserted=rows_inserted,
            rows_duplicated=known_duplicates,
            transaction_status=status,
        )
    except SQLAlchemyError as exc:
        result = LoadResult(
            database_path=resolved_path,
            table_name=TABLE_NAME,
            rows_received=rows_received,
            rows_inserted=0,
            rows_duplicated=known_duplicates,
            transaction_status="rolled_back",
        )
        raise LoadTransactionError(result) from exc
    finally:
        engine.dispose()


def export_air_quality(
    quality_result: QualityResult,
    *,
    database_path: Path | None = None,
    output_directory: Path | None = None,
) -> ExportResult:
    """Exporta el historial SQLite y los rechazos de la ejecución actual."""

    if not isinstance(quality_result, QualityResult):
        raise ExportError("La exportación requiere un QualityResult")

    project_paths = build_project_paths()
    resolved_database_path = (
        database_path or project_paths.database_path
    ).expanduser().resolve()
    resolved_output_directory = (
        output_directory or project_paths.processed_dir
    ).expanduser().resolve()
    csv_path = resolved_output_directory / (
        project_paths.processed_csv_path.name
    )
    parquet_path = resolved_output_directory / (
        project_paths.processed_parquet_path.name
    )
    rejected_csv_path = resolved_output_directory / (
        project_paths.rejected_csv_path.name
    )

    history = _read_consolidated_records(resolved_database_path)
    rejected = _copy_rejected_records(quality_result.rejected_records)
    export_jobs: tuple[
        tuple[
            _ExportTarget,
            pd.DataFrame,
            Path,
            Callable[[pd.DataFrame, Path], None],
        ],
        ...,
    ] = (
        ("csv", history, csv_path, _write_csv_file),
        ("parquet", history, parquet_path, _write_parquet_file),
        (
            "rejected_csv",
            rejected,
            rejected_csv_path,
            _write_rejected_csv_file,
        ),
    )

    statuses: dict[_ExportTarget, _ExportStatus] = {}
    rows_exported: dict[_ExportTarget, int] = {}
    issues: list[ExportIssue] = []
    for target, dataframe, destination, writer in export_jobs:
        try:
            _atomic_write(dataframe, destination, writer)
        except Exception as exc:
            statuses[target] = "failed"
            rows_exported[target] = 0
            issues.append(
                ExportIssue(
                    target=target,
                    error_type=type(exc).__name__,
                    message=f"No fue posible completar la exportación {target}",
                )
            )
        else:
            statuses[target] = "exported"
            rows_exported[target] = len(dataframe)

    return ExportResult(
        csv_path=csv_path,
        parquet_path=parquet_path,
        rejected_csv_path=rejected_csv_path,
        csv_rows_exported=rows_exported["csv"],
        parquet_rows_exported=rows_exported["parquet"],
        valid_rows_exported=min(
            rows_exported["csv"],
            rows_exported["parquet"],
        ),
        rejected_rows_exported=rows_exported["rejected_csv"],
        csv_status=statuses["csv"],
        parquet_status=statuses["parquet"],
        rejected_csv_status=statuses["rejected_csv"],
        errors=tuple(issues),
    )


def _resolve_valid_records(
    source: QualityResult | pd.DataFrame,
) -> pd.DataFrame:
    """Obtiene exclusivamente los registros válidos de la entrada."""

    if isinstance(source, QualityResult):
        return source.valid_records
    if isinstance(source, pd.DataFrame):
        return source
    raise LoadError("La carga requiere QualityResult o un DataFrame")


def _read_consolidated_records(database_path: Path) -> pd.DataFrame:
    """Lee todo el historial con orden reproducible y sin escribir en SQLite."""

    if not database_path.is_file():
        raise ExportReadError(
            "No existe una base SQLite disponible para exportar"
        )
    engine = create_engine(
        URL.create("sqlite", database=str(database_path)),
        future=True,
    )
    statement = select(AIR_QUALITY_TABLE).order_by(
        AIR_QUALITY_TABLE.c.timestamp_api,
        AIR_QUALITY_TABLE.c.record_id,
    )
    try:
        with engine.connect() as connection:
            rows = connection.execute(statement).mappings().all()
    except SQLAlchemyError:
        raise ExportReadError(
            "No fue posible leer la tabla consolidada de calidad del aire"
        ) from None
    finally:
        engine.dispose()
    return pd.DataFrame(
        (dict(row) for row in rows),
        columns=SCHEMA_COLUMNS,
    )


def _copy_rejected_records(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Copia los rechazos actuales con columnas completas y orden estable."""

    missing_columns = tuple(
        column
        for column in REJECTED_COLUMNS
        if column not in dataframe.columns
    )
    if missing_columns:
        raise ExportSchemaError(missing_columns)
    return dataframe.loc[:, REJECTED_COLUMNS].copy(deep=True)


def _atomic_write(
    dataframe: pd.DataFrame,
    destination: Path,
    writer: Callable[[pd.DataFrame, Path], None],
) -> None:
    """Escribe en el mismo directorio y reemplaza el destino al completar."""

    destination.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temporary_name = tempfile.mkstemp(
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=".tmp",
    )
    os.close(file_descriptor)
    temporary_path = Path(temporary_name)
    try:
        writer(dataframe, temporary_path)
        temporary_path.replace(destination)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise


def _write_csv_file(dataframe: pd.DataFrame, destination: Path) -> None:
    """Escribe un CSV UTF-8 sin índice."""

    dataframe.to_csv(destination, index=False, encoding="utf-8")


def _write_parquet_file(
    dataframe: pd.DataFrame,
    destination: Path,
) -> None:
    """Escribe Parquet mediante PyArrow sin índice."""

    dataframe.to_parquet(
        destination,
        index=False,
        engine="pyarrow",
    )


def _write_rejected_csv_file(
    dataframe: pd.DataFrame,
    destination: Path,
) -> None:
    """Escribe el CSV de rechazos de la ejecución actual."""

    dataframe.to_csv(destination, index=False, encoding="utf-8")


def _prepare_records(dataframe: pd.DataFrame) -> list[dict[str, Any]]:
    """Convierte escalares de Pandas a valores compatibles con SQLite."""

    records: list[dict[str, Any]] = []
    for row_number, values in enumerate(
        dataframe.itertuples(index=False, name=None)
    ):
        record = dict(zip(SCHEMA_COLUMNS, values, strict=True))
        record_id = record["record_id"]
        if (
            not isinstance(record_id, str)
            or _SHA256_HEX.fullmatch(record_id) is None
        ):
            raise InvalidRecordIdError(row_number)
        record["record_id"] = record_id.casefold()
        record["timestamp_api"] = _serialize_utc_timestamp(
            record["timestamp_api"],
            field_name="timestamp_api",
            row_number=row_number,
        )
        record["timestamp_extraction"] = _serialize_utc_timestamp(
            record["timestamp_extraction"],
            field_name="timestamp_extraction",
            row_number=row_number,
        )
        for column in SCHEMA_COLUMNS:
            if column in (
                "record_id",
                "timestamp_api",
                "timestamp_extraction",
            ):
                continue
            record[column] = _database_scalar(record[column])
        records.append(record)
    return records


def _serialize_utc_timestamp(
    value: Any,
    *,
    field_name: str,
    row_number: int,
) -> str:
    """Serializa un timestamp consciente como ISO 8601 UTC estable."""

    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError):
        raise InvalidLoadTimestampError(field_name, row_number) from None
    if (
        pd.isna(timestamp)
        or timestamp.tzinfo is None
        or timestamp.utcoffset() is None
    ):
        raise InvalidLoadTimestampError(field_name, row_number)
    normalized = timestamp.tz_convert("UTC")
    return normalized.isoformat(timespec="microseconds").replace(
        "+00:00",
        "Z",
    )


def _database_scalar(value: Any) -> Any:
    """Convierte nulos y escalares de extensión a tipos nativos."""

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        return value.item()
    return value


def _deduplicate_batch(
    records: list[dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], int]:
    """Conserva la primera aparición y cuenta repeticiones internas."""

    unique_records: dict[str, dict[str, Any]] = {}
    duplicated = 0
    for record in records:
        record_id = record["record_id"]
        if record_id in unique_records:
            duplicated += 1
            continue
        unique_records[record_id] = record
    return unique_records, duplicated


def _existing_record_ids(
    connection: Connection,
    record_ids: tuple[str, ...],
) -> set[str]:
    """Consulta las claves primarias existentes antes de insertar."""

    if not record_ids:
        return set()
    statement = select(AIR_QUALITY_TABLE.c.record_id).where(
        AIR_QUALITY_TABLE.c.record_id.in_(record_ids)
    )
    return set(connection.execute(statement).scalars())
