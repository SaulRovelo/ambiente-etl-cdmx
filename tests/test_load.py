"""Pruebas unitarias de persistencia SQLite."""

from __future__ import annotations

from dataclasses import fields
from datetime import UTC, datetime, timedelta, timezone
from inspect import signature
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from sqlalchemy import URL, create_engine, inspect, select, text

import etl.load as load_module
from etl.load import (
    AIR_QUALITY_TABLE,
    TABLE_NAME,
    ExportReadError,
    ExportResult,
    ExportSchemaError,
    InvalidRecordIdError,
    LoadResult,
    LoadSchemaError,
    LoadTransactionError,
    export_air_quality,
    load_air_quality,
)
from etl.transform import (
    REJECTED_COLUMNS,
    SCHEMA_COLUMNS,
    QualityResult,
)
from etl.utils import generate_record_id


BASE_TIMESTAMP = datetime(2026, 7, 25, 12, 0, tzinfo=UTC)


def make_valid_dataframe(
    count: int = 1,
    *,
    start_at: int = 0,
) -> pd.DataFrame:
    """Crea registros válidos deterministas sin utilizar red."""

    rows: list[dict[str, Any]] = []
    for offset in range(start_at, start_at + count):
        timestamp_api = BASE_TIMESTAMP + timedelta(minutes=offset)
        city = f"Ciudad {offset}"
        state = "Estado Ejemplo"
        country = "País Ejemplo"
        rows.append(
            {
                "record_id": generate_record_id(
                    city,
                    state,
                    country,
                    timestamp_api,
                ),
                "city": city,
                "state": state,
                "country": country,
                "latitude": 19.4326,
                "longitude": -99.1332,
                "timestamp_api": pd.Timestamp(timestamp_api),
                "timestamp_extraction": pd.Timestamp(
                    timestamp_api + timedelta(seconds=30)
                ),
                "aqius": 42 + offset,
                "main_pollutant": "p2",
                "temperature_c": 24.0,
                "humidity_pct": 46.0,
                "pressure_hpa": 1016.0,
                "wind_speed_ms": 2.1,
                "wind_direction_deg": 180.0,
            }
        )
    return pd.DataFrame(rows, columns=SCHEMA_COLUMNS)


def sqlite_url(database_path: Path) -> URL:
    """Construye una URL SQLite segura para una base temporal."""

    return URL.create("sqlite", database=str(database_path))


def read_rows(database_path: Path) -> list[dict[str, Any]]:
    """Lee la tabla completa como mappings para verificar persistencia."""

    engine = create_engine(sqlite_url(database_path))
    try:
        with engine.connect() as connection:
            return [
                dict(row)
                for row in connection.execute(
                    select(AIR_QUALITY_TABLE).order_by(
                        AIR_QUALITY_TABLE.c.record_id
                    )
                ).mappings()
            ]
    finally:
        engine.dispose()


def make_rejected_dataframe(count: int = 1) -> pd.DataFrame:
    """Crea rechazos representativos de una ejecución actual."""

    rows: list[dict[str, Any]] = []
    for offset in range(count):
        row = make_valid_dataframe(1, start_at=offset).iloc[0].to_dict()
        row["record_id"] = pd.NA
        row["humidity_pct"] = "valor inválido"
        row["rejection_reason"] = (
            "humidity_pct: debe ser numérico cuando está presente"
        )
        rows.append(row)
    return pd.DataFrame(rows, columns=REJECTED_COLUMNS)


def make_quality_result(
    *,
    valid_records: pd.DataFrame | None = None,
    rejected_records: pd.DataFrame | None = None,
) -> QualityResult:
    """Construye el contrato de calidad para pruebas de exportación."""

    valid = (
        make_valid_dataframe()
        if valid_records is None
        else valid_records
    )
    rejected = (
        pd.DataFrame(columns=REJECTED_COLUMNS)
        if rejected_records is None
        else rejected_records
    )
    return QualityResult(
        valid_records=valid,
        rejected_records=rejected,
        warnings=(),
        total_received=len(valid) + len(rejected),
        total_valid=len(valid),
        total_rejected=len(rejected),
    )


def test_public_load_and_export_contracts_are_stable() -> None:
    assert tuple(signature(load_air_quality).parameters) == (
        "source",
        "database_path",
    )
    assert tuple(signature(export_air_quality).parameters) == (
        "quality_result",
        "database_path",
        "output_directory",
    )
    assert tuple(field.name for field in fields(LoadResult)) == (
        "database_path",
        "table_name",
        "rows_received",
        "rows_inserted",
        "rows_duplicated",
        "transaction_status",
    )
    assert tuple(field.name for field in fields(ExportResult)) == (
        "csv_path",
        "parquet_path",
        "rejected_csv_path",
        "csv_rows_exported",
        "parquet_rows_exported",
        "valid_rows_exported",
        "rejected_rows_exported",
        "csv_status",
        "parquet_status",
        "rejected_csv_status",
        "errors",
    )


def test_load_creates_database_directory_and_table(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "nested" / "ambiente.db"

    result = load_air_quality(
        make_valid_dataframe(),
        database_path=database_path,
    )

    assert isinstance(result, LoadResult)
    assert result.database_path == database_path.resolve()
    assert result.table_name == TABLE_NAME
    assert database_path.is_file()
    engine = create_engine(sqlite_url(database_path))
    try:
        assert inspect(engine).has_table(TABLE_NAME)
    finally:
        engine.dispose()


def test_table_has_confirmed_schema_primary_key_and_nullability(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "schema.db"
    load_air_quality(
        pd.DataFrame(columns=SCHEMA_COLUMNS),
        database_path=database_path,
    )
    engine = create_engine(sqlite_url(database_path))
    try:
        inspector = inspect(engine)
        columns = {
            column["name"]: column
            for column in inspector.get_columns(TABLE_NAME)
        }
        primary_key = inspector.get_pk_constraint(TABLE_NAME)
    finally:
        engine.dispose()

    assert tuple(columns) == SCHEMA_COLUMNS
    assert primary_key["constrained_columns"] == ["record_id"]
    required = {
        "record_id",
        "city",
        "state",
        "country",
        "timestamp_api",
        "timestamp_extraction",
        "aqius",
    }
    assert all(not columns[column]["nullable"] for column in required)
    assert all(
        columns[column]["nullable"]
        for column in set(SCHEMA_COLUMNS) - required
    )


@pytest.mark.parametrize("row_count", [1, 4])
def test_load_inserts_one_or_multiple_records(
    row_count: int,
    tmp_path: Path,
) -> None:
    database_path = tmp_path / f"insert_{row_count}.db"

    result = load_air_quality(
        make_valid_dataframe(row_count),
        database_path=database_path,
    )

    assert result.rows_received == row_count
    assert result.rows_inserted == row_count
    assert result.rows_duplicated == 0
    assert result.transaction_status == "committed"
    assert len(read_rows(database_path)) == row_count


def test_second_load_is_idempotent(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "idempotent.db"
    dataframe = make_valid_dataframe(2)
    first = load_air_quality(dataframe, database_path=database_path)

    second = load_air_quality(dataframe, database_path=database_path)

    assert first.rows_inserted == 2
    assert second.rows_received == 2
    assert second.rows_inserted == 0
    assert second.rows_duplicated == 2
    assert second.transaction_status == "no_changes"
    assert len(read_rows(database_path)) == 2


def test_load_counts_duplicates_repeated_inside_batch(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "internal_duplicates.db"
    first = make_valid_dataframe()
    second = make_valid_dataframe(1, start_at=1)
    batch = pd.concat((first, first, second, second), ignore_index=True)

    result = load_air_quality(batch, database_path=database_path)

    assert result.rows_received == 4
    assert result.rows_inserted == 2
    assert result.rows_duplicated == 2
    assert result.transaction_status == "committed"
    assert result.rows_inserted + result.rows_duplicated == 4


def test_load_counts_existing_and_new_records(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "mixed_duplicates.db"
    existing = make_valid_dataframe()
    load_air_quality(existing, database_path=database_path)
    new = make_valid_dataframe(1, start_at=1)
    batch = pd.concat((existing, new, new), ignore_index=True)

    result = load_air_quality(batch, database_path=database_path)

    assert result.rows_received == 3
    assert result.rows_inserted == 1
    assert result.rows_duplicated == 2
    assert result.rows_inserted + result.rows_duplicated == 3
    assert len(read_rows(database_path)) == 2


def test_empty_input_creates_table_without_changes(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "empty.db"

    result = load_air_quality(
        pd.DataFrame(columns=SCHEMA_COLUMNS),
        database_path=database_path,
    )

    assert result.rows_received == 0
    assert result.rows_inserted == 0
    assert result.rows_duplicated == 0
    assert result.transaction_status == "no_changes"
    assert read_rows(database_path) == []


def test_quality_result_never_loads_rejected_records(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "quality_result.db"
    valid = make_valid_dataframe()
    rejected_row = make_valid_dataframe(1, start_at=1).iloc[0].to_dict()
    rejected_row["record_id"] = pd.NA
    rejected_row["rejection_reason"] = "registro rechazado de prueba"
    rejected = pd.DataFrame([rejected_row], columns=REJECTED_COLUMNS)
    source = QualityResult(
        valid_records=valid,
        rejected_records=rejected,
        warnings=(),
        total_received=2,
        total_valid=1,
        total_rejected=1,
    )

    result = load_air_quality(source, database_path=database_path)

    assert result.rows_received == 1
    assert result.rows_inserted == 1
    rows = read_rows(database_path)
    assert len(rows) == 1
    assert rows[0]["record_id"] == valid.loc[0, "record_id"]


@pytest.mark.parametrize(
    "invalid_record_id",
    [pd.NA, None, "", "abc", "g" * 64, "a" * 63, "a" * 65],
)
def test_load_rejects_missing_or_invalid_record_id(
    invalid_record_id: Any,
    tmp_path: Path,
) -> None:
    dataframe = make_valid_dataframe()
    dataframe["record_id"] = dataframe["record_id"].astype("object")
    dataframe.loc[0, "record_id"] = invalid_record_id

    with pytest.raises(InvalidRecordIdError, match="SHA-256"):
        load_air_quality(
            dataframe,
            database_path=tmp_path / "invalid_id.db",
        )


def test_load_preserves_nulls_and_serializes_timestamps_in_utc(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "timestamps.db"
    dataframe = make_valid_dataframe()
    timestamp_api = datetime(
        2026,
        7,
        25,
        6,
        0,
        1,
        123456,
        tzinfo=timezone(timedelta(hours=-6)),
    )
    timestamp_extraction = timestamp_api + timedelta(seconds=30)
    dataframe.loc[0, "timestamp_api"] = timestamp_api
    dataframe.loc[0, "timestamp_extraction"] = timestamp_extraction
    optional_columns = (
        "latitude",
        "longitude",
        "main_pollutant",
        "temperature_c",
        "humidity_pct",
        "pressure_hpa",
        "wind_speed_ms",
        "wind_direction_deg",
    )
    for column in optional_columns:
        dataframe[column] = pd.NA

    result = load_air_quality(dataframe, database_path=database_path)
    row = read_rows(database_path)[0]

    assert result.rows_inserted == 1
    assert row["timestamp_api"] == "2026-07-25T12:00:01.123456Z"
    assert row["timestamp_extraction"] == (
        "2026-07-25T12:00:31.123456Z"
    )
    assert all(row[column] is None for column in optional_columns)
    assert pd.to_datetime(row["timestamp_api"], utc=True).tzinfo is UTC


def test_existing_record_is_not_overwritten(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "no_overwrite.db"
    original = make_valid_dataframe()
    load_air_quality(original, database_path=database_path)
    changed = original.copy(deep=True)
    changed.loc[0, "aqius"] = 999
    changed.loc[0, "city"] = "Ciudad Modificada"

    result = load_air_quality(changed, database_path=database_path)
    row = read_rows(database_path)[0]

    assert result.rows_inserted == 0
    assert result.rows_duplicated == 1
    assert row["city"] == original.loc[0, "city"]
    assert row["aqius"] == original.loc[0, "aqius"]


def test_failed_batch_rolls_back_without_partial_inserts(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "rollback.db"
    load_air_quality(
        pd.DataFrame(columns=SCHEMA_COLUMNS),
        database_path=database_path,
    )
    engine = create_engine(sqlite_url(database_path))
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "CREATE TRIGGER reject_test_city "
                    "BEFORE INSERT ON calidad_aire "
                    "WHEN NEW.city = 'Falla' "
                    "BEGIN "
                    "SELECT RAISE(ABORT, 'fallo controlado'); "
                    "END"
                )
            )
    finally:
        engine.dispose()

    batch = make_valid_dataframe(2)
    batch.loc[1, "city"] = "Falla"

    with pytest.raises(LoadTransactionError) as captured:
        load_air_quality(batch, database_path=database_path)

    result = captured.value.result
    assert result.transaction_status == "rolled_back"
    assert result.rows_received == 2
    assert result.rows_inserted == 0
    assert read_rows(database_path) == []


def test_load_reports_missing_schema_columns(
    tmp_path: Path,
) -> None:
    dataframe = make_valid_dataframe().drop(columns=["aqius"])

    with pytest.raises(LoadSchemaError, match="aqius"):
        load_air_quality(
            dataframe,
            database_path=tmp_path / "missing_column.db",
        )


def test_load_does_not_modify_input_dataframe(
    tmp_path: Path,
) -> None:
    dataframe = make_valid_dataframe()
    original = dataframe.copy(deep=True)

    load_air_quality(
        dataframe,
        database_path=tmp_path / "immutable.db",
    )

    pd.testing.assert_frame_equal(dataframe, original)


def test_export_creates_csv_parquet_and_rejected_files(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "export.db"
    output_directory = tmp_path / "processed"
    valid = make_valid_dataframe(2)
    rejected = make_rejected_dataframe()
    load_air_quality(valid, database_path=database_path)

    result = export_air_quality(
        make_quality_result(
            valid_records=valid,
            rejected_records=rejected,
        ),
        database_path=database_path,
        output_directory=output_directory,
    )

    assert isinstance(result, ExportResult)
    assert result.csv_path == output_directory / "calidad_aire.csv"
    assert result.parquet_path == (
        output_directory / "calidad_aire.parquet"
    )
    assert result.rejected_csv_path == (
        output_directory / "registros_rechazados.csv"
    )
    assert all(
        path.is_file()
        for path in (
            result.csv_path,
            result.parquet_path,
            result.rejected_csv_path,
        )
    )
    assert result.csv_status == "exported"
    assert result.parquet_status == "exported"
    assert result.rejected_csv_status == "exported"
    assert result.valid_rows_exported == 2
    assert result.rejected_rows_exported == 1
    assert result.errors == ()


def test_valid_exports_match_consolidated_sqlite_history(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "history.db"
    output_directory = tmp_path / "processed"
    first_batch = make_valid_dataframe(2)
    latest_batch = make_valid_dataframe(1, start_at=2)
    load_air_quality(first_batch, database_path=database_path)
    load_air_quality(latest_batch, database_path=database_path)

    result = export_air_quality(
        make_quality_result(valid_records=latest_batch),
        database_path=database_path,
        output_directory=output_directory,
    )

    csv_data = pd.read_csv(result.csv_path)
    parquet_data = pd.read_parquet(result.parquet_path)
    sqlite_data = pd.DataFrame(read_rows(database_path))
    sqlite_data = sqlite_data.sort_values(
        ["timestamp_api", "record_id"],
        ignore_index=True,
    )
    pd.testing.assert_frame_equal(
        csv_data,
        parquet_data,
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        csv_data,
        sqlite_data.loc[:, SCHEMA_COLUMNS],
        check_dtype=False,
    )
    assert len(csv_data) == 3
    assert tuple(csv_data.columns) == SCHEMA_COLUMNS
    assert tuple(parquet_data.columns) == SCHEMA_COLUMNS


def test_exports_preserve_nulls_and_utc_timestamps(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "nulls.db"
    output_directory = tmp_path / "processed"
    dataframe = make_valid_dataframe()
    timestamp = datetime(
        2026,
        7,
        25,
        6,
        0,
        1,
        123456,
        tzinfo=timezone(timedelta(hours=-6)),
    )
    dataframe.loc[0, "timestamp_api"] = timestamp
    dataframe.loc[0, "timestamp_extraction"] = (
        timestamp + timedelta(seconds=30)
    )
    optional_columns = (
        "latitude",
        "longitude",
        "main_pollutant",
        "temperature_c",
        "humidity_pct",
        "pressure_hpa",
        "wind_speed_ms",
        "wind_direction_deg",
    )
    for column in optional_columns:
        dataframe[column] = pd.NA
    load_air_quality(dataframe, database_path=database_path)

    result = export_air_quality(
        make_quality_result(valid_records=dataframe),
        database_path=database_path,
        output_directory=output_directory,
    )
    csv_data = pd.read_csv(result.csv_path)
    parquet_data = pd.read_parquet(result.parquet_path)

    assert csv_data.loc[0, "timestamp_api"] == (
        "2026-07-25T12:00:01.123456Z"
    )
    assert parquet_data.loc[0, "timestamp_extraction"] == (
        "2026-07-25T12:00:31.123456Z"
    )
    assert all(
        pd.isna(csv_data.loc[0, column])
        and pd.isna(parquet_data.loc[0, column])
        for column in optional_columns
    )


def test_rejected_export_preserves_reasons_and_available_values(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "rejected.db"
    output_directory = tmp_path / "processed"
    valid = make_valid_dataframe()
    rejected = make_rejected_dataframe()
    load_air_quality(valid, database_path=database_path)

    result = export_air_quality(
        make_quality_result(
            valid_records=valid,
            rejected_records=rejected,
        ),
        database_path=database_path,
        output_directory=output_directory,
    )
    exported = pd.read_csv(result.rejected_csv_path)

    assert tuple(exported.columns) == REJECTED_COLUMNS
    assert len(exported) == 1
    assert exported.loc[0, "humidity_pct"] == "valor inválido"
    assert exported.loc[0, "rejection_reason"] == (
        "humidity_pct: debe ser numérico cuando está presente"
    )


def test_empty_rejected_export_contains_headers(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "empty_rejected.db"
    output_directory = tmp_path / "processed"
    valid = make_valid_dataframe()
    load_air_quality(valid, database_path=database_path)

    result = export_air_quality(
        make_quality_result(valid_records=valid),
        database_path=database_path,
        output_directory=output_directory,
    )
    exported = pd.read_csv(result.rejected_csv_path)

    assert tuple(exported.columns) == REJECTED_COLUMNS
    assert exported.empty
    assert result.rejected_rows_exported == 0
    assert result.rejected_csv_status == "exported"


def test_second_export_replaces_outputs_with_current_state(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "replace.db"
    output_directory = tmp_path / "processed"
    first_batch = make_valid_dataframe()
    load_air_quality(first_batch, database_path=database_path)
    first_result = export_air_quality(
        make_quality_result(
            valid_records=first_batch,
            rejected_records=make_rejected_dataframe(),
        ),
        database_path=database_path,
        output_directory=output_directory,
    )
    assert len(pd.read_csv(first_result.csv_path)) == 1
    assert len(pd.read_csv(first_result.rejected_csv_path)) == 1

    second_batch = make_valid_dataframe(1, start_at=1)
    load_air_quality(second_batch, database_path=database_path)
    second_result = export_air_quality(
        make_quality_result(valid_records=second_batch),
        database_path=database_path,
        output_directory=output_directory,
    )

    assert len(pd.read_csv(second_result.csv_path)) == 2
    assert len(pd.read_parquet(second_result.parquet_path)) == 2
    assert pd.read_csv(second_result.rejected_csv_path).empty


def test_atomic_export_writes_temporary_file_before_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "atomic.db"
    output_directory = tmp_path / "processed"
    output_directory.mkdir()
    csv_path = output_directory / "calidad_aire.csv"
    csv_path.write_text("contenido anterior", encoding="utf-8")
    valid = make_valid_dataframe()
    load_air_quality(valid, database_path=database_path)
    original_writer = load_module._write_csv_file
    observed_temporary_paths: list[Path] = []

    def observing_writer(
        dataframe: pd.DataFrame,
        destination: Path,
    ) -> None:
        observed_temporary_paths.append(destination)
        assert destination != csv_path
        assert destination.parent == csv_path.parent
        assert destination.name.startswith(f".{csv_path.name}.")
        assert csv_path.read_text(encoding="utf-8") == "contenido anterior"
        original_writer(dataframe, destination)
        assert csv_path.read_text(encoding="utf-8") == "contenido anterior"

    monkeypatch.setattr(
        load_module,
        "_write_csv_file",
        observing_writer,
    )

    result = export_air_quality(
        make_quality_result(valid_records=valid),
        database_path=database_path,
        output_directory=output_directory,
    )

    assert result.csv_status == "exported"
    assert observed_temporary_paths
    assert csv_path.read_text(encoding="utf-8") != "contenido anterior"
    assert all(not path.exists() for path in observed_temporary_paths)


@pytest.mark.parametrize(
    ("writer_name", "status_field", "path_name", "target"),
    [
        (
            "_write_csv_file",
            "csv_status",
            "calidad_aire.csv",
            "csv",
        ),
        (
            "_write_parquet_file",
            "parquet_status",
            "calidad_aire.parquet",
            "parquet",
        ),
        (
            "_write_rejected_csv_file",
            "rejected_csv_status",
            "registros_rechazados.csv",
            "rejected_csv",
        ),
    ],
)
def test_export_failure_keeps_previous_file_and_cleans_temporary(
    writer_name: str,
    status_field: str,
    path_name: str,
    target: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / f"failure_{target}.db"
    output_directory = tmp_path / "processed"
    output_directory.mkdir()
    destination = output_directory / path_name
    previous_content = b"archivo anterior intacto"
    destination.write_bytes(previous_content)
    valid = make_valid_dataframe()
    load_air_quality(valid, database_path=database_path)
    sqlite_before = read_rows(database_path)

    def failing_writer(
        dataframe: pd.DataFrame,
        temporary_path: Path,
    ) -> None:
        temporary_path.write_text(
            "contenido parcial",
            encoding="utf-8",
        )
        raise OSError(f"fallo sensible en {temporary_path}")

    monkeypatch.setattr(load_module, writer_name, failing_writer)

    result = export_air_quality(
        make_quality_result(
            valid_records=valid,
            rejected_records=make_rejected_dataframe(),
        ),
        database_path=database_path,
        output_directory=output_directory,
    )

    assert getattr(result, status_field) == "failed"
    assert destination.read_bytes() == previous_content
    assert not list(output_directory.glob(f".{path_name}.*.tmp"))
    assert read_rows(database_path) == sqlite_before
    issue = next(issue for issue in result.errors if issue.target == target)
    assert issue.error_type == "OSError"
    assert str(output_directory) not in issue.message
    assert "fallo sensible" not in issue.message


def test_export_does_not_modify_quality_dataframes(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "immutable_export.db"
    output_directory = tmp_path / "processed"
    valid = make_valid_dataframe()
    rejected = make_rejected_dataframe()
    source = make_quality_result(
        valid_records=valid,
        rejected_records=rejected,
    )
    valid_before = valid.copy(deep=True)
    rejected_before = rejected.copy(deep=True)
    load_air_quality(valid, database_path=database_path)

    export_air_quality(
        source,
        database_path=database_path,
        output_directory=output_directory,
    )

    pd.testing.assert_frame_equal(valid, valid_before)
    pd.testing.assert_frame_equal(rejected, rejected_before)


def test_export_reports_missing_rejected_schema(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "invalid_rejected.db"
    valid = make_valid_dataframe()
    load_air_quality(valid, database_path=database_path)
    source = make_quality_result(
        valid_records=valid,
        rejected_records=pd.DataFrame(columns=SCHEMA_COLUMNS),
    )

    with pytest.raises(ExportSchemaError, match="rejection_reason"):
        export_air_quality(
            source,
            database_path=database_path,
            output_directory=tmp_path / "processed",
        )


def test_export_requires_existing_sqlite_database(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "missing.db"

    with pytest.raises(ExportReadError, match="No existe"):
        export_air_quality(
            make_quality_result(),
            database_path=database_path,
            output_directory=tmp_path / "processed",
        )

    assert not database_path.exists()
