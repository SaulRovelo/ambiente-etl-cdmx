"""Pruebas unitarias de persistencia SQLite."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from sqlalchemy import URL, create_engine, inspect, select, text

from etl.load import (
    AIR_QUALITY_TABLE,
    TABLE_NAME,
    InvalidRecordIdError,
    LoadResult,
    LoadSchemaError,
    LoadTransactionError,
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
