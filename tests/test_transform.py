"""Pruebas unitarias de la transformación tabular."""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import fields
from datetime import UTC, datetime
from inspect import signature
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from etl.extract import ExtractionMetadata, ExtractionResult
from etl.transform import (
    REJECTED_COLUMNS,
    SCHEMA_COLUMNS,
    InvalidTransformTimestampError,
    MissingQualityColumnsError,
    MissingTransformStructureError,
    QualityResult,
    TransformationError,
    TransformResult,
    transform_air_quality,
    validate_air_quality,
)
from etl.utils import generate_record_id


EXTRACTED_AT = datetime(2026, 7, 24, 18, 30, 15, tzinfo=UTC)


@pytest.fixture
def normalized_dataframe(
    sample_payload: dict[str, Any],
) -> pd.DataFrame:
    """Genera el DataFrame válido de la Etapa 4."""

    return transform_air_quality(
        make_extraction_result(sample_payload)
    ).dataframe


def make_extraction_result(
    payload: dict[str, Any],
    *,
    extracted_at: datetime = EXTRACTED_AT,
) -> ExtractionResult:
    """Construye el contrato de Extract sin realizar I/O ni red."""

    data = payload.get("data", {})
    current = data.get("current", {}) if isinstance(data, dict) else {}
    pollution = (
        current.get("pollution", {})
        if isinstance(current, dict)
        else {}
    )
    weather = (
        current.get("weather", {})
        if isinstance(current, dict)
        else {}
    )
    return ExtractionResult(
        payload=payload,
        raw_path=Path("data/raw/fixture.json"),
        extracted_at=extracted_at,
        endpoint="https://api.airvisual.com/v2/city",
        status_code=200,
        metadata=ExtractionMetadata(
            provider_status="success",
            city=str(data.get("city", "")),
            state=str(data.get("state", "")),
            country=str(data.get("country", "")),
            content_type="application/json",
            response_size_bytes=len(json.dumps(payload)),
            data_fields=tuple(sorted(data)),
            pollution_fields=tuple(sorted(pollution)),
            weather_fields=tuple(sorted(weather)),
        ),
    )


def dataframe_with_value(
    dataframe: pd.DataFrame,
    column: str,
    value: Any,
) -> pd.DataFrame:
    """Copia un DataFrame y permite inyectar un escalar incluso si es inválido."""

    changed = dataframe.copy(deep=True)
    changed[column] = changed[column].astype("object")
    changed.at[changed.index[0], column] = value
    return changed


def test_public_transformation_contracts_are_stable() -> None:
    assert tuple(signature(transform_air_quality).parameters) == (
        "source",
        "extracted_at",
    )
    assert tuple(signature(validate_air_quality).parameters) == ("source",)
    assert tuple(field.name for field in fields(TransformResult)) == (
        "dataframe",
        "warnings",
        "records_transformed",
        "schema",
    )
    assert tuple(field.name for field in fields(QualityResult)) == (
        "valid_records",
        "rejected_records",
        "warnings",
        "total_received",
        "total_valid",
        "total_rejected",
    )


def test_fixture_transforms_to_one_typed_record(
    sample_payload: dict[str, Any],
) -> None:
    result = transform_air_quality(make_extraction_result(sample_payload))

    assert isinstance(result, TransformResult)
    assert result.records_transformed == 1
    assert len(result.dataframe) == 1
    assert result.warnings == ()


def test_columns_have_exact_documented_order(
    sample_payload: dict[str, Any],
) -> None:
    result = transform_air_quality(make_extraction_result(sample_payload))

    assert tuple(result.dataframe.columns) == SCHEMA_COLUMNS
    assert result.schema.columns == SCHEMA_COLUMNS


def test_location_maps_longitude_before_latitude(
    sample_payload: dict[str, Any],
) -> None:
    row = transform_air_quality(
        make_extraction_result(sample_payload)
    ).dataframe.iloc[0]

    assert row["longitude"] == pytest.approx(-99.1332)
    assert row["latitude"] == pytest.approx(19.4326)


def test_pollution_and_weather_fields_are_mapped(
    sample_payload: dict[str, Any],
) -> None:
    row = transform_air_quality(
        make_extraction_result(sample_payload)
    ).dataframe.iloc[0]

    assert row["aqius"] == 42
    assert row["main_pollutant"] == "p2"
    assert row["temperature_c"] == 24
    assert row["humidity_pct"] == 46
    assert row["pressure_hpa"] == 1016
    assert row["wind_speed_ms"] == pytest.approx(2.1)
    assert row["wind_direction_deg"] == 180


def test_timestamps_are_normalized_to_utc(
    sample_payload: dict[str, Any],
) -> None:
    extraction_timestamp = datetime.fromisoformat(
        "2026-07-24T12:30:15-06:00"
    )
    result = transform_air_quality(
        make_extraction_result(
            sample_payload,
            extracted_at=extraction_timestamp,
        )
    )
    row = result.dataframe.iloc[0]

    assert row["timestamp_api"] == pd.Timestamp(
        "2026-07-24T18:00:00Z"
    )
    assert row["timestamp_extraction"] == pd.Timestamp(
        "2026-07-24T18:30:15Z"
    )
    assert str(result.dataframe["timestamp_api"].dtype) == (
        "datetime64[ns, UTC]"
    )
    assert str(result.dataframe["timestamp_extraction"].dtype) == (
        "datetime64[ns, UTC]"
    )


def test_pollution_timestamp_is_used_when_weather_timestamp_differs(
    sample_payload: dict[str, Any],
) -> None:
    payload = deepcopy(sample_payload)
    payload["data"]["current"]["weather"]["ts"] = (
        "2026-07-24T19:00:00.000Z"
    )

    result = transform_air_quality(make_extraction_result(payload))

    assert result.dataframe.loc[0, "timestamp_api"] == pd.Timestamp(
        "2026-07-24T18:00:00Z"
    )
    assert [warning.code for warning in result.warnings] == [
        "weather_timestamp_mismatch"
    ]


def test_invalid_optional_weather_timestamp_generates_warning(
    sample_payload: dict[str, Any],
) -> None:
    payload = deepcopy(sample_payload)
    payload["data"]["current"]["weather"]["ts"] = "not-a-timestamp"

    result = transform_air_quality(make_extraction_result(payload))

    assert [warning.code for warning in result.warnings] == [
        "weather_timestamp_invalid"
    ]


def test_missing_optional_fields_become_null(
    sample_payload: dict[str, Any],
) -> None:
    payload = deepcopy(sample_payload)
    payload["data"].pop("location")
    pollution = payload["data"]["current"]["pollution"]
    pollution.pop("mainus")
    weather = payload["data"]["current"]["weather"]
    for field_name in ("tp", "hu", "pr", "ws", "wd"):
        weather.pop(field_name)

    result = transform_air_quality(make_extraction_result(payload))
    row = result.dataframe.iloc[0]

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
    assert all(pd.isna(row[column]) for column in optional_columns)
    warning = result.warnings[0]
    assert warning.code == "optional_fields_missing"
    assert set(warning.fields) == set(optional_columns)


@pytest.mark.parametrize(
    "missing_path",
    [
        ("data",),
        ("data", "city"),
        ("data", "current"),
        ("data", "current", "pollution"),
        ("data", "current", "pollution", "ts"),
        ("data", "current", "pollution", "aqius"),
    ],
)
def test_missing_required_structure_raises_clear_error(
    missing_path: tuple[str, ...],
    sample_payload: dict[str, Any],
) -> None:
    payload = deepcopy(sample_payload)
    target: dict[str, Any] = payload
    for key in missing_path[:-1]:
        target = target[key]
    target.pop(missing_path[-1])

    with pytest.raises(MissingTransformStructureError) as captured:
        transform_air_quality(
            payload,
            extracted_at=EXTRACTED_AT,
        )

    assert ".".join(missing_path) in str(captured.value)


def test_required_timestamps_must_include_timezone(
    sample_payload: dict[str, Any],
) -> None:
    payload = deepcopy(sample_payload)
    payload["data"]["current"]["pollution"]["ts"] = "2026-07-24T18:00:00"

    with pytest.raises(
        InvalidTransformTimestampError,
        match="data.current.pollution.ts",
    ):
        transform_air_quality(
            payload,
            extracted_at=EXTRACTED_AT,
        )


def test_dataframe_uses_stable_nullable_dtypes(
    sample_payload: dict[str, Any],
) -> None:
    result = transform_air_quality(make_extraction_result(sample_payload))

    assert dict(result.schema.dtypes) == {
        "record_id": "string",
        "city": "string",
        "state": "string",
        "country": "string",
        "latitude": "Float64",
        "longitude": "Float64",
        "timestamp_api": "datetime64[ns, UTC]",
        "timestamp_extraction": "datetime64[ns, UTC]",
        "aqius": "Int64",
        "main_pollutant": "string",
        "temperature_c": "Float64",
        "humidity_pct": "Float64",
        "pressure_hpa": "Float64",
        "wind_speed_ms": "Float64",
        "wind_direction_deg": "Float64",
    }


def test_payload_is_not_modified(
    sample_payload: dict[str, Any],
) -> None:
    original = deepcopy(sample_payload)

    transform_air_quality(make_extraction_result(sample_payload))

    assert sample_payload == original


def test_unselected_real_api_fields_are_excluded(
    sample_payload: dict[str, Any],
) -> None:
    payload = deepcopy(sample_payload)
    pollution = payload["data"]["current"]["pollution"]
    weather = payload["data"]["current"]["weather"]
    pollution.update({"aqicn": 18, "maincn": "p2"})
    weather.update({"ic": "01d", "heatIndex": 25})

    result = transform_air_quality(make_extraction_result(payload))

    assert all(
        field_name not in result.dataframe.columns
        for field_name in ("aqicn", "maincn", "ic", "heatIndex")
    )
    assert result.schema.excluded_source_fields == (
        "aqicn",
        "maincn",
        "ic",
        "heatIndex",
    )


def test_record_id_remains_pending(
    sample_payload: dict[str, Any],
) -> None:
    result = transform_air_quality(make_extraction_result(sample_payload))

    assert pd.isna(result.dataframe.loc[0, "record_id"])
    assert str(result.dataframe["record_id"].dtype) == "string"


def test_payload_equivalent_requires_extraction_timestamp(
    sample_payload: dict[str, Any],
) -> None:
    with pytest.raises(TransformationError, match="extracted_at"):
        transform_air_quality(sample_payload)


def test_quality_accepts_valid_record_and_generates_record_id(
    sample_payload: dict[str, Any],
) -> None:
    transformed = transform_air_quality(
        make_extraction_result(sample_payload)
    )

    result = validate_air_quality(transformed)

    assert isinstance(result, QualityResult)
    assert result.total_received == 1
    assert result.total_valid == 1
    assert result.total_rejected == 0
    assert result.rejected_records.empty
    row = result.valid_records.iloc[0]
    assert row["record_id"] == generate_record_id(
        row["city"],
        row["state"],
        row["country"],
        row["timestamp_api"].to_pydatetime(),
    )


@pytest.mark.parametrize(
    ("column", "invalid_value"),
    [
        ("city", ""),
        ("state", "   "),
        ("country", pd.NA),
    ],
)
def test_quality_rejects_missing_required_location_text(
    column: str,
    invalid_value: Any,
    normalized_dataframe: pd.DataFrame,
) -> None:
    dataframe = dataframe_with_value(
        normalized_dataframe,
        column,
        invalid_value,
    )

    result = validate_air_quality(dataframe)

    assert result.total_rejected == 1
    assert f"{column}: debe contener texto" in (
        result.rejected_records.loc[0, "rejection_reason"]
    )
    assert pd.isna(result.rejected_records.loc[0, "record_id"])


@pytest.mark.parametrize(
    ("column", "invalid_value"),
    [
        ("timestamp_api", "no-es-una-fecha"),
        ("timestamp_api", datetime(2026, 7, 25, 1, 0)),
        ("timestamp_extraction", "no-es-una-fecha"),
        ("timestamp_extraction", datetime(2026, 7, 25, 1, 0)),
    ],
)
def test_quality_rejects_invalid_required_timestamp(
    column: str,
    invalid_value: Any,
    normalized_dataframe: pd.DataFrame,
) -> None:
    dataframe = dataframe_with_value(
        normalized_dataframe,
        column,
        invalid_value,
    )

    result = validate_air_quality(dataframe)

    assert result.total_rejected == 1
    assert f"{column}: debe ser una fecha válida" in (
        result.rejected_records.loc[0, "rejection_reason"]
    )


@pytest.mark.parametrize(
    ("invalid_value", "expected_reason"),
    [
        (pd.NA, "debe existir y ser un entero válido"),
        ("no-numérico", "debe existir y ser un entero válido"),
        (42.5, "debe existir y ser un entero válido"),
        (-1, "debe ser mayor o igual que 0"),
    ],
)
def test_quality_rejects_invalid_aqius(
    invalid_value: Any,
    expected_reason: str,
    normalized_dataframe: pd.DataFrame,
) -> None:
    dataframe = dataframe_with_value(
        normalized_dataframe,
        "aqius",
        invalid_value,
    )

    result = validate_air_quality(dataframe)

    assert result.total_rejected == 1
    assert expected_reason in (
        result.rejected_records.loc[0, "rejection_reason"]
    )


@pytest.mark.parametrize(
    ("column", "invalid_value", "expected_reason"),
    [
        ("latitude", -90.1, "latitude: debe ser >= -90"),
        ("latitude", 90.1, "latitude: debe ser <= 90"),
        ("longitude", -180.1, "longitude: debe ser >= -180"),
        ("longitude", 180.1, "longitude: debe ser <= 180"),
        ("humidity_pct", -0.1, "humidity_pct: debe ser >= 0"),
        ("humidity_pct", 100.1, "humidity_pct: debe ser <= 100"),
        ("pressure_hpa", 0, "pressure_hpa: debe ser > 0"),
        ("wind_speed_ms", -0.1, "wind_speed_ms: debe ser >= 0"),
        (
            "wind_direction_deg",
            -0.1,
            "wind_direction_deg: debe ser >= 0",
        ),
        (
            "wind_direction_deg",
            360,
            "wind_direction_deg: debe ser < 360",
        ),
    ],
)
def test_quality_rejects_values_outside_confirmed_ranges(
    column: str,
    invalid_value: float,
    expected_reason: str,
    normalized_dataframe: pd.DataFrame,
) -> None:
    dataframe = dataframe_with_value(
        normalized_dataframe,
        column,
        invalid_value,
    )

    result = validate_air_quality(dataframe)

    assert result.total_rejected == 1
    assert expected_reason in (
        result.rejected_records.loc[0, "rejection_reason"]
    )


def test_quality_accepts_confirmed_range_boundaries(
    normalized_dataframe: pd.DataFrame,
) -> None:
    rows = []
    boundary_values = (
        ("latitude", -90),
        ("latitude", 90),
        ("longitude", -180),
        ("longitude", 180),
        ("humidity_pct", 0),
        ("humidity_pct", 100),
        ("pressure_hpa", 0.1),
        ("wind_speed_ms", 0),
        ("wind_direction_deg", 0),
        ("wind_direction_deg", 359.999),
    )
    for column, value in boundary_values:
        row = dataframe_with_value(normalized_dataframe, column, value)
        row["timestamp_api"] = row["timestamp_api"] + pd.Timedelta(
            seconds=len(rows)
        )
        rows.append(row)
    dataframe = pd.concat(rows, ignore_index=True)

    result = validate_air_quality(dataframe)

    assert result.total_valid == len(boundary_values)
    assert result.total_rejected == 0


def test_quality_accepts_null_optional_fields(
    normalized_dataframe: pd.DataFrame,
) -> None:
    dataframe = normalized_dataframe.copy(deep=True)
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

    result = validate_air_quality(dataframe)

    assert result.total_valid == 1
    assert result.total_rejected == 0
    assert all(
        pd.isna(result.valid_records.loc[0, column])
        for column in optional_columns
    )


@pytest.mark.parametrize(
    ("column", "invalid_value"),
    [
        ("latitude", "norte"),
        ("longitude", "oeste"),
        ("temperature_c", "cálido"),
        ("humidity_pct", "húmedo"),
        ("pressure_hpa", "alta"),
        ("wind_speed_ms", "rápido"),
        ("wind_direction_deg", "sur"),
        ("main_pollutant", 123),
    ],
)
def test_quality_rejects_invalid_present_optional_fields(
    column: str,
    invalid_value: Any,
    normalized_dataframe: pd.DataFrame,
) -> None:
    dataframe = dataframe_with_value(
        normalized_dataframe,
        column,
        invalid_value,
    )

    result = validate_air_quality(dataframe)

    assert result.total_rejected == 1
    assert column in result.rejected_records.loc[0, "rejection_reason"]
    assert result.rejected_records.loc[0, column] == invalid_value


def test_quality_preserves_multiple_reasons_in_stable_order(
    normalized_dataframe: pd.DataFrame,
) -> None:
    dataframe = normalized_dataframe.copy(deep=True).astype("object")
    dataframe.loc[0, "city"] = ""
    dataframe.loc[0, "aqius"] = -1
    dataframe.loc[0, "humidity_pct"] = 101
    dataframe.loc[0, "pressure_hpa"] = 0
    dataframe.loc[0, "wind_speed_ms"] = -1
    dataframe.loc[0, "wind_direction_deg"] = 360

    result = validate_air_quality(dataframe)

    assert result.rejected_records.loc[0, "rejection_reason"] == (
        "city: debe contener texto; "
        "aqius: debe ser mayor o igual que 0; "
        "humidity_pct: debe ser <= 100; "
        "pressure_hpa: debe ser > 0; "
        "wind_speed_ms: debe ser >= 0; "
        "wind_direction_deg: debe ser < 360"
    )


def test_quality_separates_batches_and_reports_counts(
    normalized_dataframe: pd.DataFrame,
) -> None:
    valid = normalized_dataframe.copy(deep=True)
    empty_city = dataframe_with_value(valid, "city", "")
    invalid_humidity = dataframe_with_value(
        valid,
        "humidity_pct",
        101,
    )
    dataframe = pd.concat(
        (valid, empty_city, invalid_humidity),
        ignore_index=True,
    )

    result = validate_air_quality(dataframe)

    assert result.total_received == 3
    assert result.total_valid == 1
    assert result.total_rejected == 2
    assert len(result.valid_records) == 1
    assert len(result.rejected_records) == 2


def test_quality_outputs_have_stable_columns_and_dtypes(
    normalized_dataframe: pd.DataFrame,
) -> None:
    invalid = dataframe_with_value(
        normalized_dataframe,
        "temperature_c",
        "desconocida",
    )
    dataframe = pd.concat(
        (normalized_dataframe, invalid),
        ignore_index=True,
    )

    result = validate_air_quality(dataframe)

    assert tuple(result.valid_records.columns) == SCHEMA_COLUMNS
    assert tuple(result.rejected_records.columns) == REJECTED_COLUMNS
    assert {
        column: str(result.valid_records[column].dtype)
        for column in SCHEMA_COLUMNS
    } == {
        "record_id": "string",
        "city": "string",
        "state": "string",
        "country": "string",
        "latitude": "Float64",
        "longitude": "Float64",
        "timestamp_api": "datetime64[ns, UTC]",
        "timestamp_extraction": "datetime64[ns, UTC]",
        "aqius": "Int64",
        "main_pollutant": "string",
        "temperature_c": "Float64",
        "humidity_pct": "Float64",
        "pressure_hpa": "Float64",
        "wind_speed_ms": "Float64",
        "wind_direction_deg": "Float64",
    }
    assert str(result.rejected_records["record_id"].dtype) == "string"
    assert all(
        str(result.rejected_records[column].dtype) == "object"
        for column in SCHEMA_COLUMNS
        if column != "record_id"
    )
    assert (
        str(result.rejected_records["rejection_reason"].dtype)
        == "string"
    )


def test_quality_empty_input_preserves_output_schemas() -> None:
    dataframe = pd.DataFrame(columns=SCHEMA_COLUMNS)

    result = validate_air_quality(dataframe)

    assert result.total_received == 0
    assert tuple(result.valid_records.columns) == SCHEMA_COLUMNS
    assert tuple(result.rejected_records.columns) == REJECTED_COLUMNS
    assert str(result.valid_records["timestamp_api"].dtype) == (
        "datetime64[ns, UTC]"
    )
    assert str(result.valid_records["timestamp_extraction"].dtype) == (
        "datetime64[ns, UTC]"
    )


def test_quality_does_not_modify_source_dataframe(
    normalized_dataframe: pd.DataFrame,
) -> None:
    dataframe = dataframe_with_value(
        normalized_dataframe,
        "humidity_pct",
        101,
    )
    original = dataframe.copy(deep=True)

    validate_air_quality(dataframe)

    pd.testing.assert_frame_equal(dataframe, original)


def test_quality_propagates_transform_warnings(
    sample_payload: dict[str, Any],
) -> None:
    payload = deepcopy(sample_payload)
    payload["data"]["current"]["weather"]["ts"] = (
        "2026-07-24T19:00:00.000Z"
    )
    transformed = transform_air_quality(make_extraction_result(payload))

    result = validate_air_quality(transformed)

    assert result.warnings == transformed.warnings
    assert result.warnings[0].code == "weather_timestamp_mismatch"


def test_transform_preserves_invalid_values_until_rejection(
    sample_payload: dict[str, Any],
) -> None:
    payload = deepcopy(sample_payload)
    payload["data"]["current"]["weather"]["tp"] = "desconocida"
    payload["data"]["current"]["pollution"]["aqius"] = "sin-dato"

    transformed = transform_air_quality(make_extraction_result(payload))
    result = validate_air_quality(transformed)

    assert transformed.dataframe.loc[0, "temperature_c"] == "desconocida"
    assert transformed.dataframe.loc[0, "aqius"] == "sin-dato"
    assert result.total_rejected == 1
    assert result.rejected_records.loc[0, "temperature_c"] == "desconocida"
    assert result.rejected_records.loc[0, "aqius"] == "sin-dato"


def test_quality_requires_confirmed_schema_columns(
    normalized_dataframe: pd.DataFrame,
) -> None:
    dataframe = normalized_dataframe.drop(columns=["aqius"])

    with pytest.raises(MissingQualityColumnsError, match="aqius"):
        validate_air_quality(dataframe)
