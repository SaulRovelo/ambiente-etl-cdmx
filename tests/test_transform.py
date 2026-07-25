"""Pruebas unitarias de la transformación tabular."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from etl.extract import ExtractionMetadata, ExtractionResult
from etl.transform import (
    SCHEMA_COLUMNS,
    InvalidTransformTimestampError,
    MissingTransformStructureError,
    TransformationError,
    TransformResult,
    transform_air_quality,
)


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "iqair_success.json"
EXTRACTED_AT = datetime(2026, 7, 24, 18, 30, 15, tzinfo=UTC)


@pytest.fixture
def sample_payload() -> dict[str, Any]:
    """Carga una respuesta representativa sin credenciales."""

    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


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
