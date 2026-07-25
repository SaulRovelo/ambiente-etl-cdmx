"""Transformación tabular del contrato confirmado de IQAir."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, overload

import pandas as pd

from etl.extract import ExtractionResult


SCHEMA_COLUMNS: tuple[str, ...] = (
    "record_id",
    "city",
    "state",
    "country",
    "latitude",
    "longitude",
    "timestamp_api",
    "timestamp_extraction",
    "aqius",
    "main_pollutant",
    "temperature_c",
    "humidity_pct",
    "pressure_hpa",
    "wind_speed_ms",
    "wind_direction_deg",
)

_OPTIONAL_WEATHER_FIELDS: dict[str, str] = {
    "temperature_c": "tp",
    "humidity_pct": "hu",
    "pressure_hpa": "pr",
    "wind_speed_ms": "ws",
    "wind_direction_deg": "wd",
}
_EXCLUDED_SOURCE_FIELDS: tuple[str, ...] = (
    "aqicn",
    "maincn",
    "ic",
    "heatIndex",
)


class TransformationError(ValueError):
    """Error base de la transformación tabular."""


class MissingTransformStructureError(TransformationError):
    """El payload no contiene una estructura necesaria."""

    def __init__(self, missing_fields: tuple[str, ...]) -> None:
        self.missing_fields = missing_fields
        fields = ", ".join(missing_fields)
        super().__init__(
            f"El payload no contiene la estructura requerida: {fields}"
        )


class InvalidTransformTimestampError(TransformationError):
    """Un timestamp obligatorio no puede normalizarse a UTC."""

    def __init__(self, field_name: str) -> None:
        self.field_name = field_name
        super().__init__(
            f"El campo {field_name} debe ser un timestamp válido con zona horaria"
        )


@dataclass(frozen=True, slots=True)
class TransformWarning:
    """Advertencia no bloqueante generada durante la transformación."""

    code: str
    message: str
    fields: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class TransformSchemaMetadata:
    """Descripción no sensible del DataFrame producido."""

    columns: tuple[str, ...]
    dtypes: tuple[tuple[str, str], ...]
    excluded_source_fields: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TransformResult:
    """Resultado tipado de una transformación de una respuesta de IQAir."""

    dataframe: pd.DataFrame = field(repr=False)
    warnings: tuple[TransformWarning, ...]
    records_transformed: int
    schema: TransformSchemaMetadata


@overload
def transform_air_quality(
    source: ExtractionResult,
    *,
    extracted_at: None = None,
) -> TransformResult: ...


@overload
def transform_air_quality(
    source: Mapping[str, Any],
    *,
    extracted_at: datetime,
) -> TransformResult: ...


def transform_air_quality(
    source: ExtractionResult | Mapping[str, Any],
    *,
    extracted_at: datetime | None = None,
) -> TransformResult:
    """Convierte una respuesta confirmada de IQAir en una fila normalizada."""

    payload, extraction_timestamp = _resolve_source(source, extracted_at)
    data = _required_mapping(payload, "data", "data")
    current = _required_mapping(data, "current", "data.current")
    pollution = _required_mapping(
        current,
        "pollution",
        "data.current.pollution",
    )

    missing = [
        field_path
        for key, field_path in (
            ("ts", "data.current.pollution.ts"),
            ("aqius", "data.current.pollution.aqius"),
        )
        if key not in pollution
    ]
    if missing:
        raise MissingTransformStructureError(tuple(missing))

    city = _required_text(data, "city", "data.city")
    state = _required_text(data, "state", "data.state")
    country = _required_text(data, "country", "data.country")
    timestamp_api = _parse_required_timestamp(
        pollution["ts"],
        "data.current.pollution.ts",
    )
    timestamp_extraction = _parse_required_timestamp(
        extraction_timestamp,
        "timestamp_extraction",
    )

    warnings: list[TransformWarning] = []
    optional_missing: list[str] = []
    longitude, latitude = _coordinates(data, optional_missing)
    main_pollutant = _optional_value(
        pollution,
        "mainus",
        "main_pollutant",
        optional_missing,
    )

    weather_value = current.get("weather")
    weather = weather_value if isinstance(weather_value, Mapping) else {}
    weather_values = {
        column: _optional_value(
            weather,
            source_field,
            column,
            optional_missing,
        )
        for column, source_field in _OPTIONAL_WEATHER_FIELDS.items()
    }

    if optional_missing:
        missing_fields = tuple(dict.fromkeys(optional_missing))
        warnings.append(
            TransformWarning(
                code="optional_fields_missing",
                message=(
                    "Uno o más campos opcionales no están disponibles "
                    "y se representaron como nulos"
                ),
                fields=missing_fields,
            )
        )

    weather_timestamp = weather.get("ts")
    if weather_timestamp is not None:
        try:
            normalized_weather_timestamp = _parse_required_timestamp(
                weather_timestamp,
                "data.current.weather.ts",
            )
        except InvalidTransformTimestampError:
            warnings.append(
                TransformWarning(
                    code="weather_timestamp_invalid",
                    message=(
                        "El timestamp meteorológico no pudo interpretarse; "
                        "se conservó pollution.ts como timestamp_api"
                    ),
                    fields=("data.current.weather.ts",),
                )
            )
        else:
            if normalized_weather_timestamp != timestamp_api:
                warnings.append(
                    TransformWarning(
                        code="weather_timestamp_mismatch",
                        message=(
                            "weather.ts difiere de pollution.ts; "
                            "se utilizó pollution.ts como timestamp_api"
                        ),
                        fields=(
                            "data.current.pollution.ts",
                            "data.current.weather.ts",
                        ),
                    )
                )

    dataframe = _build_dataframe(
        city=city,
        state=state,
        country=country,
        latitude=latitude,
        longitude=longitude,
        timestamp_api=timestamp_api,
        timestamp_extraction=timestamp_extraction,
        aqius=pollution["aqius"],
        main_pollutant=main_pollutant,
        weather_values=weather_values,
    )
    schema = TransformSchemaMetadata(
        columns=tuple(dataframe.columns),
        dtypes=tuple(
            (column, str(dataframe[column].dtype))
            for column in dataframe.columns
        ),
        excluded_source_fields=_EXCLUDED_SOURCE_FIELDS,
    )
    return TransformResult(
        dataframe=dataframe,
        warnings=tuple(warnings),
        records_transformed=len(dataframe),
        schema=schema,
    )


def _resolve_source(
    source: ExtractionResult | Mapping[str, Any],
    extracted_at: datetime | None,
) -> tuple[Mapping[str, Any], datetime]:
    """Obtiene payload y timestamp sin alterar el objeto de entrada."""

    if isinstance(source, ExtractionResult):
        if extracted_at is not None:
            raise TransformationError(
                "No se debe proporcionar extracted_at junto con ExtractionResult"
            )
        return source.payload, source.extracted_at

    if not isinstance(source, Mapping):
        raise TransformationError(
            "La transformación requiere ExtractionResult o un payload JSON"
        )
    if extracted_at is None:
        raise TransformationError(
            "extracted_at es obligatorio cuando se proporciona un payload"
        )
    return source, extracted_at


def _required_mapping(
    container: Mapping[str, Any],
    key: str,
    field_path: str,
) -> Mapping[str, Any]:
    """Obtiene un objeto estructural obligatorio."""

    value = container.get(key)
    if not isinstance(value, Mapping):
        raise MissingTransformStructureError((field_path,))
    return value


def _required_text(
    container: Mapping[str, Any],
    key: str,
    field_path: str,
) -> str:
    """Obtiene texto estructural obligatorio sin modificar el payload."""

    value = container.get(key)
    if not isinstance(value, str) or not value.strip():
        raise MissingTransformStructureError((field_path,))
    return value.strip()


def _coordinates(
    data: Mapping[str, Any],
    optional_missing: list[str],
) -> tuple[Any, Any]:
    """Mapea las coordenadas de IQAir en orden longitud, latitud."""

    location = data.get("location")
    coordinates = (
        location.get("coordinates")
        if isinstance(location, Mapping)
        else None
    )
    if (
        not isinstance(coordinates, (list, tuple))
        or len(coordinates) < 2
    ):
        optional_missing.extend(("longitude", "latitude"))
        return None, None

    longitude, latitude = coordinates[:2]
    if longitude is None:
        optional_missing.append("longitude")
    if latitude is None:
        optional_missing.append("latitude")
    return longitude, latitude


def _optional_value(
    container: Mapping[str, Any],
    source_field: str,
    output_field: str,
    optional_missing: list[str],
) -> Any:
    """Obtiene un campo opcional y registra su ausencia."""

    value = container.get(source_field)
    if value is None:
        optional_missing.append(output_field)
    return value


def _parse_required_timestamp(value: Any, field_name: str) -> pd.Timestamp:
    """Convierte un timestamp consciente a UTC."""

    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError):
        raise InvalidTransformTimestampError(field_name) from None

    if (
        pd.isna(timestamp)
        or timestamp.tzinfo is None
        or timestamp.utcoffset() is None
    ):
        raise InvalidTransformTimestampError(field_name)
    return timestamp.tz_convert("UTC")


def _build_dataframe(
    *,
    city: str,
    state: str,
    country: str,
    latitude: Any,
    longitude: Any,
    timestamp_api: pd.Timestamp,
    timestamp_extraction: pd.Timestamp,
    aqius: Any,
    main_pollutant: Any,
    weather_values: Mapping[str, Any],
) -> pd.DataFrame:
    """Construye una única fila con dtypes nullable y orden estable."""

    dataframe = pd.DataFrame(
        {
            "record_id": pd.Series([pd.NA], dtype="string"),
            "city": pd.Series([city], dtype="string"),
            "state": pd.Series([state], dtype="string"),
            "country": pd.Series([country], dtype="string"),
            "latitude": _nullable_float_series(latitude),
            "longitude": _nullable_float_series(longitude),
            "timestamp_api": pd.Series(
                [timestamp_api],
                dtype="datetime64[ns, UTC]",
            ),
            "timestamp_extraction": pd.Series(
                [timestamp_extraction],
                dtype="datetime64[ns, UTC]",
            ),
            "aqius": _nullable_integer_series(aqius),
            "main_pollutant": pd.Series(
                [main_pollutant],
                dtype="string",
            ),
            "temperature_c": _nullable_float_series(
                weather_values["temperature_c"]
            ),
            "humidity_pct": _nullable_float_series(
                weather_values["humidity_pct"]
            ),
            "pressure_hpa": _nullable_float_series(
                weather_values["pressure_hpa"]
            ),
            "wind_speed_ms": _nullable_float_series(
                weather_values["wind_speed_ms"]
            ),
            "wind_direction_deg": _nullable_float_series(
                weather_values["wind_direction_deg"]
            ),
        },
        columns=SCHEMA_COLUMNS,
    )
    return dataframe


def _nullable_float_series(value: Any) -> pd.Series:
    """Convierte un valor a ``Float64`` conservando nulos."""

    numeric = pd.to_numeric(pd.Series([value]), errors="coerce")
    return numeric.astype("Float64")


def _nullable_integer_series(value: Any) -> pd.Series:
    """Convierte un valor entero a ``Int64`` y deja inválidos como nulos."""

    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric) or not math.isfinite(float(numeric)):
        normalized = pd.NA
    elif float(numeric).is_integer():
        normalized = int(numeric)
    else:
        normalized = pd.NA
    return pd.Series([normalized], dtype="Int64")
