"""Transformación tabular del contrato confirmado de IQAir."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, overload

import pandas as pd

from etl.extract import ExtractionResult
from etl.utils import generate_record_id


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
REJECTED_COLUMNS: tuple[str, ...] = (
    *SCHEMA_COLUMNS,
    "rejection_reason",
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
_STRING_COLUMNS: tuple[str, ...] = (
    "record_id",
    "city",
    "state",
    "country",
    "main_pollutant",
)
_FLOAT_COLUMNS: tuple[str, ...] = (
    "latitude",
    "longitude",
    "temperature_c",
    "humidity_pct",
    "pressure_hpa",
    "wind_speed_ms",
    "wind_direction_deg",
)
_TIMESTAMP_COLUMNS: tuple[str, ...] = (
    "timestamp_api",
    "timestamp_extraction",
)
_OPTIONAL_NUMERIC_RULES: tuple[
    tuple[str, float | None, bool, float | None, bool],
    ...,
] = (
    ("latitude", -90.0, True, 90.0, True),
    ("longitude", -180.0, True, 180.0, True),
    ("temperature_c", None, True, None, True),
    ("humidity_pct", 0.0, True, 100.0, True),
    ("pressure_hpa", 0.0, False, None, True),
    ("wind_speed_ms", 0.0, True, None, True),
    ("wind_direction_deg", 0.0, True, 360.0, False),
)
_QualityStatus = Literal["missing", "invalid", "valid"]


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


class QualityValidationError(ValueError):
    """Error base de la validación de calidad tabular."""


class MissingQualityColumnsError(QualityValidationError):
    """El DataFrame no contiene todas las columnas del esquema confirmado."""

    def __init__(self, missing_columns: tuple[str, ...]) -> None:
        self.missing_columns = missing_columns
        columns = ", ".join(missing_columns)
        super().__init__(
            f"El DataFrame no contiene las columnas requeridas: {columns}"
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


@dataclass(frozen=True, slots=True)
class QualityResult:
    """Resultado tipado de validar y clasificar registros normalizados."""

    valid_records: pd.DataFrame = field(repr=False)
    rejected_records: pd.DataFrame = field(repr=False)
    warnings: tuple[TransformWarning, ...]
    total_received: int
    total_valid: int
    total_rejected: int


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


def validate_air_quality(
    source: TransformResult | pd.DataFrame,
) -> QualityResult:
    """Valida registros normalizados y los separa sin alterar la entrada."""

    if isinstance(source, TransformResult):
        dataframe = source.dataframe
        warnings = source.warnings
    elif isinstance(source, pd.DataFrame):
        dataframe = source
        warnings = ()
    else:
        raise QualityValidationError(
            "La validación requiere TransformResult o un DataFrame"
        )

    missing_columns = tuple(
        column
        for column in SCHEMA_COLUMNS
        if column not in dataframe.columns
    )
    if missing_columns:
        raise MissingQualityColumnsError(missing_columns)

    source_copy = dataframe.loc[:, SCHEMA_COLUMNS].copy(deep=True)
    valid_rows: list[dict[str, Any]] = []
    rejected_rows: list[dict[str, Any]] = []

    for _, row in source_copy.iterrows():
        normalized, rejection_reasons = _validate_normalized_record(row)
        if rejection_reasons:
            rejected = {
                column: row[column]
                for column in SCHEMA_COLUMNS
            }
            rejected["record_id"] = pd.NA
            rejected["rejection_reason"] = "; ".join(rejection_reasons)
            rejected_rows.append(rejected)
            continue

        normalized["record_id"] = generate_record_id(
            normalized["city"],
            normalized["state"],
            normalized["country"],
            normalized["timestamp_api"].to_pydatetime(),
        )
        valid_rows.append(normalized)

    valid_records = _build_valid_records(valid_rows)
    rejected_records = _build_rejected_records(rejected_rows)
    return QualityResult(
        valid_records=valid_records,
        rejected_records=rejected_records,
        warnings=warnings,
        total_received=len(source_copy),
        total_valid=len(valid_records),
        total_rejected=len(rejected_records),
    )


def _validate_normalized_record(
    row: pd.Series,
) -> tuple[dict[str, Any], list[str]]:
    """Valida una fila y devuelve valores normalizados y motivos estables."""

    normalized = {
        column: row[column]
        for column in SCHEMA_COLUMNS
    }
    normalized["record_id"] = pd.NA
    reasons: list[str] = []

    for column in ("city", "state", "country"):
        value = row[column]
        if not isinstance(value, str) or not value.strip():
            reasons.append(f"{column}: debe contener texto")
        else:
            normalized[column] = value

    for column in _TIMESTAMP_COLUMNS:
        timestamp = _quality_timestamp(row[column])
        if timestamp is None:
            reasons.append(
                f"{column}: debe ser una fecha válida con zona horaria"
            )
        else:
            normalized[column] = timestamp

    aqius, aqius_status = _quality_integer(row["aqius"])
    if aqius_status != "valid":
        reasons.append("aqius: debe existir y ser un entero válido")
    else:
        normalized["aqius"] = aqius
        if aqius < 0:
            reasons.append("aqius: debe ser mayor o igual que 0")

    for (
        column,
        minimum,
        minimum_inclusive,
        maximum,
        maximum_inclusive,
    ) in _OPTIONAL_NUMERIC_RULES:
        numeric, status = _quality_number(row[column])
        if status == "missing":
            normalized[column] = pd.NA
            continue
        if status == "invalid":
            reasons.append(
                f"{column}: debe ser numérico cuando está presente"
            )
            continue

        normalized[column] = numeric
        if minimum is not None:
            below_minimum = (
                numeric < minimum
                if minimum_inclusive
                else numeric <= minimum
            )
            if below_minimum:
                operator = ">=" if minimum_inclusive else ">"
                reasons.append(
                    f"{column}: debe ser {operator} {minimum:g}"
                )
                continue
        if maximum is not None:
            above_maximum = (
                numeric > maximum
                if maximum_inclusive
                else numeric >= maximum
            )
            if above_maximum:
                operator = "<=" if maximum_inclusive else "<"
                reasons.append(
                    f"{column}: debe ser {operator} {maximum:g}"
                )

    main_pollutant = row["main_pollutant"]
    if _is_null_scalar(main_pollutant):
        normalized["main_pollutant"] = pd.NA
    elif not isinstance(main_pollutant, str) or not main_pollutant.strip():
        reasons.append(
            "main_pollutant: debe contener texto cuando está presente"
        )
    else:
        normalized["main_pollutant"] = main_pollutant

    return normalized, reasons


def _quality_timestamp(value: Any) -> pd.Timestamp | None:
    """Normaliza un timestamp válido y consciente a UTC."""

    if _is_null_scalar(value):
        return None
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if (
        pd.isna(timestamp)
        or timestamp.tzinfo is None
        or timestamp.utcoffset() is None
    ):
        return None
    return timestamp.tz_convert("UTC")


def _quality_integer(value: Any) -> tuple[int, _QualityStatus]:
    """Convierte un entero requerido e informa si es válido."""

    if _is_null_scalar(value) or isinstance(value, bool):
        return 0, "missing"
    numeric, status = _quality_number(value)
    if status != "valid" or not numeric.is_integer():
        return 0, "invalid"
    return int(numeric), "valid"


def _quality_number(value: Any) -> tuple[float, _QualityStatus]:
    """Convierte un escalar numérico finito conservando el estado de nulo."""

    if _is_null_scalar(value):
        return 0.0, "missing"
    if isinstance(value, bool):
        return 0.0, "invalid"
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError):
        return 0.0, "invalid"
    if not math.isfinite(numeric):
        return 0.0, "invalid"
    return numeric, "valid"


def _build_valid_records(rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Construye registros aceptados con el esquema tipado confirmado."""

    dataframe = pd.DataFrame(rows, columns=SCHEMA_COLUMNS)
    for column in _STRING_COLUMNS:
        dataframe[column] = dataframe[column].astype("string")
    for column in _FLOAT_COLUMNS:
        dataframe[column] = pd.to_numeric(
            dataframe[column],
            errors="coerce",
        ).astype("Float64")
    for column in _TIMESTAMP_COLUMNS:
        dataframe[column] = pd.to_datetime(
            dataframe[column],
            errors="coerce",
            utc=True,
        ).astype("datetime64[ns, UTC]")
    dataframe["aqius"] = pd.to_numeric(
        dataframe["aqius"],
        errors="coerce",
    ).astype("Int64")
    return dataframe


def _build_rejected_records(rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Conserva valores rechazados con dtypes aptos para datos inválidos."""

    dataframe = pd.DataFrame(rows, columns=REJECTED_COLUMNS)
    for column in SCHEMA_COLUMNS:
        dataframe[column] = dataframe[column].astype("object")
    dataframe["record_id"] = dataframe["record_id"].astype("string")
    dataframe["rejection_reason"] = dataframe[
        "rejection_reason"
    ].astype("string")
    return dataframe


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
    """Convierte a ``Float64`` sin ocultar valores de origen inválidos."""

    if _is_null_scalar(value):
        return pd.Series([pd.NA], dtype="Float64")
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce")
    if pd.isna(numeric.iloc[0]):
        return pd.Series([value], dtype="object")
    return numeric.astype("Float64")


def _nullable_integer_series(value: Any) -> pd.Series:
    """Convierte a ``Int64`` sin confundir inválidos con valores ausentes."""

    if _is_null_scalar(value):
        return pd.Series([pd.NA], dtype="Int64")
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric) or not math.isfinite(float(numeric)):
        return pd.Series([value], dtype="object")
    elif float(numeric).is_integer():
        normalized = int(numeric)
    else:
        return pd.Series([value], dtype="object")
    return pd.Series([normalized], dtype="Int64")


def _is_null_scalar(value: Any) -> bool:
    """Identifica nulos escalares sin evaluar colecciones como booleanos."""

    if value is None:
        return True
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        return False
    if hasattr(missing, "__len__"):
        return False
    try:
        return bool(missing)
    except TypeError:
        return False
