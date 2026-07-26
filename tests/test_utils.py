"""Pruebas unitarias de utilidades compartidas."""

from __future__ import annotations

import hashlib
import io
import logging
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from etl.config import build_project_paths
from etl.utils import (
    REDACTED,
    configure_safe_logger,
    ensure_data_directories,
    format_utc_timestamp,
    generate_record_id,
    redact_sensitive_text,
    utc_now,
)


def test_utc_now_returns_timezone_aware_utc_datetime() -> None:
    timestamp = utc_now()

    assert timestamp.tzinfo is UTC
    assert timestamp.utcoffset() == timedelta(0)


def test_format_utc_timestamp_normalizes_offset_to_z() -> None:
    source = datetime(
        2026,
        7,
        24,
        8,
        30,
        15,
        tzinfo=timezone(timedelta(hours=-6)),
    )

    assert format_utc_timestamp(source) == "2026-07-24T14:30:15Z"


def test_format_utc_timestamp_rejects_naive_datetime() -> None:
    with pytest.raises(ValueError, match="zona horaria"):
        format_utc_timestamp(datetime(2026, 7, 24, 8, 30, 15))


def test_generate_record_id_matches_canonical_sha256() -> None:
    timestamp = datetime(2026, 7, 25, 1, 2, 3, 456000, tzinfo=UTC)
    canonical_value = (
        "ciudad ejemplo|estado ejemplo|país ejemplo|"
        "2026-07-25T01:02:03.456000Z"
    )

    record_id = generate_record_id(
        "Ciudad Ejemplo",
        "Estado Ejemplo",
        "País Ejemplo",
        timestamp,
    )

    assert record_id == hashlib.sha256(
        canonical_value.encode("utf-8")
    ).hexdigest()
    assert len(record_id) == 64


def test_generate_record_id_normalizes_case_spaces_and_timezone() -> None:
    canonical = generate_record_id(
        "ciudad ejemplo",
        "estado ejemplo",
        "méxico",
        datetime(2026, 7, 25, 7, 0, tzinfo=UTC),
    )
    equivalent = generate_record_id(
        "  CIUDAD   EJEMPLO  ",
        " Estado\tEjemplo ",
        "MÉXICO",
        datetime(
            2026,
            7,
            25,
            1,
            0,
            tzinfo=timezone(timedelta(hours=-6)),
        ),
    )

    assert equivalent == canonical


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    [
        ("city", "Otra Ciudad"),
        ("state", "Otro Estado"),
        ("country", "Otro País"),
        (
            "timestamp_api",
            datetime(2026, 7, 25, 7, 0, 1, tzinfo=UTC),
        ),
    ],
)
def test_generate_record_id_changes_with_identity_components(
    field_name: str,
    replacement: str | datetime,
) -> None:
    components: dict[str, str | datetime] = {
        "city": "Ciudad Ejemplo",
        "state": "Estado Ejemplo",
        "country": "País Ejemplo",
        "timestamp_api": datetime(2026, 7, 25, 7, 0, tzinfo=UTC),
    }
    original = generate_record_id(
        components["city"],
        components["state"],
        components["country"],
        components["timestamp_api"],
    )
    components[field_name] = replacement

    changed = generate_record_id(
        components["city"],
        components["state"],
        components["country"],
        components["timestamp_api"],
    )

    assert changed != original


def test_generate_record_id_rejects_invalid_identity_components() -> None:
    timestamp = datetime(2026, 7, 25, 7, 0, tzinfo=UTC)

    with pytest.raises(ValueError, match="city"):
        generate_record_id("  ", "Estado", "País", timestamp)
    with pytest.raises(ValueError, match="zona horaria"):
        generate_record_id(
            "Ciudad",
            "Estado",
            "País",
            datetime(2026, 7, 25, 7, 0),
        )


def test_ensure_data_directories_is_controlled_and_idempotent(
    tmp_path: Path,
) -> None:
    paths = build_project_paths(tmp_path)

    first_result = ensure_data_directories(paths)
    second_result = ensure_data_directories(paths)

    assert first_result == second_result == (
        paths.raw_dir,
        paths.processed_dir,
        paths.db_dir,
    )
    assert all(directory.is_dir() for directory in first_result)
    assert sorted(path.name for path in paths.data_dir.iterdir()) == [
        "db",
        "processed",
        "raw",
    ]


def test_redact_sensitive_text_hides_known_and_query_secrets() -> None:
    secret = "fake-secret-for-tests"
    message = (
        f"key={secret} "
        "https://api.example.test/v2?api_key=another-fake-secret&city=cdmx"
    )

    redacted = redact_sensitive_text(message, secrets=(secret,))

    assert secret not in redacted
    assert "another-fake-secret" not in redacted
    assert redacted.count(REDACTED) == 2


def test_safe_logger_redacts_api_key_from_messages_and_exceptions() -> None:
    secret = "fake-secret-for-logger-tests"
    stream = io.StringIO()
    logger = configure_safe_logger(
        "etl.tests.safe",
        api_key=secret,
        level=logging.INFO,
        stream=stream,
    )

    logger.info(
        "Solicitud a %s",
        f"https://api.example.test/v2/city?key={secret}&city=cdmx",
    )
    try:
        raise RuntimeError(f"Falló la solicitud con api_key={secret}")
    except RuntimeError:
        logger.exception("Error controlado")

    output = stream.getvalue()
    assert secret not in output
    assert REDACTED in output
    assert "RuntimeError" in output
    assert "Z | INFO | etl.tests.safe" in output
