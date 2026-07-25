"""Pruebas unitarias de configuración y rutas."""

from __future__ import annotations

from pathlib import Path

import pytest

from etl.config import (
    DEFAULT_PROJECT_ROOT,
    ConfigurationError,
    build_project_paths,
    load_settings,
)


VALID_ENV = {
    "IQAIR_API_KEY": "fake-key-for-tests",
    "IQAIR_CITY": "Mexico City",
    "IQAIR_STATE": "Mexico City",
    "IQAIR_COUNTRY": "Mexico",
    "IQAIR_BASE_URL": "https://api.airvisual.com/v2/",
    "IQAIR_TIMEOUT_SECONDS": "30",
}


def test_load_settings_from_default_dotenv(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    (project_root / ".env").write_text(
        "\n".join(f"{key}={value}" for key, value in VALID_ENV.items()),
        encoding="utf-8",
    )

    settings = load_settings(environ={}, project_root=project_root)

    assert settings.api_key == "fake-key-for-tests"
    assert settings.city == "Mexico City"
    assert settings.state == "Mexico City"
    assert settings.country == "Mexico"
    assert settings.base_url == "https://api.airvisual.com/v2"
    assert settings.timeout_seconds == 30.0


def test_environment_values_override_dotenv(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(f"{key}={value}" for key, value in VALID_ENV.items()),
        encoding="utf-8",
    )

    settings = load_settings(
        env_file=env_file,
        environ={"IQAIR_CITY": "Ciudad de México"},
        project_root=tmp_path,
    )

    assert settings.city == "Ciudad de México"
    assert settings.country == "Mexico"


@pytest.mark.parametrize("missing_variable", sorted(VALID_ENV))
def test_missing_required_variable_is_reported(
    missing_variable: str,
    tmp_path: Path,
) -> None:
    values = VALID_ENV.copy()
    values.pop(missing_variable)

    with pytest.raises(ConfigurationError, match=missing_variable):
        load_settings(
            env_file=tmp_path / "missing.env",
            environ=values,
            project_root=tmp_path,
        )


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("IQAIR_API_KEY", "replace_with_your_api_key"),
        ("IQAIR_CITY", "confirm_with_iqair"),
        ("IQAIR_STATE", " "),
        ("IQAIR_COUNTRY", "changeme"),
    ],
)
def test_invalid_text_values_are_rejected(
    variable: str,
    value: str,
    tmp_path: Path,
) -> None:
    values = VALID_ENV | {variable: value}

    with pytest.raises(ConfigurationError, match=variable):
        load_settings(
            env_file=tmp_path / "missing.env",
            environ=values,
            project_root=tmp_path,
        )


@pytest.mark.parametrize(
    "base_url",
    [
        "http://api.airvisual.com/v2",
        "not-a-url",
        "https://user:password@api.airvisual.com/v2",
        "https://api.airvisual.com/v2?key=secret",
        "https://api.airvisual.com/v2#section",
        "https://api.airvisual.com/v 2",
        "https://api.airvisual.com:not-a-port/v2",
        "https://[invalid",
    ],
)
def test_invalid_base_url_is_rejected(
    base_url: str,
    tmp_path: Path,
) -> None:
    values = VALID_ENV | {"IQAIR_BASE_URL": base_url}

    with pytest.raises(ConfigurationError, match="IQAIR_BASE_URL"):
        load_settings(
            env_file=tmp_path / "missing.env",
            environ=values,
            project_root=tmp_path,
        )


@pytest.mark.parametrize(
    "timeout",
    ["not-a-number", "0", "-1", "nan", "inf"],
)
def test_invalid_timeout_is_rejected(
    timeout: str,
    tmp_path: Path,
) -> None:
    values = VALID_ENV | {"IQAIR_TIMEOUT_SECONDS": timeout}

    with pytest.raises(ConfigurationError, match="IQAIR_TIMEOUT_SECONDS"):
        load_settings(
            env_file=tmp_path / "missing.env",
            environ=values,
            project_root=tmp_path,
        )


def test_api_key_is_not_exposed_in_settings_repr(tmp_path: Path) -> None:
    settings = load_settings(
        env_file=tmp_path / "missing.env",
        environ=VALID_ENV,
        project_root=tmp_path,
    )

    assert settings.api_key not in repr(settings)


def test_default_paths_do_not_depend_on_current_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)

    settings = load_settings(
        env_file=tmp_path / "missing.env",
        environ=VALID_ENV,
    )

    assert settings.paths.project_root == DEFAULT_PROJECT_ROOT
    assert settings.paths.raw_dir == DEFAULT_PROJECT_ROOT / "data" / "raw"
    assert settings.paths.database_path == (
        DEFAULT_PROJECT_ROOT / "data" / "db" / "ambiente.db"
    )


def test_build_project_paths_contains_expected_outputs(
    tmp_path: Path,
) -> None:
    paths = build_project_paths(tmp_path)

    assert paths.project_root == tmp_path.resolve()
    assert paths.processed_csv_path == (
        tmp_path / "data" / "processed" / "calidad_aire.csv"
    )
    assert paths.processed_parquet_path == (
        tmp_path / "data" / "processed" / "calidad_aire.parquet"
    )
    assert paths.rejected_csv_path == (
        tmp_path / "data" / "processed" / "registros_rechazados.csv"
    )
