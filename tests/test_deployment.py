"""Pruebas de la configuración local del deployment Prefect."""

from __future__ import annotations

import asyncio
from datetime import datetime
from importlib import import_module
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml
from prefect.server.schemas.schedules import CronSchedule


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PREFECT_FILE = PROJECT_ROOT / "prefect.yaml"
EXPECTED_COLUMNS = {
    "name",
    "description",
    "entrypoint",
    "parameters",
    "schedule",
    "concurrency_limit",
    "work_pool",
}


def _load_deployment() -> dict[str, object]:
    """Carga el único deployment declarado para las comprobaciones."""

    configuration = yaml.safe_load(PREFECT_FILE.read_text(encoding="utf-8"))
    deployments = configuration["deployments"]
    assert isinstance(deployments, list)
    assert len(deployments) == 1
    deployment = deployments[0]
    assert isinstance(deployment, dict)
    return deployment


def test_deployment_targets_public_flow_and_local_process_pool() -> None:
    deployment = _load_deployment()

    assert EXPECTED_COLUMNS <= deployment.keys()
    assert deployment["name"] == "calidad-aire-cdmx-horario"
    assert deployment["entrypoint"] == "etl/flow.py:air_quality_flow"
    assert deployment["parameters"] == {}
    assert deployment["work_pool"] == {
        "name": "ambiente-etl-local",
        "work_queue_name": "default",
        "job_variables": {"working_dir": "."},
    }

    module = import_module("etl.flow")
    assert getattr(module, "air_quality_flow").name == (
        "pipeline-calidad-aire-cdmx"
    )


def test_schedule_and_next_run_use_mexico_city_timezone() -> None:
    deployment = _load_deployment()
    schedule_config = deployment["schedule"]
    assert isinstance(schedule_config, dict)
    assert schedule_config == {
        "cron": "10 * * * *",
        "timezone": "America/Mexico_City",
        "active": True,
    }

    schedule = CronSchedule(
        cron=schedule_config["cron"],
        timezone=schedule_config["timezone"],
    )
    start = datetime(
        2026,
        7,
        26,
        12,
        5,
        tzinfo=ZoneInfo("America/Mexico_City"),
    )
    dates = asyncio.run(schedule.get_dates(n=1, start=start))

    assert dates[0].isoformat() == "2026-07-26T12:10:00-06:00"


def test_concurrency_cancels_new_overlapping_runs() -> None:
    deployment = _load_deployment()

    assert deployment["concurrency_limit"] == {
        "limit": 1,
        "collision_strategy": "CANCEL_NEW",
    }


def test_prefect_configuration_contains_no_secrets() -> None:
    contents = PREFECT_FILE.read_text(encoding="utf-8").casefold()

    for sensitive_name in (
        "api_key",
        "iqair_api_key",
        "password",
        "secret",
        "token",
    ):
        assert sensitive_name not in contents
    assert ".env" not in contents
