"""TTL GC tests — verify that terminal FournosJobs with an expired TTL
are deleted by the background GC loop, and that the duration parser works.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from fournos.core.duration import parse_duration
from fournos.operator import _gc_expired_jobs, _get_completion_time

# ---------------------------------------------------------------------------
# Duration parser unit tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [
        ("12h", timedelta(hours=12)),
        ("30m", timedelta(minutes=30)),
        ("7d", timedelta(days=7)),
        ("1h30m", timedelta(hours=1, minutes=30)),
        ("2d6h30m", timedelta(days=2, hours=6, minutes=30)),
        ("90s", timedelta(seconds=90)),
        ("1d0h0m0s", timedelta(days=1)),
        ("", None),
        ("invalid", None),
        ("abc123", None),
    ],
)
def test_parse_duration(value, expected):
    assert parse_duration(value) == expected


# ---------------------------------------------------------------------------
# _get_completion_time unit tests
# ---------------------------------------------------------------------------


def test_get_completion_time_from_status_field():
    job = {
        "metadata": {"creationTimestamp": "2026-08-20T08:00:00Z"},
        "status": {
            "phase": "Succeeded",
            "completionTime": "2026-08-20T10:00:00Z",
        },
    }
    result = _get_completion_time(job)
    assert result == datetime(2026, 8, 20, 10, 0, 0, tzinfo=UTC)


def test_get_completion_time_falls_back_to_creation_timestamp():
    job = {
        "metadata": {"creationTimestamp": "2026-08-20T09:00:00Z"},
        "status": {"phase": "Failed"},
    }
    result = _get_completion_time(job)
    assert result == datetime(2026, 8, 20, 9, 0, 0, tzinfo=UTC)


def test_get_completion_time_no_timestamps():
    job = {"metadata": {}, "status": {"phase": "Failed"}}
    assert _get_completion_time(job) is None


# ---------------------------------------------------------------------------
# _gc_expired_jobs integration-style unit tests (mocked K8s client)
# ---------------------------------------------------------------------------


def _make_terminal_job(name, phase, ttl, completed_at):
    """Build a minimal FournosJob dict in a terminal phase."""
    return {
        "metadata": {"name": name},
        "spec": {"ttl": ttl},
        "status": {
            "phase": phase,
            "completionTime": completed_at,
        },
    }


@patch("fournos.operator.client")
@patch("fournos.operator.settings")
def test_gc_expired_jobs_deletes_expired(mock_settings, mock_client):
    mock_settings.workload_namespace = "test-ns"

    expired_job = _make_terminal_job(
        "old-job", "Succeeded", "1h", "2026-08-20T08:00:00Z"
    )
    fresh_job = _make_terminal_job(
        "fresh-job", "Succeeded", "1h", "2099-12-31T23:00:00Z"
    )
    no_ttl_job = {
        "metadata": {"name": "no-ttl"},
        "spec": {},
        "status": {
            "phase": "Failed",
            "completionTime": "2020-01-01T00:00:00Z",
        },
    }
    running_job = {
        "metadata": {"name": "running"},
        "spec": {"ttl": "1h"},
        "status": {"phase": "Running", "conditions": []},
    }

    mock_custom = MagicMock()
    mock_client.CustomObjectsApi.return_value = mock_custom
    mock_custom.list_namespaced_custom_object.return_value = {
        "items": [expired_job, fresh_job, no_ttl_job, running_job]
    }

    _gc_expired_jobs()

    mock_custom.delete_namespaced_custom_object.assert_called_once_with(
        "fournos.dev", "v1", "test-ns", "fournosjobs", "old-job"
    )


@patch("fournos.operator.client")
@patch("fournos.operator.settings")
def test_gc_expired_jobs_no_deletions_when_none_expired(mock_settings, mock_client):
    mock_settings.workload_namespace = "test-ns"

    fresh_job = _make_terminal_job("fresh-job", "Failed", "12h", "2099-12-31T23:00:00Z")

    mock_custom = MagicMock()
    mock_client.CustomObjectsApi.return_value = mock_custom
    mock_custom.list_namespaced_custom_object.return_value = {"items": [fresh_job]}

    _gc_expired_jobs()

    mock_custom.delete_namespaced_custom_object.assert_not_called()
