import base64
import os
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")

from app import inference_logs


def _job() -> dict:
    return {
        "metadata": {
            "annotations": {"rhaiis.run-uuid": "run-123"},
        },
        "spec": {"cluster": "hera"},
        "status": {
            "engineStatus": {
                "forge": {
                    "relevantDeployments": [
                        {
                            "apiVersion": "serving.kserve.io/v1beta1",
                            "kind": "InferenceService",
                            "name": "llama-3-70b-abc123",
                            "namespace": "rhaiis",
                            "runUUID": "run-123",
                        }
                    ]
                }
            }
        },
    }


def test_get_inference_service_reference_reads_forge_status() -> None:
    assert inference_logs.get_inference_service_reference(_job()) == {
        "name": "llama-3-70b-abc123",
        "namespace": "rhaiis",
        "run_uuid": "run-123",
    }


def test_get_inference_service_reference_rejects_stale_run() -> None:
    job = _job()
    job["status"]["engineStatus"]["forge"]["relevantDeployments"][0]["runUUID"] = (
        "old-run"
    )

    assert inference_logs.get_inference_service_reference(job) is None


def test_get_inference_service_reference_accepts_label_value_characters() -> None:
    job = _job()
    job["metadata"]["annotations"]["rhaiis.run-uuid"] = "run_123.abc"
    job["status"]["engineStatus"]["forge"]["relevantDeployments"][0]["runUUID"] = (
        "run_123.abc"
    )

    assert (
        inference_logs.get_inference_service_reference(job)["run_uuid"] == "run_123.abc"
    )


def test_fetch_logs_uses_run_scoped_target_pod(monkeypatch: pytest.MonkeyPatch) -> None:
    api_client = SimpleNamespace(close=lambda: None)
    monkeypatch.setattr(
        inference_logs.k8s_config,
        "new_client_from_config_dict",
        lambda kubeconfig: api_client,
    )

    class FakeCustomApi:
        def __init__(self, _api_client):
            pass

        def get_namespaced_custom_object(self, *_args, **_kwargs):
            return {"metadata": {"labels": {"deployment_uuid": "run-123"}}}

    class FakeCoreApi:
        def __init__(self, _api_client):
            self.selector = None
            self.log_kwargs = None

        def list_namespaced_pod(self, _namespace, *, label_selector, **_kwargs):
            self.selector = label_selector
            pod = SimpleNamespace(
                metadata=SimpleNamespace(
                    name="predictor-abc", creation_timestamp=datetime.now(timezone.utc)
                ),
                status=SimpleNamespace(phase="Running"),
                spec=SimpleNamespace(
                    containers=[SimpleNamespace(name="kserve-container")]
                ),
            )
            return SimpleNamespace(items=[pod])

        def read_namespaced_pod_log(self, *_args, **kwargs):
            self.log_kwargs = kwargs
            return "2026-09-30T12:00:00Z server ready"

    fake_core = FakeCoreApi(None)
    monkeypatch.setattr(inference_logs.client, "CustomObjectsApi", FakeCustomApi)
    monkeypatch.setattr(
        inference_logs.client, "CoreV1Api", lambda _api_client: fake_core
    )

    secret_request = {}

    def secret_reader(name, namespace):
        secret_request["name"] = name
        secret_request["namespace"] = namespace
        return {
            "data": {
                "kubeconfig": base64.b64encode(b"current-context: hera\n").decode(
                    "ascii"
                )
            }
        }

    result = inference_logs.get_inference_server_logs(_job(), secret_reader)

    assert result["pod"] == "predictor-abc"
    assert result["logs"].endswith("server ready")
    assert secret_request == {"name": "kubeconfig-hera", "namespace": "psap-secrets"}
    assert fake_core.selector == "serving.kserve.io/inferenceservice=llama-3-70b-abc123"
    assert fake_core.log_kwargs["container"] == "kserve-container"
    assert fake_core.log_kwargs["tail_lines"] == 300


def test_fetch_logs_rejects_missing_reference_before_reading_secret() -> None:
    job = _job()
    job["status"]["engineStatus"]["forge"]["relevantDeployments"] = []

    with pytest.raises(inference_logs.InferenceLogsError) as error:
        inference_logs.get_inference_server_logs(
            job, lambda *_args: pytest.fail("unexpected secret read")
        )

    assert error.value.status_code == 409
