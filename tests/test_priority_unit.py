import pytest
from kubernetes import client

from fournos.core.kueue import KueueClient


@pytest.fixture(autouse=True)
def _clean_around_test():
    """Override the integration cleanup fixture; this test uses only a fake client."""


class FakeK8sClient:
    def __init__(self, priority_class_values: dict[str, int] | None = None):
        self.priority_class_values = (
            {
                "manual": 200,
                "nightly": 100,
                "presubmit": 50,
                "adhoc": 10,
            }
            if priority_class_values is None
            else priority_class_values
        )
        self.priority_class_lookups = []
        self.body = None

    def get_cluster_custom_object(self, **kwargs):
        self.priority_class_lookups.append(kwargs)
        name = kwargs["name"]
        if name not in self.priority_class_values:
            raise client.exceptions.ApiException(status=404, reason="Not Found")
        return {"value": self.priority_class_values[name]}

    def create_namespaced_custom_object(self, **kwargs):
        self.body = kwargs["body"]
        return self.body


@pytest.mark.parametrize(
    ("priority", "value"),
    [("manual", 200), ("nightly", 100), ("presubmit", 50), ("adhoc", 10)],
)
def test_create_workload_sets_priority_class(priority: str, value: int) -> None:
    api = FakeK8sClient()

    KueueClient(api).create_workload(name="priority-test", priority=priority)

    assert api.body["spec"]["priorityClassRef"] == {
        "group": "kueue.x-k8s.io",
        "kind": "WorkloadPriorityClass",
        "name": priority,
    }
    assert api.body["spec"]["priority"] == value
    assert "priorityClassName" not in api.body["spec"]
    assert api.priority_class_lookups == [
        {
            "group": "kueue.x-k8s.io",
            "version": "v1beta2",
            "plural": "workloadpriorityclasses",
            "name": priority,
        }
    ]


def test_create_workload_without_priority_does_not_lookup_a_class() -> None:
    api = FakeK8sClient()

    KueueClient(api).create_workload(name="priority-test")

    assert "priorityClassRef" not in api.body["spec"]
    assert "priority" not in api.body["spec"]
    assert api.priority_class_lookups == []


def test_create_workload_fails_when_priority_class_is_missing() -> None:
    api = FakeK8sClient(priority_class_values={})

    with pytest.raises(ValueError, match="WorkloadPriorityClass 'manual' does not exist"):
        KueueClient(api).create_workload(name="priority-test", priority="manual")

    assert api.body is None


def test_create_workload_fails_when_priority_class_value_is_not_an_integer() -> None:
    api = FakeK8sClient(priority_class_values={"manual": "high"})

    with pytest.raises(
        TypeError,
        match="WorkloadPriorityClass 'manual' does not have an integer value",
    ):
        KueueClient(api).create_workload(name="priority-test", priority="manual")

    assert api.body is None
