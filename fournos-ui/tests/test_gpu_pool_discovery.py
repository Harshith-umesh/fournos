from app.k8s_client import _gpu_pools_from_kueue


def test_gpu_pools_only_include_queued_flavors_with_positive_quota() -> None:
    cluster_queues = [
        {
            "spec": {
                "resourceGroups": [
                    {
                        "coveredResources": ["fournos/gpu-h200"],
                        "flavors": [
                            {
                                "name": "hera",
                                "resources": [
                                    {
                                        "name": "fournos/gpu-h200",
                                        "nominalQuota": 8,
                                    }
                                ],
                            },
                            {
                                "name": "old-zeus",
                                "resources": [
                                    {
                                        "name": "fournos/gpu-h200",
                                        "nominalQuota": 0,
                                    }
                                ],
                            },
                        ],
                    }
                ]
            }
        }
    ]
    resource_flavors = [
        {
            "metadata": {"name": "hera"},
            "spec": {"nodeLabels": {"fournos.dev/gpu-pool-h200": "h200-pool"}},
        },
        {
            "metadata": {"name": "old-zeus"},
            "spec": {"nodeLabels": {"fournos.dev/gpu-pool-h200": "h200-pool"}},
        },
        {
            "metadata": {"name": "unconfigured"},
            "spec": {"nodeLabels": {"fournos.dev/gpu-pool-h200": "h200-pool"}},
        },
    ]

    assert _gpu_pools_from_kueue(cluster_queues, resource_flavors) == [
        {"gpu_type": "h200", "name": "h200-pool"}
    ]
