"""RHAIIS project plugin — routes, config fetcher, CPT submit, and job helpers."""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

import yaml
from fastapi import APIRouter, HTTPException, Request

from app import db, k8s_client
from app.config import settings
from app.github import fetch_yaml, list_yamls

logger = logging.getLogger(__name__)

router = APIRouter()

PROJECT = "rhaiis"
SECRET_REFS = ["psap-forge-dashboard-s3", "psap-forge-notifications"]

_ORCHESTRATION = "projects/rhaiis/orchestration"

_CATEGORY_KEYS = {
    "rhaiis.accelerator": "accelerator",
    "rhaiis.engine": "engine",
    "rhaiis.cluster_tag": "cluster",
    "tests.rhaiis.run_benchmark": "benchmark",
    "tests.rhaiis.model_key": "model",
    "tests.rhaiis.workload_key": "workload",
}

_CATEGORY_PLURAL = {
    "accelerator": "accelerators",
    "engine": "engines",
    "cluster": "clusters",
    "benchmark": "benchmarks",
    "model": "models",
    "workload": "workloads",
}

_SETTINGS_KEYS = {
    "tests.rhaiis.warmup": "warmup",
    "rhaiis.profiler.enabled": "profiler",
    "tests.rhaiis.slack_notify_always": "slack",
    "rhaiis.agent_analysis.enabled": "agent_analysis",
    "caliper.postprocess.csv_dashboard.enabled": "csv_dashboard",
    "rhaiis.compare_versions.enabled": "compare_versions",
    "tests.rhaiis.run_benchmark": "benchmark",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_cpt_models(raw_models) -> list[dict]:
    """Normalize __models from a CPT definition into [{name, preset, overrides, tp}, ...].

    Keys may contain a ``/suffix`` to allow the same preset multiple times
    with different settings (e.g. ``llama-70b/tp2``).  The part before ``/``
    is the Forge preset name; the full key is used as the display label.
    """
    if isinstance(raw_models, dict):
        result = []
        for m, ov in raw_models.items():
            parts = m.split("/", 1)
            preset = parts[0]
            suffix = parts[1] if len(parts) > 1 else ""
            entry: dict[str, Any] = {"name": m, "preset": preset, "overrides": {}}
            tp = None
            if isinstance(ov, dict):
                tp = ov.pop("__tp", None)
                entry["overrides"] = ov
            if tp is None and suffix.startswith("tp") and suffix[2:].isdigit():
                tp = int(suffix[2:])
            entry["tp"] = tp
            result.append(entry)
        return result
    return [{"name": m, "preset": m, "overrides": {}} for m in raw_models]


def fetch_config() -> dict:
    """Fetch and categorize rhaiis presets from the forge GitHub repo."""
    config_dir = f"{_ORCHESTRATION}/config.d"
    presets_dir = f"{_ORCHESTRATION}/presets.d"

    categories: dict[str, list[dict]] = {
        "quick_presets": [],
        "accelerators": [],
        "engines": [],
        "clusters": [],
        "models": [],
        "workloads": [],
        "benchmarks": [],
    }

    model_display_names: dict[str, str] = {}
    model_tp_sizes: dict[str, int] = {}
    try:
        models_data = fetch_yaml(f"{config_dir}/models.yaml")
        for key, val in models_data.items():
            if isinstance(val, dict):
                model_display_names[key] = val.get("name", key)
                tp = (
                    val.get("vllm_args", {}).get("tensor-parallel-size")
                    or val.get("sglang_args", {}).get("tp-size")
                    or val.get("tensor_parallel")
                    or val.get("tp_size")
                    or val.get("tp")
                )
                if tp is not None:
                    try:
                        model_tp_sizes[key] = int(tp)
                    except (ValueError, TypeError):
                        pass
    except Exception as exc:
        logger.warning("Failed to fetch models.yaml from GitHub: %s", exc)

    cluster_gpu_types: dict[str, str] = {"hera": "h200", "zeus": "h200"}
    try:
        clusters_data = fetch_yaml(f"{config_dir}/clusters.yaml")
        for key, val in clusters_data.items():
            if isinstance(val, dict):
                gpu = val.get("gpu_type", val.get("gpuType", val.get("gpu")))
                if gpu:
                    cluster_gpu_types[key] = str(gpu)
    except Exception as exc:
        logger.debug("No clusters.yaml in config.d, using defaults: %s", exc)

    engine_images: dict[str, dict[str, str]] = {}
    try:
        rhaiis_data = fetch_yaml(f"{config_dir}/rhaiis.yaml")
        for ename, edata in (rhaiis_data.get("engines") or {}).items():
            if isinstance(edata, dict):
                for accel, img in (edata.get("images") or {}).items():
                    if isinstance(img, str):
                        engine_images.setdefault(ename, {})[accel] = img
    except Exception as exc:
        logger.debug("Failed to fetch rhaiis.yaml for engine defaults: %s", exc)

    workload_profiles: dict[str, dict] = {}
    try:
        workloads_data = fetch_yaml(f"{config_dir}/workloads.yaml")
        for wk, wv in workloads_data.items():
            if isinstance(wv, dict):
                workload_profiles[wk] = wv
    except Exception as exc:
        logger.debug("Failed to fetch workloads.yaml: %s", exc)

    model_key_to_preset: dict[str, str] = {}
    workload_key_to_preset: dict[str, str] = {}
    cpt_pipelines: list[dict] = []

    try:
        preset_files = list_yamls(presets_dir)
    except Exception as exc:
        logger.error("Failed to list presets.d from GitHub: %s", exc)
        preset_files = []

    compound_presets: list[tuple[str, dict]] = []

    for file_path in preset_files:
        try:
            data = fetch_yaml(file_path)
        except Exception as exc:
            logger.warning("Failed to fetch %s: %s", file_path, exc)
            continue

        if data.get("__cpt"):
            for key, entry in data.items():
                if key.startswith("__") or not isinstance(entry, dict):
                    continue
                raw_models = entry.get("__models", [])
                models_list = parse_cpt_models(raw_models)
                cpt_pipelines.append({
                    "key": key,
                    "description": entry.get("__description", ""),
                    "engine": entry.get("__engine", ""),
                    "accelerator": entry.get("__accelerator", ""),
                    "models": models_list,
                    "workloads": entry.get("__workloads", []),
                    "overrides": {
                        k: v for k, v in entry.items()
                        if not k.startswith("__")
                    },
                })
            continue

        for key, overrides in data.items():
            if key.startswith("__"):
                continue
            if not isinstance(overrides, dict):
                continue

            matched_cats = [
                cat for cat_key, cat in _CATEGORY_KEYS.items()
                if cat_key in overrides
            ]

            if len(matched_cats) >= 2:
                compound_presets.append((key, overrides))
                continue

            if "rhaiis.accelerator" in overrides:
                categories["accelerators"].append({"key": key, "label": key.upper(), "overrides": dict(overrides)})
            elif "rhaiis.engine" in overrides:
                categories["engines"].append({"key": key, "label": key, "overrides": dict(overrides)})
            elif "rhaiis.cluster_tag" in overrides:
                cluster_tag = overrides["rhaiis.cluster_tag"]
                entry = {"key": key, "label": key.capitalize(), "overrides": dict(overrides)}
                gpu = cluster_gpu_types.get(cluster_tag, cluster_gpu_types.get(key))
                if gpu:
                    entry["gpu_type"] = gpu
                categories["clusters"].append(entry)
            elif "tests.rhaiis.run_benchmark" in overrides:
                categories["benchmarks"].append({"key": key, "label": key.capitalize(), "overrides": dict(overrides)})
            elif "tests.rhaiis.model_key" in overrides:
                model_key = overrides["tests.rhaiis.model_key"]
                display = model_display_names.get(model_key, key)
                entry = {"key": key, "label": display, "overrides": dict(overrides)}
                tp = model_tp_sizes.get(model_key)
                if tp:
                    entry["gpu_count"] = tp
                categories["models"].append(entry)
                model_key_to_preset[model_key] = key
            elif "tests.rhaiis.workload_key" in overrides:
                wk = overrides["tests.rhaiis.workload_key"]
                entry: dict[str, Any] = {"key": key, "label": key, "overrides": dict(overrides)}
                profile = workload_profiles.get(wk)
                if profile:
                    entry["profile"] = profile
                categories["workloads"].append(entry)
                workload_key_to_preset[wk] = key

    for key, overrides in compound_presets:
        fills: dict[str, Any] = {}
        if "tests.rhaiis.model_key" in overrides:
            mk = overrides["tests.rhaiis.model_key"]
            fills["model"] = model_key_to_preset.get(mk, "")
        if "tests.rhaiis.workload_key" in overrides:
            wk = overrides["tests.rhaiis.workload_key"]
            fills["workload"] = workload_key_to_preset.get(wk, "")
        if "tests.rhaiis.version" in overrides:
            fills["version"] = overrides["tests.rhaiis.version"]
        for cfg_key, fill_key in _SETTINGS_KEYS.items():
            if cfg_key in overrides:
                fills[fill_key] = bool(overrides[cfg_key])

        categories["quick_presets"].append({
            "key": key,
            "label": key.replace("-", " ").replace("_", " ").title(),
            "fills": fills,
            "overrides": dict(overrides),
        })

    engine_defaults: dict[str, str] = {}
    for ename, accel_versions in engine_images.items():
        for accel, ver in accel_versions.items():
            engine_defaults[f"{accel}_{ename}"] = ver

    accel_keys = {e["key"] for e in categories["accelerators"]}
    engine_keys = {e["key"] for e in categories["engines"]}
    invalid_combos = [
        {"accelerator": a, "engine": e}
        for a in accel_keys for e in engine_keys
        if f"{a}_{e}" not in engine_defaults
    ]

    categories["engine_defaults"] = engine_defaults
    categories["invalid_combos"] = invalid_combos
    categories["workload_profiles"] = workload_profiles

    if not cpt_pipelines:
        local_dir = Path(__file__).resolve().parent.parent.parent
        for local_cpt in sorted(local_dir.glob("cpt*.yaml")):
            try:
                with open(local_cpt) as f:
                    cpt_data = yaml.safe_load(f) or {}
                if not cpt_data.get("__cpt"):
                    continue
                for key, entry in cpt_data.items():
                    if key.startswith("__") or not isinstance(entry, dict):
                        continue
                    raw_models = entry.get("__models", [])
                    models_list = parse_cpt_models(raw_models)
                    cpt_pipelines.append({
                        "key": key,
                        "description": entry.get("__description", ""),
                        "engine": entry.get("__engine", ""),
                        "accelerator": entry.get("__accelerator", ""),
                        "models": models_list,
                        "workloads": entry.get("__workloads", []),
                        "overrides": {k: v for k, v in entry.items() if not k.startswith("__")},
                    })
                logger.info("Loaded CPT pipeline(s) from local %s", local_cpt)
            except Exception as exc:
                logger.debug("Failed to load local CPT file %s: %s", local_cpt, exc)

    categories["cpt_pipelines"] = cpt_pipelines

    return categories


# ---------------------------------------------------------------------------
# Config cache
# ---------------------------------------------------------------------------

_config_cache: dict | None = None


async def get_config() -> dict:
    """Return cached config, fetching from GitHub if needed."""
    global _config_cache
    if _config_cache is None:
        result = await asyncio.to_thread(fetch_config)
        if result.get("accelerators") and result.get("engines"):
            _config_cache = result
        else:
            logger.warning("rhaiis config fetch returned incomplete data — not caching")
            return result
    return _config_cache


# ---------------------------------------------------------------------------
# Job helpers (used by main.py submit handler)
# ---------------------------------------------------------------------------

def build_job_overrides(
    rhaiis_args: str,
    rhaiis_version: str,
    rhaiis_overrides: str,
    cluster: str,
) -> dict[str, Any]:
    """Build RHAIIS-specific job fields from the submit form values.

    Returns a dict with keys: args, display_name, generate_name,
    extra_overrides, secret_refs.
    """
    args = [a.strip() for a in rhaiis_args.split(",") if a.strip()]
    display_name = f"rhaiis-{cluster}-{'-'.join(args[:2])}" if args else f"rhaiis-{cluster}"
    generate_name = f"rhaiis-{cluster}-"

    extra_overrides: dict[str, Any] = {}
    if rhaiis_version.strip():
        extra_overrides["tests.rhaiis.version"] = rhaiis_version.strip()
    if rhaiis_overrides.strip():
        try:
            rh_ov = json.loads(rhaiis_overrides)
            if isinstance(rh_ov, dict):
                extra_overrides.update(rh_ov)
        except (ValueError, TypeError):
            pass

    return {
        "args": args,
        "display_name": display_name,
        "generate_name": generate_name,
        "extra_overrides": extra_overrides,
        "secret_refs": list(SECRET_REFS),
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/api/rhaiis-config")
async def rhaiis_config_api():
    """Return categorized rhaiis preset options for the submit form."""
    return await get_config()


@router.post("/api/rhaiis-config/refresh")
async def rhaiis_config_refresh():
    """Force-refresh the cached rhaiis config from GitHub."""
    global _config_cache
    _config_cache = None
    result = await asyncio.to_thread(fetch_config)
    if result.get("accelerators") and result.get("engines"):
        _config_cache = result
    return {
        "status": "ok",
        "accelerators": len(result.get("accelerators", [])),
        "engines": len(result.get("engines", [])),
        "models": len(result.get("models", [])),
    }


@router.post("/api/submit-cpt")
async def submit_cpt(request: Request):
    """Submit a CPT pipeline — creates one FournosJob per model."""
    payload = await request.json()
    models: list[str] = payload.get("models", [])
    workloads: list[str] = payload.get("workloads", [])
    accelerator: str = payload.get("accelerator", "nvidia")
    engine: str = payload.get("engine", "vllm")
    cluster: str = payload.get("cluster", "hera")
    pipeline: str = payload.get("pipeline", "forge-full")
    owner: str = payload.get("owner", "fournos-dashboard")
    priority: str = payload.get("priority", "manual")
    version_label: str = payload.get("version_label", "")
    pull_sha: str = payload.get("pull_sha", "")
    overrides: dict = payload.get("overrides", {})
    engine_version: str = payload.get("engine_version", "")

    if not models or not workloads:
        raise HTTPException(status_code=400, detail="models and workloads are required")

    config = await get_config()
    model_entries = {m["key"]: m for m in config.get("models", [])}
    cluster_entries = {c["key"]: c for c in config.get("clusters", [])}
    gpu_type = cluster_entries.get(cluster, {}).get("gpu_type", "h200")

    results = []
    for model_item in models:
        if isinstance(model_item, dict):
            model_preset = model_item.get("preset", model_item.get("name", ""))
            model_label = model_item.get("name", model_preset)
            per_model_overrides = model_item.get("overrides", {})
        else:
            model_preset = model_item
            model_label = model_item
            per_model_overrides = {}

        model_entry = model_entries.get(model_preset, {})
        model_preset_overrides = model_entry.get("overrides", {})
        model_key = model_preset_overrides.get("tests.rhaiis.model_key", model_preset)
        cpt_tp = model_item.get("tp") if isinstance(model_item, dict) else None
        gpu_count = cpt_tp or model_entry.get("gpu_count", 1)

        args = [accelerator, engine, cluster, model_preset]

        job_overrides: dict[str, Any] = {}
        job_overrides.update(overrides)
        job_overrides.update(per_model_overrides)
        job_overrides["tests.rhaiis.workload_keys"] = workloads
        if version_label:
            job_overrides["tests.rhaiis.version"] = version_label
        if engine_version:
            job_overrides[f"rhaiis.engines.{engine}.images.{accelerator}"] = engine_version

        display_name = f"rhaiis-cpt-{model_preset}-{cluster}"
        generate_name = f"rhaiis-cpt-{cluster}-"

        env: dict[str, str] = {}
        if pull_sha.strip():
            env["PULL_PULL_SHA"] = pull_sha.strip()

        spec: dict[str, Any] = {
            "cluster": cluster,
            "displayName": display_name,
            "owner": owner,
            "pipeline": pipeline,
            "exclusive": False,
            "priority": priority,
            "hardware": {"gpuType": gpu_type, "gpuCount": gpu_count},
            "secretRefs": list(SECRET_REFS),
            "executionEngine": {
                "forge": {
                    "project": PROJECT,
                    "args": args,
                    "configOverrides": job_overrides,
                }
            },
        }

        body = {
            "apiVersion": f"{settings.fournos_api_group}/{settings.fournos_api_version}",
            "kind": "FournosJob",
            "metadata": {
                "generateName": generate_name,
                "namespace": settings.fournos_namespace,
            },
            "spec": spec,
        }

        if env:
            body["spec"]["env"] = env

        try:
            created = await asyncio.to_thread(k8s_client.create_fournos_job, body)
            created_name = created.get("metadata", {}).get("name", generate_name)
            results.append({"model": model_label, "job_name": created_name, "status": "created"})

            try:
                async with db.async_session() as session:
                    async with session.begin():
                        await db.upsert_job(
                            session,
                            name=created_name,
                            project=PROJECT,
                            preset=f"cpt-{model_preset}",
                            cluster=cluster,
                            pipeline=pipeline,
                            owner=owner,
                            status="Pending",
                            config_overrides=job_overrides,
                            fjob_spec=body.get("spec", {}),
                        )
            except Exception as exc:
                logger.error("DB upsert failed for CPT job %s: %s", created_name, exc)
        except Exception as exc:
            results.append({"model": model_label, "error": str(exc), "status": "failed"})

    return {"status": "ok", "jobs": results, "total": len(results)}
