"""Fournos Launcher Dashboard -- production FastAPI application."""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import re
import time
import urllib.parse
import urllib.request
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import yaml
from fastapi import FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader

from app import db, inference_logs, k8s_client, watcher
from app.config import DEFAULT_FJOB_TTL, settings
from app.forge_discovery import discover_projects, get_project_presets

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=getattr(logging, settings.log_level))
    await db.init_db()
    watcher.start_watcher()
    yield

app = FastAPI(title="Fournos Launcher Dashboard", lifespan=lifespan)

BASE_DIR = Path(__file__).resolve().parent
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")

_jinja_env = Environment(
    loader=FileSystemLoader(str(BASE_DIR / "templates")),
    autoescape=True,
    auto_reload=False,
)

# ---------------------------------------------------------------------------
# Template helpers
# ---------------------------------------------------------------------------

def _format_age(timestamp_str: str) -> str:
    from dateutil.parser import parse

    try:
        created = parse(timestamp_str)
    except Exception:
        return "?"
    delta = datetime.now(timezone.utc) - created
    total_seconds = int(delta.total_seconds())
    if total_seconds < 0:
        return "0s"
    if total_seconds < 60:
        return f"{total_seconds}s"
    if total_seconds < 3600:
        return f"{total_seconds // 60}m"
    hours = total_seconds // 3600
    mins = (total_seconds % 3600) // 60
    if hours < 24:
        return f"{hours}h {mins}m"
    days = hours // 24
    return f"{days}d {hours % 24}h"


def _format_duration(seconds: float | None) -> str:
    if seconds is None:
        return "-"
    s = int(seconds)
    h, remainder = divmod(s, 3600)
    m, sec = divmod(remainder, 60)
    return f"{h:02d}h {m:02d}m {sec:02d}s"


def _phase_class(phase: str) -> str:
    return {
        "Running": "phase-running",
        "Succeeded": "phase-succeeded",
        "Failed": "phase-failed",
        "Stopped": "phase-stopped",
        "Resolving": "phase-resolving",
        "Pending": "phase-resolving",
    }.get(phase, "phase-unknown")


def _extract_forge_info(job: dict) -> dict:
    forge = job.get("spec", {}).get("executionEngine", {}).get("forge", {})
    env = job.get("spec", {}).get("env", {})
    pr_number = env.get("PULL_NUMBER", "")
    pr_title = env.get("PULL_TITLE", "")
    repo_owner = env.get("REPO_OWNER", "")
    repo_name = env.get("REPO_NAME", "")
    pr_url = f"https://github.com/{repo_owner}/{repo_name}/pull/{pr_number}" if pr_number else ""
    return {
        "project": forge.get("project", ""),
        "args": forge.get("args", []),
        "config_overrides": forge.get("configOverrides", {}),
        "pr_number": pr_number,
        "pr_title": pr_title,
        "pr_url": pr_url,
    }


def _parse_task_progress(message: str) -> dict | None:
    m = re.search(
        r"Tasks Completed:\s*(\d+)\s*\(Failed:\s*(\d+),\s*Cancelled\s*(\d+)\),\s*Incomplete:\s*(\d+),\s*Skipped:\s*(\d+)",
        message,
    )
    if not m:
        return None
    return {
        "completed": int(m.group(1)),
        "failed": int(m.group(2)),
        "cancelled": int(m.group(3)),
        "incomplete": int(m.group(4)),
        "skipped": int(m.group(5)),
        "total": int(m.group(1)) + int(m.group(4)) + int(m.group(5)),
    }


def _build_timeline(stages: list[dict]) -> list[dict]:
    from dateutil.parser import parse

    now = datetime.now(timezone.utc)
    n = len(stages) or 1
    equal_pct = 100.0 / n

    result = []
    for s in stages:
        start = parse(s["startTime"]) if s["startTime"] else None
        end = parse(s["completionTime"]) if s["completionTime"] else None
        if start and end:
            dur = (end - start).total_seconds()
        elif start:
            dur = (now - start).total_seconds()
        else:
            dur = 0
        dur = max(dur, 0)

        if dur < 60:
            dur_label = f"{int(dur)}s"
        elif dur < 3600:
            dur_label = f"{int(dur // 60)}m {int(dur % 60)}s"
        else:
            dur_label = f"{int(dur // 3600)}h {int((dur % 3600) // 60)}m"

        status_class = {
            "Succeeded": "ptl-ok",
            "Running": "ptl-run",
            "Failed": "ptl-err",
            "Pending": "ptl-wait",
            "Cancelled": "ptl-cancel",
            "Skipped": "ptl-skip",
        }.get(s["status"], "ptl-wait")

        result.append({
            **s,
            "width_pct": equal_pct,
            "min_width": 8,
            "duration_label": dur_label if s["startTime"] else "",
            "status_class": status_class,
        })
    return result


def _extract_mlflow_url(status: dict) -> str:
    """Extract MLflow run URL from FournosJob status."""
    mlflow = (
        status.get("engineStatus", {})
        .get("forge", {})
        .get("exportArtifacts", {})
        .get("caliper_artifacts_export", {})
        .get("backends", {})
        .get("mlflow", {})
    )
    return mlflow.get("run_url", "") if mlflow else ""


_CACHE_BUST = str(int(datetime.now(timezone.utc).timestamp()))

def _to_fjob_yaml(job_dict: dict) -> str:
    """Reconstruct the FournosJob YAML from the job dict."""
    spec = job_dict.get("spec", {})
    metadata = job_dict.get("metadata", {})
    fjob = {
        "apiVersion": "fournos.dev/v1",
        "kind": "FournosJob",
        "metadata": {"name": metadata.get("name", "")},
        "spec": spec,
    }
    return yaml.dump(fjob, default_flow_style=False, sort_keys=False, allow_unicode=True)


_jinja_env.globals.update(
    format_age=_format_age,
    format_duration=_format_duration,
    phase_class=_phase_class,
    extract_forge_info=_extract_forge_info,
    parse_task_progress=_parse_task_progress,
    build_timeline=_build_timeline,
    extract_mlflow_url=_extract_mlflow_url,
    cpt_status_class=lambda status: _cpt_status_class(status),
    to_fjob_yaml=_to_fjob_yaml,
    default_fjob_ttl=DEFAULT_FJOB_TTL,
    url_for=lambda name, **kw: app.url_path_for(name, **kw),
    cache_bust=_CACHE_BUST,
)


_NAV_MAP = {
    "jobs_list.html": "jobs",
    "job_detail.html": "jobs",
    "components/jobs_table_body.html": "jobs",
    "submit_job.html": "submit",
    "schedules.html": "schedules",
    "schedule_runs.html": "schedules",
    "cpt_jobs.html": "cpt-jobs",
    "cpt_run_detail.html": "cpt-jobs",
    "components/cpt_runs_table_body.html": "cpt-jobs",
    "components/cpt_run_matrix.html": "cpt-jobs",
}


def _render(template_name: str, **context: Any) -> HTMLResponse:
    context.setdefault("active_nav", _NAV_MAP.get(template_name, ""))
    tpl = _jinja_env.get_template(template_name)
    return HTMLResponse(tpl.render(**context))


def _cpt_status_class(status: str) -> str:
    if status == "Succeeded":
        return "phase-succeeded"
    if status in {"Failed", "Not submitted", "Partially failed", "Failed job"}:
        return "phase-failed"
    if status in {"Stopped", "Partially stopped"}:
        return "phase-stopped"
    if status in {"Pending", "Submitting", "Running", "Admitted"}:
        return "phase-running"
    if status == "Resolving":
        return "phase-resolving"
    return "phase-unknown"


def _cpt_job_view(job: db.CptRunJob) -> dict[str, Any]:
    if job.submission_status == "Failed":
        status = "Not submitted"
    elif not job.job_name and job.submission_status != "Created":
        status = "Submitting"
    else:
        status = job.status or "Unknown"

    return {
        "id": job.id,
        "job_name": job.job_name,
        "model_name": job.model_name,
        "model_preset": job.model_preset,
        "workloads": list(job.workloads or []),
        "submission_status": job.submission_status,
        "status": status,
        "message": job.message or "",
    }


def _cpt_run_view(run: db.CptRun, child_jobs: list[db.CptRunJob]) -> dict[str, Any]:
    jobs = [_cpt_job_view(job) for job in child_jobs]
    statuses = [job["status"] for job in jobs]
    succeeded = statuses.count("Succeeded")
    failed = sum(status in {"Failed", "Not submitted"} for status in statuses)
    stopped = statuses.count("Stopped")
    terminal = {"Succeeded", "Failed", "Not submitted", "Stopped"}

    if not statuses:
        status = "Submitting"
    elif any(item not in terminal for item in statuses):
        status = "Running" if any(job["job_name"] for job in jobs) else "Submitting"
    elif failed and succeeded:
        status = "Partially failed"
    elif failed:
        status = "Failed"
    elif stopped and succeeded:
        status = "Partially stopped"
    elif stopped:
        status = "Stopped"
    elif succeeded == len(statuses):
        status = "Succeeded"
    else:
        status = "Unknown"

    profiles = list(dict.fromkeys(
        workload for job in jobs for workload in job["workloads"]
    ))
    return {
        "run": run,
        "jobs": jobs,
        "profiles": profiles,
        "status": status,
        "total_jobs": len(jobs),
        "succeeded_jobs": succeeded,
        "failed_jobs": failed,
        "stopped_jobs": stopped,
        "total_profile_cases": sum(len(job["workloads"]) for job in jobs),
        "succeeded_profile_cases": sum(
            len(job["workloads"]) for job in jobs if job["status"] == "Succeeded"
        ),
    }


# ---------------------------------------------------------------------------
# Data fetching helpers
# ---------------------------------------------------------------------------

_COMPLETED_GRACE_SECONDS = 180  # keep completed jobs on Live tab for 3 minutes


def _get_live_jobs_sync() -> list[dict]:
    """Get FournosJobs from K8s, sorted newest-first, hiding old completed jobs."""
    from dateutil.parser import parse

    jobs = k8s_client.list_fournos_jobs()
    now = datetime.now(timezone.utc)
    visible: list[dict] = []
    for j in jobs:
        phase = j.get("status", {}).get("phase", "")
        if phase in ("Succeeded", "Failed", "Stopped"):
            conditions = j.get("status", {}).get("conditions", [])
            last_ts = None
            for c in conditions:
                ts_str = c.get("lastTransitionTime")
                if ts_str:
                    try:
                        last_ts = parse(ts_str)
                    except Exception:
                        pass
            if last_ts and (now - last_ts).total_seconds() > _COMPLETED_GRACE_SECONDS:
                continue
        visible.append(j)

    visible.sort(
        key=lambda j: j.get("metadata", {}).get("creationTimestamp", ""),
        reverse=True,
    )
    return visible


async def _get_live_jobs() -> list[dict]:
    """Async wrapper -- offloads blocking K8s I/O to a thread."""
    return await asyncio.to_thread(_get_live_jobs_sync)


def _compute_current_steps_sync(jobs: list[dict]) -> dict[str, dict]:
    """For each running job, fetch the currently active pipeline step."""
    steps: dict[str, dict] = {}
    for j in jobs:
        phase = j.get("status", {}).get("phase", "")
        if phase not in ("Running", "Admitted"):
            continue
        name = j.get("metadata", {}).get("name", "")
        try:
            step = k8s_client.get_current_step_for_job(name)
            if step:
                steps[name] = step
        except Exception:
            pass
    return steps


async def _compute_current_steps(jobs: list[dict]) -> dict[str, dict]:
    """Async wrapper -- offloads blocking K8s I/O to a thread."""
    return await asyncio.to_thread(_compute_current_steps_sync, jobs)


def _get_pipeline_stages_sync(job: dict) -> list[dict]:
    """Get pipeline stages for a job from its PipelineRun."""
    job_name = job.get("metadata", {}).get("name", "")

    pr_name = job.get("status", {}).get("pipelineRun", "")
    if pr_name:
        pr = k8s_client.get_pipelinerun(pr_name)
        if pr:
            return k8s_client.extract_pipeline_stages(pr)

    prs = k8s_client.list_pipelineruns_for_job(job_name)
    if prs:
        return k8s_client.extract_pipeline_stages(prs[0])

    return []


async def _get_pipeline_stages(job: dict) -> list[dict]:
    """Async wrapper -- offloads blocking K8s I/O to a thread."""
    return await asyncio.to_thread(_get_pipeline_stages_sync, job)


# ---------------------------------------------------------------------------
# Routes: Jobs
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def jobs_list(
    request: Request,
    tab: str = Query("live", pattern="^(live|history)$"),
    project: str = Query("", alias="project"),
    cluster: str = Query("", alias="cluster"),
    status: str = Query("", alias="status"),
    owner: str = Query("", alias="owner"),
    page: int = Query(1, ge=1),
):
    per_page = 50
    filters = {"project": project, "cluster": cluster, "status": status, "owner": owner}

    if tab == "live":
        jobs = await _get_live_jobs()
        if project:
            jobs = [j for j in jobs if _extract_forge_info(j).get("project") == project]
        if cluster:
            jobs = [j for j in jobs if j.get("spec", {}).get("cluster") == cluster]
        if status:
            jobs = [j for j in jobs if j.get("status", {}).get("phase") == status]
        if owner:
            jobs = [j for j in jobs if j.get("spec", {}).get("owner") == owner]
        total = len(jobs)
        all_clusters = _collect_clusters(jobs)
        offset = (page - 1) * per_page
        jobs = jobs[offset:offset + per_page]
        history_jobs = []
        total_history = 0
    else:
        jobs = []
        all_clusters = []
        async with db.async_session() as session:
            history_jobs_db, total_history = await db.list_jobs(
                session,
                project=project or None,
                cluster=cluster or None,
                status=status or None,
                owner=owner or None,
                limit=per_page,
                offset=(page - 1) * per_page,
            )
            history_jobs = [_db_job_to_dict(j) for j in history_jobs_db]
        total = total_history

    projects_list = [p.name for p in discover_projects()]
    clusters = all_clusters
    current_steps = await _compute_current_steps(jobs) if tab == "live" else {}

    return _render(
        "jobs_list.html",
        jobs=jobs,
        history_jobs=history_jobs,
        tab=tab,
        filters=filters,
        projects=projects_list,
        clusters=clusters,
        page=page,
        per_page=per_page,
        total=total,
        current_steps=current_steps,
    )


@app.get("/api/jobs-table", response_class=HTMLResponse)
async def jobs_table_partial(
    request: Request,
    project: str = Query(""),
    cluster: str = Query(""),
    status: str = Query(""),
    owner: str = Query(""),
):
    jobs = await _get_live_jobs()
    if project:
        jobs = [j for j in jobs if _extract_forge_info(j).get("project") == project]
    if cluster:
        jobs = [j for j in jobs if j.get("spec", {}).get("cluster") == cluster]
    if status:
        jobs = [j for j in jobs if j.get("status", {}).get("phase") == status]
    if owner:
        jobs = [j for j in jobs if j.get("spec", {}).get("owner") == owner]
    current_steps = await _compute_current_steps(jobs)
    return _render("components/jobs_table_body.html", jobs=jobs, current_steps=current_steps)


@app.get("/jobs/{job_name}", response_class=HTMLResponse)
async def job_detail(request: Request, job_name: str):
    job = None
    source = "live"
    pods: list[dict] = []
    stages: list[dict] = []

    job = await asyncio.to_thread(k8s_client.get_fournos_job, job_name)
    if job:
        pods = await asyncio.to_thread(k8s_client.list_pods_for_job, job_name)
        stages = await _get_pipeline_stages(job)

    if not job:
        source = "history"
        async with db.async_session() as session:
            db_job = await db.get_job_by_name(session, job_name)
            if db_job is None:
                raise HTTPException(status_code=404, detail="Job not found")

            job = _db_job_to_fjob_dict(db_job)

    return _render(
        "job_detail.html",
        job=job,
        pods=pods,
        stages=stages,
        source=source,
    )


@app.get("/api/jobs/{job_name}/detail-partial", response_class=HTMLResponse)
async def job_detail_partial(request: Request, job_name: str):
    """Return the dynamic portions of the job detail page for HTMX polling."""
    job = await asyncio.to_thread(k8s_client.get_fournos_job, job_name)
    if not job:
        return HTMLResponse("")
    pods = await asyncio.to_thread(k8s_client.list_pods_for_job, job_name)
    stages = await _get_pipeline_stages(job)
    return _render(
        "components/job_detail_dynamic.html",
        job=job,
        pods=pods,
        stages=stages,
    )


@app.post("/api/jobs/{job_name}/cancel")
async def cancel_job(job_name: str):
    try:
        await asyncio.to_thread(k8s_client.shutdown_fournos_job, job_name)
        return {"status": "ok", "message": f"Shutdown requested for {job_name}"}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/jobs/{job_name}/rerun")
async def rerun_job(job_name: str):
    """Clone an existing FournosJob's spec into a brand-new job."""
    job = await _get_job_for_rerun(job_name)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    spec = dict(job.get("spec", {}))
    forge = spec.get("executionEngine", {}).get("forge", {})
    project = forge.get("project", "unknown")

    spec.pop("shutdown", None)
    spec["ttl"] = DEFAULT_FJOB_TTL

    new_name = sanitize_job_name(f"forge-{project}")
    body = {
        "apiVersion": f"{settings.fournos_api_group}/{settings.fournos_api_version}",
        "kind": "FournosJob",
        "metadata": {
            "name": new_name,
            "namespace": settings.fournos_namespace,
        },
        "spec": spec,
    }

    try:
        created = await asyncio.to_thread(k8s_client.create_fournos_job, body)
        created_name = created.get("metadata", {}).get("name", new_name)
        return {"status": "ok", "job_name": created_name, "redirect": f"/jobs/{created_name}"}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


async def _get_job_for_rerun(job_name: str) -> dict | None:
    """Fetch a FournosJob by name from live K8s or DB history."""
    live = await asyncio.to_thread(k8s_client.get_fournos_job, job_name)
    if live:
        return live
    async with db.async_session() as session:
        db_job = await db.get_job_by_name(session, job_name)
        if db_job:
            return _db_job_to_fjob_dict(db_job)
    return None


def _job_to_edit_draft(job_name: str, job: dict) -> dict[str, Any]:
    """Convert a live or archived FournosJob into safe, editable form values."""
    spec = job.get("spec", {}) or {}
    forge = (
        spec.get("executionEngine", {}).get("forge", {})
        if isinstance(spec.get("executionEngine", {}), dict)
        else {}
    )
    project = str(forge.get("project", "unknown"))
    args = forge.get("args", []) or []
    if not isinstance(args, list):
        args = [args]
    args = [str(arg) for arg in args]
    overrides = forge.get("configOverrides", {}) or {}
    if not isinstance(overrides, dict):
        overrides = {}

    env = spec.get("env", {}) or {}
    hardware = spec.get("hardware", {}) or {}
    supported_spec_fields = {
        "cluster", "displayName", "owner", "pipeline", "exclusive", "priority",
        "hardware", "secretRefs", "executionEngine", "env",
    }
    annotations = job.get("metadata", {}).get("annotations", {}) or {}

    version_key = "tests.rhaiis.version" if project == "rhaiis" else _get_version_config_key(project)
    return {
        "source_job_name": job_name,
        "project": project,
        "cluster": spec.get("cluster", ""),
        "pipeline": spec.get("pipeline", "forge-test-only"),
        "preset": args[0] if args and project != "rhaiis" else "",
        "args": args,
        "owner": spec.get("owner", ""),
        "priority": spec.get("priority", "manual"),
        "exclusive": bool(spec.get("exclusive", False)),
        "pull_sha": env.get("PULL_PULL_SHA", "") if isinstance(env, dict) else "",
        "version": overrides.get(version_key, ""),
        "version_key": version_key,
        "gpu_type": hardware.get("gpuType", "") if isinstance(hardware, dict) else "",
        "gpu_count": hardware.get("gpuCount", 1) if isinstance(hardware, dict) else 1,
        "config_overrides": copy.deepcopy(overrides),
        "is_rhaiis": project == "rhaiis",
        "is_cpt_child": bool(
            annotations.get("fournos.dev/cpt-run-id")
            or str(spec.get("displayName", "")).startswith("rhaiis-cpt-")
        ),
        "preserved_spec_fields": sorted(set(spec) - supported_spec_fields),
    }


def _merge_edit_source_spec(
    source_job: dict,
    submitted_spec: dict,
    project: str,
    submitted_env: dict[str, str],
) -> dict:
    """Keep source-only spec fields while replacing values edited in the form."""
    source_spec = copy.deepcopy(source_job.get("spec", {}) or {})
    source_forge = (
        source_spec.get("executionEngine", {}).get("forge", {})
        if isinstance(source_spec.get("executionEngine", {}), dict)
        else {}
    )
    if source_forge.get("project", "unknown") != project:
        return submitted_spec

    merged = {**source_spec, **submitted_spec}
    source_engine = source_spec.get("executionEngine", {}) or {}
    submitted_engine = submitted_spec.get("executionEngine", {}) or {}
    merged_engine = {**source_engine, **submitted_engine}
    submitted_forge = submitted_engine.get("forge", {}) or {}
    merged_forge = {**source_forge, **submitted_forge}

    # Generic submit exposes one preset. Keep any additional original Forge
    # arguments; RHAIIS arguments are rehydrated individually by its adapter.
    source_args = source_forge.get("args", []) or []
    submitted_args = list(submitted_forge.get("args", []) or [])
    if project != "rhaiis" and isinstance(source_args, list) and len(source_args) > 1:
        merged_forge["args"] = submitted_args + source_args[1:]

    merged_engine["forge"] = merged_forge
    merged["executionEngine"] = merged_engine

    source_hardware = source_spec.get("hardware", {}) or {}
    submitted_hardware = submitted_spec.get("hardware", {}) or {}
    if isinstance(source_hardware, dict) and isinstance(submitted_hardware, dict):
        merged["hardware"] = {**source_hardware, **submitted_hardware}

    # Keep environment variables other than the editable Forge build source.
    source_env = copy.deepcopy(source_spec.get("env", {}) or {})
    if isinstance(source_env, dict):
        old_pull_sha = source_env.get("PULL_PULL_SHA", "")
        source_env.pop("PULL_PULL_SHA", None)
        new_pull_sha = submitted_env.get("PULL_PULL_SHA", "")
        if old_pull_sha != new_pull_sha:
            for key in ("PULL_NUMBER", "PULL_TITLE", "REPO_OWNER", "REPO_NAME"):
                source_env.pop(key, None)
        source_env.update(submitted_env)
        if source_env:
            merged["env"] = source_env
        else:
            merged.pop("env", None)

    # RHAIIS's normal form supplies its standard refs, but retain any refs
    # attached to the source job (including CPT child jobs).
    if "secretRefs" in source_spec and project == "rhaiis":
        merged["secretRefs"] = source_spec["secretRefs"]

    merged.pop("shutdown", None)
    return merged


@app.get("/jobs/{job_name}/edit", response_class=HTMLResponse)
async def edit_job_form(request: Request, job_name: str):
    source_job = await _get_job_for_rerun(job_name)
    if source_job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    return _render(
        "submit_job.html",
        request=request,
        projects=discover_projects(),
        pipelines=list(settings.default_pipelines),
        fournos_namespace=settings.fournos_namespace,
        edit_draft=_job_to_edit_draft(job_name, source_job),
    )


@app.delete("/api/history/{job_name}")
async def delete_history_job(job_name: str):
    """Delete a job from the history database."""
    async with db.async_session() as session:
        async with session.begin():
            deleted = await db.delete_job_by_name(session, job_name)
    if not deleted:
        raise HTTPException(status_code=404, detail="Job not found in history")
    return {"status": "ok"}


_LOG_ISSUE_RE = re.compile(r"\b(?:error|warning|warn)\b", re.IGNORECASE)
_LOG_DECORATION_RE = re.compile(
    r"^\s*(?:error|warning|warn)?\s*:?\s*[-=*_]{3,}\s*$",
    re.IGNORECASE,
)
_LOG_CONTEXT_LINES = 300
_LOG_CONTEXT_HISTORY = _LOG_CONTEXT_LINES - 1
_LOG_CONTEXT_AFTER = (_LOG_CONTEXT_LINES - 1) // 2


def _is_log_issue(line: str) -> bool:
    """Return whether a log line contains an error or warning marker."""
    stripped = line.strip()
    if not stripped or not _LOG_ISSUE_RE.search(stripped):
        return False
    if _LOG_DECORATION_RE.fullmatch(stripped):
        return False

    # Ignore empty ``ERROR:``/``WARNING:`` prefixes as well as separator-only
    # lines, so a decorative footer cannot hide the useful final error.
    issue_text = re.sub(
        r"^\s*(?:error|warning|warn)\s*:?\s*",
        "",
        stripped,
        flags=re.IGNORECASE,
    ).strip()
    return bool(issue_text) and not re.fullmatch(r"[-=*_]{3,}", issue_text)


@app.get("/api/jobs/{job_name}/logs/{pod_name}")
async def stream_logs(job_name: str, pod_name: str):
    """Stream a bounded context window around the latest pod warning/error."""
    job_pods = await asyncio.to_thread(k8s_client.list_pods_for_job, job_name)
    pod_map = {p["name"]: p for p in job_pods}
    if pod_name not in pod_map:
        raise HTTPException(status_code=404, detail="Pod not found for this job")

    pod = pod_map[pod_name]
    is_running = pod.get("phase") in ("Running", "Pending")

    async def generate():
        stop = asyncio.Event()
        # Keep one pending context window plus the end sentinel. The full log
        # remains available through the separate download endpoint.
        queue: asyncio.Queue[tuple[list[str], int] | None] = asyncio.Queue(maxsize=2)
        loop = asyncio.get_event_loop()

        def _publish_latest(context: tuple[list[str], int]) -> None:
            while True:
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            queue.put_nowait(context)

        def _finish() -> None:
            queue.put_nowait(None)

        def _reader():
            recent_lines: deque[str] = deque(maxlen=_LOG_CONTEXT_HISTORY)
            current_issue: str | None = None
            current_before: list[str] = []
            current_after: list[str] = []

            def _build_context() -> tuple[list[str], int] | None:
                if current_issue is None:
                    return None
                after = current_after[:_LOG_CONTEXT_AFTER]
                before_limit = _LOG_CONTEXT_LINES - 1 - len(after)
                before = current_before[-before_limit:] if before_limit else []
                return before + [current_issue] + after, len(before)

            def _publish_context() -> None:
                context = _build_context()
                if context is not None:
                    loop.call_soon_threadsafe(
                        _publish_latest,
                        context,
                    )

            try:
                for line in k8s_client.read_pod_log(pod_name, follow=is_running):
                    if stop.is_set():
                        break
                    if _is_log_issue(line):
                        current_issue = line
                        current_before = list(recent_lines)
                        current_after = []
                        if is_running:
                            _publish_context()
                    elif current_issue is not None:
                        if len(current_after) < _LOG_CONTEXT_AFTER:
                            current_after.append(line)
                            if is_running and (
                                len(current_after) % 25 == 0
                                or len(current_after) == _LOG_CONTEXT_AFTER
                            ):
                                _publish_context()
                    recent_lines.append(line)
            finally:
                _publish_context()
                loop.call_soon_threadsafe(_finish)

        loop.run_in_executor(None, _reader)
        latest_context = None
        try:
            while True:
                context = await queue.get()
                if context is None:
                    break
                latest_context = context
                if is_running:
                    lines, issue_index = context
                    payload = json.dumps(
                        {"lines": lines, "issue_index": issue_index},
                        ensure_ascii=False,
                    )
                    yield f"event: context\ndata: {payload}\n\n"

            if not is_running and latest_context is not None:
                lines, issue_index = latest_context
                payload = json.dumps(
                    {"lines": lines, "issue_index": issue_index},
                    ensure_ascii=False,
                )
                yield f"event: context\ndata: {payload}\n\n"

            yield f"event: complete\ndata: {'found' if latest_context else 'none'}\n\n"
        finally:
            stop.set()

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/jobs/{job_name}/logs/{pod_name}/download")
async def download_logs(job_name: str, pod_name: str):
    """Download the complete current log for a job pod."""
    job_pods = await asyncio.to_thread(k8s_client.list_pods_for_job, job_name)
    pod_map = {p["name"]: p for p in job_pods}
    if pod_name not in pod_map:
        raise HTTPException(status_code=404, detail="Pod not found for this job")

    log_text = await asyncio.to_thread(k8s_client.read_pod_log_full, pod_name)
    safe_job = re.sub(r"[^A-Za-z0-9._-]", "-", job_name)
    safe_pod = re.sub(r"[^A-Za-z0-9._-]", "-", pod_name)
    return Response(
        content=log_text,
        media_type="text/plain",
        headers={
            "Cache-Control": "no-store",
            "Content-Disposition": f'attachment; filename="{safe_job}-{safe_pod}.log"',
        },
    )


def _is_rhaiis_testing_step(step: dict | None) -> bool:
    if not step:
        return False
    step_name = str(step.get("name") or step.get("displayName") or "")
    tokens = re.findall(r"[a-z0-9]+", step_name.lower())
    return "test" in tokens or "testing" in tokens


def _rhaiis_inference_logs_job_eligible(job: dict) -> bool:
    return (
        (job.get("status") or {}).get("phase") == "Running"
        and _extract_forge_info(job).get("project") == "rhaiis"
        and inference_logs.get_inference_service_reference(job) is not None
    )


async def _rhaiis_inference_logs_available(job_name: str, job: dict | None = None) -> bool:
    if job is None:
        job = await asyncio.to_thread(k8s_client.get_fournos_job, job_name)
    if not job or not _rhaiis_inference_logs_job_eligible(job):
        return False
    try:
        step = await asyncio.to_thread(k8s_client.get_current_step_for_job, job_name)
    except Exception:
        logger.debug("Unable to determine active step for %s", job_name, exc_info=True)
        return False
    return _is_rhaiis_testing_step(step)


@app.get("/api/jobs/{job_name}/inference-logs/availability")
async def inference_logs_availability(job_name: str):
    """Tell the live detail page when RHAIIS inference logs can be opened."""
    return {"available": await _rhaiis_inference_logs_available(job_name)}


@app.get("/api/jobs/{job_name}/inference-logs")
async def get_rhaiis_inference_logs(job_name: str, response: Response):
    """Return the latest bounded predictor log tail for an active RHAIIS test."""
    response.headers["Cache-Control"] = "no-store"
    job = await asyncio.to_thread(k8s_client.get_fournos_job, job_name)
    if not job:
        raise HTTPException(status_code=404, detail="FournosJob not found")
    if _extract_forge_info(job).get("project") != "rhaiis":
        raise HTTPException(
            status_code=404,
            detail="Inference logs are only available for RHAIIS jobs.",
        )
    if not _rhaiis_inference_logs_job_eligible(job):
        raise HTTPException(
            status_code=409,
            detail="Inference logs are available only while the RHAIIS test is active.",
        )
    if not await _rhaiis_inference_logs_available(job_name, job):
        raise HTTPException(
            status_code=409,
            detail="Inference logs are available only while the RHAIIS test is active.",
        )
    try:
        return await asyncio.to_thread(
            inference_logs.get_inference_server_logs,
            job,
            k8s_client.get_secret,
            tail_lines=300,
        )
    except inference_logs.InferenceLogsError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


# ---------------------------------------------------------------------------
# Routes: Submit Job
# ---------------------------------------------------------------------------

@app.get("/submit", response_class=HTMLResponse)
async def submit_form(request: Request):
    projects = discover_projects()
    return _render(
        "submit_job.html",
        request=request,
        projects=projects,
        pipelines=list(settings.default_pipelines),
        fournos_namespace=settings.fournos_namespace,
        edit_draft=None,
    )


@app.get("/api/project-info/{project_name}")
async def project_info_api(project_name: str):
    from app.forge_discovery import get_project
    proj = get_project(project_name)
    if proj is None:
        return {"presets": [], "cluster": ""}
    return {"presets": proj.presets, "cluster": proj.cluster}


def _fetch_github_open_prs() -> list[dict]:
    """Blocking call to the GitHub API -- run via asyncio.to_thread."""
    import urllib.request
    import json as _json

    url = f"https://api.github.com/repos/{settings.forge_github_repo}/pulls?state=open&per_page=100"
    req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        prs = _json.loads(resp.read())

    return [
        {
            "number": pr["number"],
            "title": pr["title"],
            "author": pr["user"]["login"],
            "head_sha": pr["head"]["sha"],
            "branch": pr["head"]["ref"],
            "draft": pr["draft"],
        }
        for pr in prs
    ]


@app.get("/api/github/open-prs")
async def github_open_prs():
    """Fetch open pull requests from the Forge GitHub repo (public, no token needed)."""
    try:
        return await asyncio.to_thread(_fetch_github_open_prs)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"GitHub API error: {exc}")


def _fetch_github_releases() -> list[dict]:
    """Fetch published Forge releases for the build-source selector."""
    import urllib.request
    import json as _json

    url = f"https://api.github.com/repos/{settings.forge_github_repo}/releases?per_page=100"
    req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        releases = _json.loads(resp.read())

    return [
        {
            "tag_name": release["tag_name"],
            "name": release.get("name") or release["tag_name"],
            "prerelease": bool(release.get("prerelease")),
            "published_at": release.get("published_at"),
            "html_url": release.get("html_url", ""),
        }
        for release in releases
        if isinstance(release, dict) and release.get("tag_name")
    ]


@app.get("/api/github/releases")
async def github_releases():
    """Fetch published release tags from the Forge GitHub repo."""
    try:
        return await asyncio.to_thread(_fetch_github_releases)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"GitHub API error: {exc}")


_RHAIIS_ORCHESTRATION = "projects/rhaiis/orchestration"


def _github_fetch_yaml(path: str, ref: str | None = None) -> dict:
    """Fetch a single YAML file from the forge GitHub repo and return parsed content."""
    if ref:
        raw_url = (
            f"https://raw.githubusercontent.com/{settings.forge_github_repo}/"
            f"{urllib.parse.quote(ref, safe='')}/{path}"
        )
        raw_req = urllib.request.Request(raw_url)
        with urllib.request.urlopen(raw_req, timeout=15) as resp:
            return yaml.safe_load(resp.read()) or {}

    url = f"https://api.github.com/repos/{settings.forge_github_repo}/contents/{path}"
    req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        meta = json.loads(resp.read())

    download_url = meta.get("download_url", "")
    if not download_url:
        return {}

    raw_req = urllib.request.Request(download_url)
    with urllib.request.urlopen(raw_req, timeout=15) as resp:
        return yaml.safe_load(resp.read()) or {}


def _github_list_yamls(directory: str, ref: str | None = None) -> list[str]:
    """List .yaml file paths in a forge GitHub repo directory."""
    url = f"https://api.github.com/repos/{settings.forge_github_repo}/contents/{directory}"
    if ref:
        url += "?" + urllib.parse.urlencode({"ref": ref})
    req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        items = json.loads(resp.read())

    return sorted(
        item["path"] for item in items
        if isinstance(item, dict) and item.get("name", "").endswith(".yaml")
    )


def _github_default_branch_sha() -> str:
    """Return the current Forge default-branch SHA with one API request."""
    url = f"https://api.github.com/repos/{settings.forge_github_repo}/commits?per_page=1"
    req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        commits = json.loads(resp.read())

    if (
        not isinstance(commits, list)
        or not commits
        or not isinstance(commits[0], dict)
        or not commits[0].get("sha")
    ):
        raise RuntimeError("GitHub returned no commit for the Forge default branch")
    return str(commits[0]["sha"])


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


def _parse_cpt_models(raw_models) -> list[dict]:
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
            model_workloads = None
            if isinstance(ov, dict):
                # Copy the YAML mapping before removing CPT-only metadata.
                # The same parsed definition may be inspected by another
                # consumer during a config refresh.
                model_overrides = dict(ov)
                tp = model_overrides.pop("__tp", None)
                model_workloads = model_overrides.pop("__workloads", None)
                entry["overrides"] = model_overrides
            if tp is None and suffix.startswith("tp") and suffix[2:].isdigit():
                tp = int(suffix[2:])
            entry["tp"] = tp
            if isinstance(model_workloads, list):
                entry["workloads"] = list(model_workloads)
            result.append(entry)
        return result
    return [{"name": m, "preset": m, "overrides": {}} for m in raw_models]


def _parse_cpt_pipelines(data: dict) -> list[dict]:
    """Extract CPT definitions, including target and per-model metadata."""
    if not data.get("__cpt"):
        return []

    pipelines = []
    for key, entry in data.items():
        if key.startswith("__") or not isinstance(entry, dict):
            continue
        pipelines.append({
            "key": key,
            "description": entry.get("__description", ""),
            "engine": entry.get("__engine", ""),
            "accelerator": entry.get("__accelerator", ""),
            "gpu_type": entry.get("__gpu_type", ""),
            "clusters": list(entry.get("__clusters", []) or []),
            "models": _parse_cpt_models(entry.get("__models", [])),
            "workloads": list(entry.get("__workloads", []) or []),
            "overrides": {
                k: v for k, v in entry.items()
                if not k.startswith("__")
            },
        })
    return pipelines


def _fetch_rhaiis_config_from_github(ref: str | None = None) -> dict:
    """Fetch and categorize rhaiis presets from the forge GitHub repo."""
    config_dir = f"{_RHAIIS_ORCHESTRATION}/config.d"
    presets_dir = f"{_RHAIIS_ORCHESTRATION}/presets.d"
    cpt_dir = f"{_RHAIIS_ORCHESTRATION}/cpt.d"

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
        models_data = _github_fetch_yaml(f"{config_dir}/models.yaml", ref=ref)
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

    cluster_gpu_types: dict[str, str] = {
        "hera": "h200",
        "zeus": "h200",
        "old-zeus": "h200",
        "b200": "b200",
        # Hearth exposes the MI355X target's AMD capacity as the generic
        # ``fournos/gpu-amd`` resource because the target does not expose a
        # model-specific GPU product label.
        "mi355x": "amd",
    }
    try:
        clusters_data = _github_fetch_yaml(f"{config_dir}/clusters.yaml", ref=ref)
        for key, val in clusters_data.items():
            if isinstance(val, dict):
                gpu = val.get("gpu_type", val.get("gpuType", val.get("gpu")))
                if gpu:
                    cluster_gpu_types[key] = str(gpu)
    except Exception as exc:
        logger.debug("No clusters.yaml in config.d, using defaults: %s", exc)

    engine_images: dict[str, dict[str, str]] = {}
    try:
        rhaiis_data = _github_fetch_yaml(f"{config_dir}/rhaiis.yaml", ref=ref)
        for ename, edata in (rhaiis_data.get("engines") or {}).items():
            if isinstance(edata, dict):
                for accel, img in (edata.get("images") or {}).items():
                    if isinstance(img, str):
                        engine_images.setdefault(ename, {})[accel] = img
    except Exception as exc:
        logger.debug("Failed to fetch rhaiis.yaml for engine defaults: %s", exc)

    workload_profiles: dict[str, dict] = {}
    try:
        workloads_data = _github_fetch_yaml(f"{config_dir}/workloads.yaml", ref=ref)
        for wk, wv in workloads_data.items():
            if isinstance(wv, dict):
                workload_profiles[wk] = wv
    except Exception as exc:
        logger.debug("Failed to fetch workloads.yaml: %s", exc)

    model_key_to_preset: dict[str, str] = {}
    cpt_pipelines: list[dict] = []

    preset_files = []
    for config_source_dir in (presets_dir, cpt_dir):
        try:
            preset_files.extend(_github_list_yamls(config_source_dir, ref=ref))
        except Exception as exc:
            logger.error("Failed to list %s from GitHub: %s", config_source_dir, exc)
    preset_files = sorted(set(preset_files))

    # Prefix-cache feature presets are exposed as a Run Settings toggle rather
    # than as standalone Quick Presets.  Keep their state so compound presets
    # such as benchmark-multi-turn-prefix-on can fill the toggle through
    # ``extends``.
    prefix_cache_preset_states: dict[str, bool] = {}

    # First pass: categorize simple presets and detect compound ones
    compound_presets: list[tuple[str, dict]] = []

    for file_path in preset_files:
        try:
            data = _github_fetch_yaml(file_path, ref=ref)
        except Exception as exc:
            logger.warning("Failed to fetch %s: %s", file_path, exc)
            continue

        if data.get("__cpt"):
            cpt_pipelines.extend(_parse_cpt_pipelines(data))
            continue

        for key, overrides in data.items():
            if key.startswith("__"):
                continue
            if not isinstance(overrides, dict):
                continue

            # These are user-selectable compound/feature presets rather than
            # individual accelerator, model, or workload options.
            if "rhaiis.engines.vllm.args.enable-prefix-caching" in overrides:
                prefix_cache_preset_states[key] = bool(
                    overrides["rhaiis.engines.vllm.args.enable-prefix-caching"]
                )
                continue
            if "rhaiis.engines.vllm.args.no-enable-prefix-caching" in overrides:
                prefix_cache_preset_states[key] = not bool(
                    overrides["rhaiis.engines.vllm.args.no-enable-prefix-caching"]
                )
                continue
            if "extends" in overrides:
                compound_presets.append((key, overrides))
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
                if not isinstance(wk, str):
                    # Composite benchmark presets may contain several
                    # workload keys; they are handled as quick presets.
                    compound_presets.append((key, overrides))
                    continue
                entry: dict[str, Any] = {
                    "key": key,
                    "label": key,
                    "workload_key": wk,
                    "overrides": dict(overrides),
                }
                profile = workload_profiles.get(wk)
                if profile:
                    entry["profile"] = profile
                categories["workloads"].append(entry)

    _SETTINGS_KEYS = {
        "tests.rhaiis.warmup": "warmup",
        "rhaiis.profiler.enabled": "profiler",
        "tests.rhaiis.slack_notify_always": "slack",
        "rhaiis.agent_analysis.enabled": "agent_analysis",
        "caliper.postprocess.csv_dashboard.enabled": "csv_dashboard",
        "rhaiis.compare_versions.enabled": "compare_versions",
        "tests.rhaiis.run_benchmark": "benchmark",
    }

    # Second pass: build quick presets with fill mappings
    for key, overrides in compound_presets:
        fills: dict[str, Any] = {}
        if "tests.rhaiis.model_key" in overrides:
            mk = overrides["tests.rhaiis.model_key"]
            fills["model"] = model_key_to_preset.get(mk, "")
        if "tests.rhaiis.workload_key" in overrides:
            wk = overrides["tests.rhaiis.workload_key"]
            if isinstance(wk, str):
                fills["workload"] = wk
        if "tests.rhaiis.version" in overrides:
            fills["version"] = overrides["tests.rhaiis.version"]
        for cfg_key, fill_key in _SETTINGS_KEYS.items():
            if cfg_key in overrides:
                fills[fill_key] = bool(overrides[cfg_key])

        # Resolve prefix-cache state from the feature preset extended by a
        # compound preset.  This keeps the Quick Preset useful while making
        # the actual setting visible and editable in Run Settings.
        prefix_cache = None
        if "rhaiis.engines.vllm.args.enable-prefix-caching" in overrides:
            prefix_cache = bool(
                overrides["rhaiis.engines.vllm.args.enable-prefix-caching"]
            )
        elif "rhaiis.engines.vllm.args.no-enable-prefix-caching" in overrides:
            prefix_cache = not bool(
                overrides["rhaiis.engines.vllm.args.no-enable-prefix-caching"]
            )
        else:
            extends = overrides.get("extends", [])
            if isinstance(extends, str):
                extends = [extends]
            for parent in extends:
                if parent in prefix_cache_preset_states:
                    prefix_cache = prefix_cache_preset_states[parent]
                    break
        if prefix_cache is not None:
            fills["prefix_caching"] = prefix_cache

        quick_entry = {
            "key": key,
            "label": key.replace("-", " ").replace("_", " ").title(),
            "fills": fills,
            "overrides": dict(overrides),
        }
        workload_keys = overrides.get("tests.rhaiis.workload_key")
        if isinstance(workload_keys, str):
            quick_entry["workload_keys"] = [workload_keys]
        elif isinstance(workload_keys, list):
            quick_entry["workload_keys"] = list(workload_keys)
        categories["quick_presets"].append(quick_entry)

    engine_defaults: dict[str, str] = {}
    for ename, accel_versions in engine_images.items():
        for accel, ver in accel_versions.items():
            engine_defaults[f"{accel}_{ename}"] = ver

    # The dashboard default for AMD runs is intentionally pinned here until
    # the corresponding Forge default is updated.  This value is submitted
    # as an explicit image override when an AMD/vLLM job is created.
    engine_defaults["amd_vllm"] = "vllm/vllm-openai-rocm:v0.26.0"

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
        local_dirs = [Path(__file__).resolve().parent.parent]
        if settings.forge_repo_path:
            local_dirs.append(Path(settings.forge_repo_path) / _RHAIIS_ORCHESTRATION / "cpt.d")
        local_files = []
        for local_dir in local_dirs:
            local_files.extend(local_dir.glob("cpt*.yaml"))
        for local_cpt in sorted(set(local_files)):
            try:
                with open(local_cpt) as f:
                    cpt_data = yaml.safe_load(f) or {}
                cpt_pipelines.extend(_parse_cpt_pipelines(cpt_data))
                logger.info("Loaded CPT pipeline(s) from local %s", local_cpt)
            except Exception as exc:
                logger.debug("Failed to load local CPT file %s: %s", local_cpt, exc)

    categories["cpt_pipelines"] = cpt_pipelines
    categories["forge_commit_sha"] = ref or ""

    return categories


_rhaiis_config_cache: dict | None = None
_rhaiis_config_cache_sha: str | None = None
_rhaiis_config_cache_checked_at = 0.0
_rhaiis_config_refresh_lock = asyncio.Lock()


def _is_complete_rhaiis_config(config: dict) -> bool:
    return bool(config.get("accelerators") and config.get("engines"))


async def _load_rhaiis_config(force_refresh: bool = False) -> dict:
    """Return cached Forge config, checking the default-branch SHA periodically."""
    global _rhaiis_config_cache
    global _rhaiis_config_cache_sha
    global _rhaiis_config_cache_checked_at

    ttl = max(1, settings.rhaiis_config_cache_ttl_seconds)
    now = time.monotonic()
    if (
        not force_refresh
        and _rhaiis_config_cache is not None
        and now - _rhaiis_config_cache_checked_at < ttl
    ):
        return _rhaiis_config_cache

    async with _rhaiis_config_refresh_lock:
        now = time.monotonic()
        if (
            not force_refresh
            and _rhaiis_config_cache is not None
            and now - _rhaiis_config_cache_checked_at < ttl
        ):
            return _rhaiis_config_cache

        latest_sha: str | None = None
        try:
            latest_sha = await asyncio.to_thread(_github_default_branch_sha)
        except (OSError, ValueError, RuntimeError) as exc:
            logger.warning("Failed to check Forge default-branch SHA: %s", exc)
            if _rhaiis_config_cache is not None and not force_refresh:
                # Keep serving the last good config and back off before retrying.
                _rhaiis_config_cache_checked_at = time.monotonic()
                return _rhaiis_config_cache

        if (
            not force_refresh
            and _rhaiis_config_cache is not None
            and latest_sha
            and latest_sha == _rhaiis_config_cache_sha
        ):
            _rhaiis_config_cache_checked_at = time.monotonic()
            return _rhaiis_config_cache

        try:
            result = await asyncio.to_thread(_fetch_rhaiis_config_from_github, latest_sha)
        except (OSError, ValueError, RuntimeError, yaml.YAMLError) as exc:
            logger.warning("Failed to refresh RHAIIS config from Forge: %s", exc)
            _rhaiis_config_cache_checked_at = time.monotonic()
            if _rhaiis_config_cache is not None:
                return _rhaiis_config_cache
            raise

        _rhaiis_config_cache_checked_at = time.monotonic()
        if _is_complete_rhaiis_config(result):
            _rhaiis_config_cache = result
            _rhaiis_config_cache_sha = latest_sha or result.get("forge_commit_sha") or None
            return _rhaiis_config_cache

        logger.warning("rhaiis config fetch returned incomplete data — not caching")
        return _rhaiis_config_cache if _rhaiis_config_cache is not None else result


@app.get("/api/rhaiis-config")
async def rhaiis_config():
    """Return categorized rhaiis preset options for the submit form."""
    return await _load_rhaiis_config()


@app.post("/api/rhaiis-config/refresh")
async def rhaiis_config_refresh():
    """Force-refresh the cached rhaiis config from GitHub."""
    result = await _load_rhaiis_config(force_refresh=True)
    return {"status": "ok", "accelerators": len(result.get("accelerators", [])),
            "engines": len(result.get("engines", [])),
            "models": len(result.get("models", [])),
            "forge_commit_sha": result.get("forge_commit_sha", "")}


def _parse_yaml_value(raw: str) -> Any:
    """Coerce a raw string from the config overrides textarea into a typed value."""
    import json as _json

    if raw.lower() == "true":
        return True
    if raw.lower() == "false":
        return False
    if raw in ("null", "~", ""):
        return None
    if raw.startswith("[") and raw.endswith("]"):
        try:
            return _json.loads(raw)
        except (ValueError, TypeError):
            pass
    if raw.startswith('"') and raw.endswith('"'):
        return raw[1:-1]
    if raw.startswith("'") and raw.endswith("'"):
        return raw[1:-1]
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        pass
    return raw


@app.post("/submit")
async def submit_job(
    request: Request,
    project: str = Form(...),
    cluster: str = Form(...),
    pipeline: str = Form("forge-test-only"),
    preset: str = Form(""),
    version: str = Form(""),
    owner: str = Form(""),
    exclusive: str = Form("false"),
    config_overrides_raw: str = Form(""),
    pull_sha: str = Form(""),
    use_latest_main: str = Form("false"),
    rhaiis_args: str = Form(""),
    rhaiis_version: str = Form(""),
    rhaiis_overrides: str = Form(""),
    priority: str = Form("manual"),
    gpu_type: str = Form(""),
    gpu_count: str = Form("1"),
    edit_source_job_name: str = Form(""),
):
    exclusive_bool = exclusive.lower() in ("true", "on", "1", "yes")
    edit_source_job = None
    edit_source_job_name = edit_source_job_name.strip()
    if edit_source_job_name:
        edit_source_job = await _get_job_for_rerun(edit_source_job_name)
        if edit_source_job is None:
            raise HTTPException(status_code=404, detail="Source job not found")

    config_overrides: dict[str, Any] = {}
    if config_overrides_raw.strip():
        if edit_source_job:
            try:
                parsed_overrides = yaml.safe_load(config_overrides_raw)
            except yaml.YAMLError as exc:
                raise HTTPException(status_code=400, detail=f"Invalid config overrides: {exc}") from exc
            if parsed_overrides is not None and not isinstance(parsed_overrides, dict):
                raise HTTPException(status_code=400, detail="Config overrides must be a YAML mapping")
            config_overrides = parsed_overrides or {}
        else:
            for line in config_overrides_raw.strip().splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if ":" in line:
                    k, v = line.split(":", 1)
                    config_overrides[k.strip()] = _parse_yaml_value(v.strip())

    if version:
        version_key = _get_version_config_key(project)
        config_overrides[version_key] = version

    display_name = f"{project} {preset}".strip()

    if project == "rhaiis" and rhaiis_args.strip():
        args = [a.strip() for a in rhaiis_args.split(",") if a.strip()]
        display_name = f"rhaiis-{cluster}-{'-'.join(args[:2])}" if args else f"rhaiis-{cluster}"
        if rhaiis_version.strip():
            config_overrides["tests.rhaiis.version"] = rhaiis_version.strip()
        if rhaiis_overrides.strip():
            import json as _json
            try:
                rh_ov = _json.loads(rhaiis_overrides)
                if isinstance(rh_ov, dict):
                    config_overrides.update(rh_ov)
            except (ValueError, TypeError):
                pass
    else:
        args = [preset] if preset else []

    generate_name = f"rhaiis-{cluster}-" if project == "rhaiis" else f"forge-{project}-"

    pull_sha = pull_sha.strip()
    use_latest_main_bool = use_latest_main.lower() in ("true", "on", "1", "yes")
    if project == "rhaiis":
        if use_latest_main_bool and pull_sha:
            raise HTTPException(
                status_code=400,
                detail="Choose either a pinned RHAIIS build source or latest main, not both.",
            )
        if not pull_sha and not use_latest_main_bool:
            raise HTTPException(
                status_code=400,
                detail="RHAIIS build source is required: choose a PR, commit SHA, release tag, or latest main.",
            )
        if use_latest_main_bool:
            # The Forge resolve step fetches this branch before running CI.
            pull_sha = "main"
    env: dict[str, str] = {}
    if pull_sha:
        env["PULL_PULL_SHA"] = pull_sha

    spec: dict[str, Any] = {
        "cluster": cluster,
        "displayName": display_name,
        "owner": owner or "fournos-dashboard",
        "pipeline": pipeline,
        "exclusive": exclusive_bool,
        "priority": priority,
        "executionEngine": {
            "forge": {
                "project": project,
                "args": args,
                "configOverrides": config_overrides,
            }
        },
    }

    try:
        gpu_count_int = int(gpu_count) if gpu_count.strip() else 1
    except ValueError:
        gpu_count_int = 1
    if gpu_type.strip():
        spec["hardware"] = {"gpuType": gpu_type.strip(), "gpuCount": gpu_count_int}

    if project == "rhaiis":
        spec["secretRefs"] = ["psap-forge-dashboard-s3", "psap-forge-notifications"]

    if edit_source_job:
        spec = _merge_edit_source_spec(edit_source_job, spec, project, env)
    elif env:
        spec["env"] = env
    spec["ttl"] = DEFAULT_FJOB_TTL

    body = {
        "apiVersion": f"{settings.fournos_api_group}/{settings.fournos_api_version}",
        "kind": "FournosJob",
        "metadata": {
            "generateName": generate_name,
            "namespace": settings.fournos_namespace,
        },
        "spec": spec,
    }

    try:
        created = await asyncio.to_thread(k8s_client.create_fournos_job, body)
    except Exception as exc:
        projects = discover_projects()
        retry_draft = None
        if edit_source_job:
            retry_draft = _job_to_edit_draft(edit_source_job_name, edit_source_job)
            retry_draft.update({
                "project": project,
                "cluster": cluster,
                "pipeline": pipeline,
                "preset": preset,
                "args": list(args),
                "owner": owner,
                "priority": priority,
                "exclusive": exclusive_bool,
                "pull_sha": pull_sha,
                "version": rhaiis_version if project == "rhaiis" else version,
                "gpu_type": gpu_type,
                "gpu_count": gpu_count,
                "config_overrides": config_overrides,
            })
        return _render(
            "submit_job.html",
            request=request,
            projects=projects,
            pipelines=list(settings.default_pipelines),
            fournos_namespace=settings.fournos_namespace,
            error=str(exc),
            edit_draft=retry_draft,
        )

    created_name = created.get("metadata", {}).get("name", generate_name)

    try:
        async with db.async_session() as session:
            async with session.begin():
                await db.upsert_job(
                    session,
                    name=created_name,
                    project=project,
                    preset=preset,
                    cluster=cluster,
                    pipeline=pipeline,
                    owner=owner or "fournos-dashboard",
                    status="Pending",
                    config_overrides=config_overrides,
                    fjob_spec=body.get("spec", {}),
                )
    except Exception as exc:
        logger.error("DB upsert failed for job %s (job was created in K8s): %s", created_name, exc)

    return RedirectResponse(url=f"/jobs/{created_name}", status_code=303)


@app.post("/api/submit-cpt")
async def submit_cpt(request: Request):
    """Submit a CPT pipeline — creates one FournosJob per model."""
    import json as _json

    payload = await request.json()
    models = payload.get("models", [])
    workloads = payload.get("workloads", [])
    accelerator: str = payload.get("accelerator", "nvidia")
    engine: str = payload.get("engine", "vllm")
    cluster: str = payload.get("cluster", "hera")
    pipeline: str = payload.get("pipeline", "forge-full")
    owner: str = payload.get("owner", "fournos-dashboard")
    priority: str = payload.get("priority", "manual")
    cpt_pipeline_key: str = payload.get("cpt_pipeline_key", "")
    version_label: str = payload.get("version_label", "")
    pull_sha: str = payload.get("pull_sha", "")
    use_latest_main = payload.get("use_latest_main", False)
    overrides: dict = payload.get("overrides", {})
    engine_version: str = payload.get("engine_version", "")

    if not isinstance(models, list) or not models:
        raise HTTPException(status_code=400, detail="models must be a non-empty list")
    if isinstance(workloads, str):
        workloads = [workloads]
    if not isinstance(workloads, list):
        raise HTTPException(status_code=400, detail="workloads must be a non-empty list")
    workloads = [str(workload).strip() for workload in workloads if str(workload).strip()]
    if not workloads:
        raise HTTPException(status_code=400, detail="models and workloads are required")
    if not cpt_pipeline_key:
        raise HTTPException(status_code=400, detail="A CPT pipeline must be selected")
    if not isinstance(overrides, dict):
        raise HTTPException(status_code=400, detail="overrides must be an object")

    if isinstance(use_latest_main, str):
        use_latest_main = use_latest_main.lower() in ("true", "on", "1", "yes")
    pull_sha = str(pull_sha or "").strip()
    if use_latest_main and pull_sha:
        raise HTTPException(
            status_code=400,
            detail="Choose either a pinned RHAIIS build source or latest main, not both.",
        )
    if not pull_sha and not use_latest_main:
        raise HTTPException(
            status_code=400,
            detail="RHAIIS build source is required: provide a commit SHA, release tag, or latest main.",
        )
    if use_latest_main:
        # The Forge resolve step fetches this branch before running CI.
        pull_sha = "main"

    config = await _load_rhaiis_config()
    cpt_pipeline = next(
        (p for p in config.get("cpt_pipelines", []) if p.get("key") == cpt_pipeline_key),
        None,
    )
    if cpt_pipeline_key and cpt_pipeline is None:
        raise HTTPException(status_code=400, detail=f"Unknown CPT pipeline: {cpt_pipeline_key}")
    if cpt_pipeline:
        allowed_clusters = cpt_pipeline.get("clusters", [])
        if allowed_clusters and cluster not in allowed_clusters:
            raise HTTPException(
                status_code=400,
                detail=f"Cluster {cluster} is not valid for CPT pipeline {cpt_pipeline_key}; "
                f"choose one of: {', '.join(allowed_clusters)}",
            )
        if cpt_pipeline.get("accelerator") and accelerator != cpt_pipeline["accelerator"]:
            raise HTTPException(
                status_code=400,
                detail=f"Accelerator {accelerator} is not valid for CPT pipeline {cpt_pipeline_key}",
            )
        if cpt_pipeline.get("engine") and engine != cpt_pipeline["engine"]:
            raise HTTPException(
                status_code=400,
                detail=f"Engine {engine} is not valid for CPT pipeline {cpt_pipeline_key}",
            )
    model_entries = {m["key"]: m for m in config.get("models", [])}
    cluster_entries = {c["key"]: c for c in config.get("clusters", [])}
    gpu_type = cluster_entries.get(cluster, {}).get("gpu_type", "h200")

    submission_items = []
    for position, model_item in enumerate(models):
        if isinstance(model_item, dict):
            model_preset = model_item.get("preset", model_item.get("name", ""))
            model_label = model_item.get("name", model_preset)
            per_model_overrides = model_item.get("overrides", {})
        else:
            model_preset = model_item
            model_label = model_item
            per_model_overrides = {}

        model_entry = model_entries.get(model_preset, {})
        cpt_tp = model_item.get("tp") if isinstance(model_item, dict) else None
        gpu_count = cpt_tp or model_entry.get("gpu_count", 1)
        model_workloads = model_item.get("workloads") if isinstance(model_item, dict) else None
        if isinstance(model_workloads, str):
            model_workloads = [model_workloads]
        if not isinstance(model_workloads, list) or not model_workloads:
            model_workloads = workloads
        model_workloads = [
            str(workload).strip()
            for workload in model_workloads
            if str(workload).strip()
        ] or workloads
        submission_items.append({
            "id": str(uuid4()),
            "position": position,
            "model_item": model_item,
            "model_preset": model_preset,
            "model_label": model_label,
            "per_model_overrides": per_model_overrides,
            "cpt_tp": cpt_tp,
            "gpu_count": gpu_count,
            "model_workloads": list(model_workloads),
        })

    run_id = str(uuid4())
    try:
        async with db.async_session() as session:
            async with session.begin():
                await db.create_cpt_run(
                    session,
                    id=run_id,
                    project="rhaiis",
                    pipeline_key=cpt_pipeline_key or "unspecified",
                    forge_pipeline=pipeline,
                    version_label=version_label,
                    forge_source=pull_sha,
                    owner=owner,
                    cluster=cluster,
                    accelerator=accelerator,
                    engine=engine,
                    engine_version=engine_version,
                    run_metadata={
                        "priority": priority,
                        "requested_workloads": list(workloads),
                    },
                )
                for item in submission_items:
                    await db.upsert_cpt_run_job(
                        session,
                        id=item["id"],
                        run_id=run_id,
                        position=item["position"],
                        job_name=None,
                        model_name=str(item["model_label"]),
                        model_preset=str(item["model_preset"]),
                        workloads=item["model_workloads"],
                        submission_status="Submitting",
                        status="Pending",
                        message="",
                    )
    except Exception as exc:
        logger.exception("Failed to initialize CPT run tracking")
        raise HTTPException(
            status_code=503,
            detail="Could not initialize CPT run tracking; no FournosJobs were submitted.",
        ) from exc

    results = []
    for item in submission_items:
        model_item = item["model_item"]
        model_preset = item["model_preset"]
        model_label = item["model_label"]
        per_model_overrides = item["per_model_overrides"]
        model_entry = model_entries.get(model_preset, {})
        cpt_tp = item["cpt_tp"]
        gpu_count = item["gpu_count"]
        model_workloads = item["model_workloads"]

        args = [accelerator, engine, cluster, model_preset]

        job_overrides: dict[str, Any] = {}
        job_overrides.update(overrides)
        job_overrides.update(per_model_overrides)
        if cpt_tp is not None:
            tp_key = f"rhaiis.engines.{engine}.args.tensor-parallel-size"
            job_overrides.setdefault(tp_key, cpt_tp)
        job_overrides["tests.rhaiis.workload_keys"] = model_workloads
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
            "ttl": DEFAULT_FJOB_TTL,
            "exclusive": False,
            "priority": priority,
            "hardware": {"gpuType": gpu_type, "gpuCount": gpu_count},
            "secretRefs": ["psap-forge-dashboard-s3", "psap-forge-notifications"],
            "executionEngine": {
                "forge": {
                    "project": "rhaiis",
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
                "annotations": {
                    "fournos.dev/cpt-run-id": run_id,
                    "fournos.dev/cpt-run-job-id": item["id"],
                    "fournos.dev/cpt-run-position": str(item["position"]),
                    "fournos.dev/cpt-model-name": str(model_label),
                    "fournos.dev/cpt-model-preset": str(model_preset),
                },
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
                            project="rhaiis",
                            preset=f"cpt-{model_preset}",
                            cluster=cluster,
                            pipeline=pipeline,
                            owner=owner,
                            config_overrides=job_overrides,
                            fjob_spec=body.get("spec", {}),
                        )
                        await db.upsert_cpt_run_job(
                            session,
                            id=item["id"],
                            run_id=run_id,
                            position=item["position"],
                            job_name=created_name,
                            model_name=str(model_label),
                            model_preset=str(model_preset),
                            workloads=model_workloads,
                            submission_status="Created",
                            message="",
                        )
            except Exception as exc:
                logger.error("DB upsert failed for CPT job %s: %s", created_name, exc)
        except Exception as exc:
            results.append({"model": model_label, "error": str(exc), "status": "failed"})
            try:
                async with db.async_session() as session:
                    async with session.begin():
                        await db.upsert_cpt_run_job(
                            session,
                            id=item["id"],
                            run_id=run_id,
                            position=item["position"],
                            job_name=None,
                            model_name=str(model_label),
                            model_preset=str(model_preset),
                            workloads=model_workloads,
                            submission_status="Failed",
                            status="Failed",
                            message=str(exc)[:4000],
                        )
            except Exception as tracking_exc:
                logger.error(
                    "Failed to record CPT submission failure for %s: %s",
                    model_label,
                    tracking_exc,
                )

    return {"status": "ok", "run_id": run_id, "jobs": results, "total": len(results)}


async def _load_cpt_run_views(project: str = "") -> tuple[list[str], list[dict[str, Any]]]:
    async with db.async_session() as session:
        projects = await db.list_cpt_run_projects(session)
        runs, jobs_by_run = await db.list_cpt_runs_with_jobs(
            session,
            project=project or None,
        )
    projects = sorted(set(projects) | {"rhaiis"})
    views = [
        _cpt_run_view(run, list(jobs_by_run.get(run.id, [])))
        for run in runs
    ]
    return projects, views


async def _load_cpt_run_view(run_id: str) -> dict[str, Any] | None:
    async with db.async_session() as session:
        run = await db.get_cpt_run(session, run_id)
        if run is None:
            return None
        jobs = await db.list_cpt_run_jobs(session, run_id)
    return _cpt_run_view(run, list(jobs))


@app.get("/cpt-jobs", response_class=HTMLResponse)
async def cpt_jobs_page(
    request: Request,
    project: str = Query("rhaiis"),
):
    projects, runs = await _load_cpt_run_views(project)
    return _render(
        "cpt_jobs.html",
        request=request,
        projects=projects,
        project=project,
        runs=runs,
    )


@app.get("/api/cpt-jobs/table", response_class=HTMLResponse)
async def cpt_jobs_table(project: str = Query("rhaiis")):
    _, runs = await _load_cpt_run_views(project)
    return _render("components/cpt_runs_table_body.html", runs=runs, project=project)


@app.get("/cpt-jobs/{run_id}", response_class=HTMLResponse)
async def cpt_run_detail(request: Request, run_id: str):
    run_view = await _load_cpt_run_view(run_id)
    if run_view is None:
        raise HTTPException(status_code=404, detail="CPT run not found")
    return _render("cpt_run_detail.html", request=request, run_view=run_view)


@app.get("/api/cpt-jobs/{run_id}/matrix", response_class=HTMLResponse)
async def cpt_run_matrix(run_id: str):
    run_view = await _load_cpt_run_view(run_id)
    if run_view is None:
        raise HTTPException(status_code=404, detail="CPT run not found")
    return _render("components/cpt_run_matrix.html", run_view=run_view)


# ---------------------------------------------------------------------------
# Routes: Schedules
# ---------------------------------------------------------------------------

@app.get("/schedules", response_class=HTMLResponse)
async def schedules_list(request: Request):
    cronjobs = await asyncio.to_thread(k8s_client.list_managed_cronjobs)
    projects = discover_projects()
    return _render(
        "schedules.html",
        cronjobs=cronjobs,
        projects=projects,
        pipelines=list(settings.default_pipelines),
    )


@app.get("/schedules/{name}/runs", response_class=HTMLResponse)
async def schedule_runs(request: Request, name: str):
    """Show all jobs triggered by a specific schedule."""
    async with db.async_session() as session:
        jobs = await db.list_jobs_by_schedule(session, name)
    runs = []
    for j in jobs:
        runs.append({
            "name": j.name,
            "status": j.status,
            "preset": j.preset,
            "trigger_type": j.trigger_type or "scheduled",
            "duration_seconds": j.duration_seconds,
            "mlflow_url": j.mlflow_url,
            "created_at": j.created_at.isoformat() if j.created_at else "",
        })
    return _render("schedule_runs.html", schedule_name=name, runs=runs)


@app.post("/schedules")  # handles both create and edit
async def create_schedule(
    request: Request,
    name: str = Form(...),
    project: str = Form(...),
    cluster: str = Form(...),
    pipeline: str = Form("forge-test-only"),
    preset: str = Form(""),
    cron_expr: str = Form(...),
    image_source: str = Form(""),
    owner: str = Form(""),
    resolver_script: str = Form(""),
    resolver_image: str = Form(""),
    resolver_filename: str = Form(""),
    edit_target: str = Form(""),
):
    try:
        if edit_target and edit_target != name:
            await asyncio.to_thread(
                k8s_client.create_cronjob,
                name=name,
                schedule=cron_expr,
                project=project,
                cluster=cluster,
                pipeline=pipeline,
                preset=preset,
                image=image_source,
                owner=owner,
                resolver_script=resolver_script.strip().replace("\r\n", "\n").replace("\r", "\n"),
                resolver_image=resolver_image.strip(),
                resolver_filename=resolver_filename.strip(),
            )
            try:
                await asyncio.to_thread(k8s_client.delete_cronjob, edit_target)
            except Exception as del_exc:
                logger.warning("Failed to delete old schedule %s after replacement: %s", edit_target, del_exc)
        else:
            if edit_target:
                await asyncio.to_thread(k8s_client.delete_cronjob, edit_target)
            await asyncio.to_thread(
                k8s_client.create_cronjob,
                name=name,
                schedule=cron_expr,
                project=project,
                cluster=cluster,
                pipeline=pipeline,
                preset=preset,
                image=image_source,
                owner=owner,
                resolver_script=resolver_script.strip().replace("\r\n", "\n").replace("\r", "\n"),
                resolver_image=resolver_image.strip(),
                resolver_filename=resolver_filename.strip(),
            )

        return RedirectResponse(url="/schedules", status_code=303)
    except Exception as exc:
        cronjobs = await asyncio.to_thread(k8s_client.list_managed_cronjobs)
        projects = discover_projects()
        return _render(
            "schedules.html",
            cronjobs=cronjobs,
            projects=projects,
            pipelines=list(settings.default_pipelines),
            error=str(exc),
        )


@app.post("/api/schedules/{name}/toggle")
async def toggle_schedule(name: str):
    cj = await asyncio.to_thread(k8s_client.get_managed_cronjob, name)
    if cj is None:
        raise HTTPException(404, "Schedule not found")
    await asyncio.to_thread(k8s_client.patch_cronjob_suspend, name, not cj["suspend"])
    return {"status": "ok"}


@app.get("/api/schedules/{name}/resolver")
async def get_resolver_script(name: str):
    """Return the resolver script content for a schedule."""
    cj = await asyncio.to_thread(k8s_client.get_managed_cronjob, name)
    if cj is None:
        raise HTTPException(404, "Schedule not found")
    cm_name = cj.get("resolver_configmap", "")
    if not cm_name:
        raise HTTPException(404, "No resolver script configured for this schedule")
    filename, content = await asyncio.to_thread(k8s_client.get_resolver_script, cm_name)
    if not content:
        raise HTTPException(404, "Resolver ConfigMap not found")
    return {"filename": filename, "content": content}


@app.post("/api/schedules/{name}/trigger")
async def trigger_schedule(name: str):
    """Manually trigger a CronJob by creating a one-off Job from it."""
    try:
        job = await asyncio.to_thread(k8s_client.trigger_cronjob, name)
        return {"status": "ok", "job_name": job}
    except Exception as exc:
        raise HTTPException(500, str(exc))


@app.post("/api/schedules/{name}/delete")
async def delete_schedule(name: str):
    try:
        await asyncio.to_thread(k8s_client.delete_cronjob, name)
        return {"status": "ok"}
    except Exception as exc:
        raise HTTPException(500, str(exc))


# ---------------------------------------------------------------------------
# Conversion helpers
# ---------------------------------------------------------------------------

def _collect_clusters(live_jobs: list[dict]) -> list[str]:
    """Collect unique cluster names from live jobs."""
    clusters = set()
    for j in live_jobs:
        c = j.get("spec", {}).get("cluster", "")
        if c:
            clusters.add(c)
    return sorted(clusters)


def _db_job_to_dict(job: db.Job) -> dict:
    """Convert a DB Job row to a dict suitable for the history table template."""
    return {
        "name": job.name,
        "project": job.project,
        "preset": job.preset,
        "cluster": job.cluster,
        "pipeline": job.pipeline,
        "owner": job.owner,
        "phase": job.status,
        "message": job.message,
        "created_at": job.created_at.isoformat() if job.created_at else "",
        "completed_at": job.completed_at.isoformat() if job.completed_at else "",
        "duration_seconds": job.duration_seconds,
        "mlflow_url": job.mlflow_url,
        "error_message": job.error_message,
        "triggered_by_schedule": job.triggered_by_schedule,
        "trigger_type": job.trigger_type or "manual",
        "source": "history",
    }


def _db_job_to_fjob_dict(job: db.Job) -> dict:
    """Convert a DB Job row to a FournosJob-like dict for the detail template."""
    spec = job.fjob_spec or {}
    status = job.fjob_status or {}

    forge = spec.get("executionEngine", {}).get("forge", {})
    if not forge:
        forge = {"project": job.project, "args": job.preset.split() if job.preset else [], "configOverrides": job.config_overrides or {}}
        spec.setdefault("executionEngine", {})["forge"] = forge

    spec.setdefault("cluster", job.cluster)
    spec.setdefault("pipeline", job.pipeline)
    spec.setdefault("owner", job.owner)
    spec.setdefault("displayName", f"{job.project} {job.preset}".strip())
    spec.setdefault("exclusive", True)
    spec.setdefault("env", {})
    spec.setdefault("secretRefs", [])

    status.setdefault("phase", job.status)
    status.setdefault("message", job.message)
    status.setdefault("conditions", [])

    return {
        "metadata": {
            "name": job.name,
            "namespace": settings.fournos_namespace,
            "creationTimestamp": job.created_at.isoformat() if job.created_at else "",
            "uid": job.id,
        },
        "spec": spec,
        "status": status,
        "_source": "history",
        "_duration_seconds": job.duration_seconds,
        "_mlflow_url": job.mlflow_url,
        "_ci_artifacts_url": job.ci_artifacts_url,
    }


_PROJECT_VERSION_KEYS: dict[str, str] = {
    "mcp_gateway": "infrastructure.mcp_gateway_version",
}


def _get_version_config_key(project: str) -> str:
    """Return the configOverrides key used to pass the version for a project."""
    return _PROJECT_VERSION_KEYS.get(project, "infrastructure.version")


def sanitize_job_name(prefix: str) -> str:
    """Generate a K8s-safe job name with timestamp."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    name = f"{prefix}-{ts}".lower()
    name = re.sub(r"[^a-z0-9-]", "-", name)
    name = re.sub(r"-+", "-", name).strip("-")
    return name[:63]
