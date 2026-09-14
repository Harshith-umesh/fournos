"""Fournos Launcher Dashboard -- production FastAPI application."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from fastapi import FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader

from app import db, k8s_client, watcher
from app.config import settings
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
    to_fjob_yaml=_to_fjob_yaml,
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
}


def _render(template_name: str, **context: Any) -> HTMLResponse:
    context.setdefault("active_nav", _NAV_MAP.get(template_name, ""))
    tpl = _jinja_env.get_template(template_name)
    return HTMLResponse(tpl.render(**context))


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




# ---------------------------------------------------------------------------
# Routes: Submit Job
# ---------------------------------------------------------------------------

@app.get("/submit", response_class=HTMLResponse)
async def submit_form(request: Request):
    projects = discover_projects()
    return _render(
        "submit_job.html",
        projects=projects,
        pipelines=list(settings.default_pipelines),
        fournos_namespace=settings.fournos_namespace,
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


_RHAIIS_ORCHESTRATION = "projects/rhaiis/orchestration"


def _github_fetch_yaml(path: str) -> dict:
    """Fetch a single YAML file from the forge GitHub repo and return parsed content."""
    import urllib.request
    import json as _json

    url = f"https://api.github.com/repos/{settings.forge_github_repo}/contents/{path}"
    req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        meta = _json.loads(resp.read())

    download_url = meta.get("download_url", "")
    if not download_url:
        return {}

    raw_req = urllib.request.Request(download_url)
    with urllib.request.urlopen(raw_req, timeout=15) as resp:
        return yaml.safe_load(resp.read()) or {}


def _github_list_yamls(directory: str) -> list[str]:
    """List .yaml file paths in a forge GitHub repo directory."""
    import urllib.request
    import json as _json

    url = f"https://api.github.com/repos/{settings.forge_github_repo}/contents/{directory}"
    req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        items = _json.loads(resp.read())

    return sorted(
        item["path"] for item in items
        if isinstance(item, dict) and item.get("name", "").endswith(".yaml")
    )


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
            if isinstance(ov, dict):
                tp = ov.pop("__tp", None)
                entry["overrides"] = ov
            if tp is None and suffix.startswith("tp") and suffix[2:].isdigit():
                tp = int(suffix[2:])
            entry["tp"] = tp
            result.append(entry)
        return result
    return [{"name": m, "preset": m, "overrides": {}} for m in raw_models]


def _fetch_rhaiis_config_from_github() -> dict:
    """Fetch and categorize rhaiis presets from the forge GitHub repo."""
    config_dir = f"{_RHAIIS_ORCHESTRATION}/config.d"
    presets_dir = f"{_RHAIIS_ORCHESTRATION}/presets.d"

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
        models_data = _github_fetch_yaml(f"{config_dir}/models.yaml")
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
    }
    try:
        clusters_data = _github_fetch_yaml(f"{config_dir}/clusters.yaml")
        for key, val in clusters_data.items():
            if isinstance(val, dict):
                gpu = val.get("gpu_type", val.get("gpuType", val.get("gpu")))
                if gpu:
                    cluster_gpu_types[key] = str(gpu)
    except Exception as exc:
        logger.debug("No clusters.yaml in config.d, using defaults: %s", exc)

    engine_images: dict[str, dict[str, str]] = {}
    try:
        rhaiis_data = _github_fetch_yaml(f"{config_dir}/rhaiis.yaml")
        for ename, edata in (rhaiis_data.get("engines") or {}).items():
            if isinstance(edata, dict):
                for accel, img in (edata.get("images") or {}).items():
                    if isinstance(img, str):
                        engine_images.setdefault(ename, {})[accel] = img
    except Exception as exc:
        logger.debug("Failed to fetch rhaiis.yaml for engine defaults: %s", exc)

    workload_profiles: dict[str, dict] = {}
    try:
        workloads_data = _github_fetch_yaml(f"{config_dir}/workloads.yaml")
        for wk, wv in workloads_data.items():
            if isinstance(wv, dict):
                workload_profiles[wk] = wv
    except Exception as exc:
        logger.debug("Failed to fetch workloads.yaml: %s", exc)

    model_key_to_preset: dict[str, str] = {}
    cpt_pipelines: list[dict] = []

    try:
        preset_files = _github_list_yamls(presets_dir)
    except Exception as exc:
        logger.error("Failed to list presets.d from GitHub: %s", exc)
        preset_files = []

    # First pass: categorize simple presets and detect compound ones
    compound_presets: list[tuple[str, dict]] = []

    for file_path in preset_files:
        try:
            data = _github_fetch_yaml(file_path)
        except Exception as exc:
            logger.warning("Failed to fetch %s: %s", file_path, exc)
            continue

        if data.get("__cpt"):
            for key, entry in data.items():
                if key.startswith("__") or not isinstance(entry, dict):
                    continue
                raw_models = entry.get("__models", [])
                models_list = _parse_cpt_models(raw_models)
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

            # These are user-selectable compound/feature presets rather than
            # individual accelerator, model, or workload options.
            if "extends" in overrides or any(
                key in overrides
                for key in (
                    "rhaiis.engines.vllm.args.enable-prefix-caching",
                    "rhaiis.engines.vllm.args.no-enable-prefix-caching",
                )
            ):
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
        local_dir = Path(__file__).resolve().parent.parent
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
                    models_list = _parse_cpt_models(raw_models)
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


_rhaiis_config_cache: dict | None = None


@app.get("/api/rhaiis-config")
async def rhaiis_config():
    """Return categorized rhaiis preset options for the submit form."""
    global _rhaiis_config_cache
    if _rhaiis_config_cache is None:
        result = await asyncio.to_thread(_fetch_rhaiis_config_from_github)
        if result.get("accelerators") and result.get("engines"):
            _rhaiis_config_cache = result
        else:
            logger.warning("rhaiis config fetch returned incomplete data — not caching")
            return result
    return _rhaiis_config_cache


@app.post("/api/rhaiis-config/refresh")
async def rhaiis_config_refresh():
    """Force-refresh the cached rhaiis config from GitHub."""
    global _rhaiis_config_cache
    _rhaiis_config_cache = None
    result = await asyncio.to_thread(_fetch_rhaiis_config_from_github)
    if result.get("accelerators") and result.get("engines"):
        _rhaiis_config_cache = result
    return {"status": "ok", "accelerators": len(result.get("accelerators", [])),
            "engines": len(result.get("engines", [])),
            "models": len(result.get("models", []))}


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
):
    exclusive_bool = exclusive.lower() in ("true", "on", "1", "yes")

    config_overrides: dict[str, Any] = {}
    if config_overrides_raw.strip():
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
    except Exception as exc:
        projects = discover_projects()
        return _render(
            "submit_job.html",
            projects=projects,
            pipelines=list(settings.default_pipelines),
            fournos_namespace=settings.fournos_namespace,
            error=str(exc),
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
    use_latest_main = payload.get("use_latest_main", False)
    overrides: dict = payload.get("overrides", {})
    engine_version: str = payload.get("engine_version", "")

    if not models or not workloads:
        raise HTTPException(status_code=400, detail="models and workloads are required")

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

    config = _rhaiis_config_cache or await asyncio.to_thread(_fetch_rhaiis_config_from_github)
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
                            status="Pending",
                            config_overrides=job_overrides,
                            fjob_spec=body.get("spec", {}),
                        )
            except Exception as exc:
                logger.error("DB upsert failed for CPT job %s: %s", created_name, exc)
        except Exception as exc:
            results.append({"model": model_label, "error": str(exc), "status": "failed"})

    return {"status": "ok", "jobs": results, "total": len(results)}


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
