"""GitHub API helpers for the Forge repository (public, no token needed)."""

from __future__ import annotations

import json
import urllib.request

import yaml

from app.config import settings


def fetch_yaml(path: str) -> dict:
    """Fetch a single YAML file from the forge GitHub repo and return parsed content."""
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


def list_yamls(directory: str) -> list[str]:
    """List .yaml file paths in a forge GitHub repo directory."""
    url = f"https://api.github.com/repos/{settings.forge_github_repo}/contents/{directory}"
    req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        items = json.loads(resp.read())

    return sorted(
        item["path"] for item in items
        if isinstance(item, dict) and item.get("name", "").endswith(".yaml")
    )


def fetch_open_prs() -> list[dict]:
    """Fetch open pull requests from the Forge GitHub repo."""
    url = f"https://api.github.com/repos/{settings.forge_github_repo}/pulls?state=open&per_page=100"
    req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        prs = json.loads(resp.read())

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
