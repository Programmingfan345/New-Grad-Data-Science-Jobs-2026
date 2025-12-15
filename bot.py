import json
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any

import requests

# ✅ Correct data source (raw JSON)
JOBS_URL = "https://raw.githubusercontent.com/zapplyjobs/New-Grad-Data-Science-Jobs-2026/main/jobboard/src/data/transformed_jobs.json"
STATE_FILE = "seen_jobs.json"

DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
if not DISCORD_WEBHOOK:
    print("Missing DISCORD_WEBHOOK_URL env var.")
    sys.exit(1)

MAX_POSTS_PER_RUN = int(os.environ.get("MAX_POSTS_PER_RUN", "1"))

# ---- Analyst filtering ----
# Include these keywords (case-insensitive) in the title
INCLUDE_TITLE_KEYWORDS = [
    "data analyst",
    "business analyst",
    "product analyst",
    "analytics",
    "bi analyst",
    "business intelligence",
    "reporting analyst",
    "insights analyst",
    "data science",
    "data scientist",
    "science",
]

# Exclude obvious non-analyst roles
EXCLUDE_TITLE_KEYWORDS = [
    "software engineer",
    "software development",
    "developer",
    "full stack",
    "frontend",
    "back end",
    "backend",
    "devops",
    "site reliability",
    "sre",
    "platform engineer",
    "ml engineer",
    "machine learning engineer",
    "data engineer",
    "cloud engineer",
    "security engineer",
    "product manager",
    "program manager",
    "director",
    "sr ",
    "senior ",
    "principal",
    "staff",
    "lead",
    "manager",
    "intern",  # optional; remove if you want internships too
]

def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).lower()

def load_seen() -> set[str]:
    if not os.path.exists(STATE_FILE):
        return set()
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return set(map(str, data))
    except Exception:
        pass
    return set()

def save_seen(seen: set[str]) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(sorted(seen), f, indent=2)

def fetch_jobs() -> list[dict[str, Any]]:
    r = requests.get(JOBS_URL, timeout=45)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        return []
    return data

def is_analyst_role(title: str) -> bool:
    t = norm(title)

    # must include at least one include keyword
    if not any(k in t for k in INCLUDE_TITLE_KEYWORDS):
        return False

    # exclude if any exclude keyword appears
    if any(k in t for k in EXCLUDE_TITLE_KEYWORDS):
        return False

    return True

def stable_job_id(job: dict[str, Any]) -> str:
    # Use fields that are stable; apply link is usually unique.
    employer = norm(str(job.get("employer_name", "")))
    title = norm(str(job.get("job_title", "")))
    city = norm(str(job.get("job_city", "")))
    state = norm(str(job.get("job_state", "")))
    link = norm(str(job.get("job_apply_link", "")))
    posted = norm(str(job.get("job_posted_at", "")))

    return f"{employer}||{title}||{city}||{state}||{link}||{posted}"

def format_location(job: dict[str, Any]) -> str:
    city = (job.get("job_city") or "").strip()
    state = (job.get("job_state") or "").strip()
    if city and state:
        return f"{city}, {state}"
    if city:
        return city
    if state:
        return state
    return "Unknown"

def post_embed(job: dict[str, Any]) -> None:
    employer = (job.get("employer_name") or "Unknown Company").strip()
    title = (job.get("job_title") or "Analyst Role").strip()
    loc = format_location(job)
    age = (job.get("job_posted_at") or "N/A").strip()
    url = (job.get("job_apply_link") or "").strip() or "https://github.com/zapplyjobs/New-Grad-Data-Science-Jobs-2026"

    embed = {
        "title": f"{employer} — {title}",
        "url": url,
        "fields": [
            {"name": "Source", "value": "Zapply (New-Grad-Data-Science-Jobs-2026)", "inline": False},
            {"name": "Location", "value": f"📍 {loc}", "inline": True},
            {"name": "Age", "value": age, "inline": True},
        ],
        "footer": {"text": "Keep pushing — you’ve got this!"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    payload = {"embeds": [embed]}
    resp = requests.post(DISCORD_WEBHOOK, json=payload, timeout=45)
    resp.raise_for_status()

def main():
    seen = load_seen()
    jobs = fetch_jobs()

    # Filter to analyst-type roles only
    analyst_jobs = jobs  # FORCE TEST: bypass analyst filter

    # New ones only
    new_jobs = []
    for j in analyst_jobs:
        jid = stable_job_id(j)
        new_jobs.append((jid, j))

    if not new_jobs:
        print("No new analyst jobs found.")
        return

    # Post a few per run to prevent spam
    posted = 0
    for jid, job in new_jobs[:MAX_POSTS_PER_RUN]:
        post_embed(job)
        seen.add(jid)
        posted += 1

    save_seen(seen)
    print(f"Posted {posted} new analyst job(s).")

if __name__ == "__main__":
    main()
