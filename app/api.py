import io
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List
from collections import defaultdict

import aiohttp
import matplotlib.pyplot as plt
from fastapi import FastAPI, HTTPException, Query, status
from fastapi.responses import Response, JSONResponse

from .storage import store
from .metrics import avg_pr_interval, counts_by_type
from .collectors import attach_to, GITHUB_EVENTS_URL, INTERESTING, _headers, fetch_repo_events

def _parse_iso(s: str) -> datetime:
    """
    Parse an ISO8601 string ending in Z into a UTC datetime.
    Example: "2025-05-30T12:34:56Z"
    """
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)

app = FastAPI(title="GitHub Events Monitor")
attach_to(app)


# ----------------------------------------
# 1) repo endpoints
# ----------------------------------------

@app.get("/metrics/{owner}/{repo}/pr-interval", response_model=Dict[str, float])
def get_pr_interval(owner: str, repo: str):
    """
    Calculate the average time between pull requests for a given repository.
    Returns the average interval in seconds.
    """
    repo_name = f"{owner}/{repo}".lower()
    
    # Get all PR events for the repo
    events = store.get_events_for_repo(repo_name)
    
    # Filter for PR open events and sort by timestamp
    pr_events = [
        e for e in events
        if e["type"] == "PullRequestEvent" and e.get("payload", {}).get("action") == "opened"
    ]
    
    if len(pr_events) < 2:
        raise HTTPException(
            status_code=404,
            detail="Need at least 2 PRs to calculate average interval"
        )
    
    # Sort by timestamp
    pr_events.sort(key=lambda e: e["created_at"])
    
    # Calculate intervals between successive PRs
    intervals = []
    for i in range(1, len(pr_events)):
        prev_time = _parse_iso(pr_events[i-1]["created_at"])
        curr_time = _parse_iso(pr_events[i]["created_at"])
        interval = (curr_time - prev_time).total_seconds()
        intervals.append(interval)
    
    # Calculate average interval
    avg_interval = sum(intervals) / len(intervals)
    return {"average_seconds": avg_interval}


@app.get("/metrics/event-counts", response_model=Dict[str, int])
async def get_event_counts(
    owner: str | None = None,
    repo: str | None = None,
    offset: int = Query(10, description="Minutes to look back")
):
    """
    Return the total number of events grouped by event type for events created
    in the last 'offset' minutes. If owner and repo are provided, returns counts
    for that specific repository. Otherwise, returns counts across all repositories.
    """
    if owner and repo:
        repo_name = f"{owner}/{repo}".lower()
        # First check our stored events
        counts = store.get_events_by_type(repo_name, minutes=offset)
        
        # If we don't have any events, try fetching directly from GitHub
        if not counts:
            events = await fetch_repo_events(owner, repo)
            # Filter by time
            since = datetime.now(timezone.utc) - timedelta(minutes=offset)
            filtered_events = [
                e for e in events
                if _parse_iso(e["created_at"]) >= since
            ]
            # Count by type
            counts = defaultdict(int)
            for event in filtered_events:
                counts[event["type"]] += 1
            counts = dict(counts)
    else:
        # Get events from all repositories
        all_events = store.recent(offset)
        counts = defaultdict(int)
        for event in all_events:
            counts[event["type"]] += 1
        counts = dict(counts)
    
    return counts


@app.get("/viz/{owner}/{repo}/pr-metrics.png")
async def visualize_pr_metrics(
    owner: str,
    repo: str,
    days: int = Query(30, ge=1, le=365, description="Days to analyze")
):
    """
    Generate a visualization of PR metrics including:
    1. PR creation timeline
    2. PR interval distribution
    3. PR creation time heatmap (hour of day vs day of week)
    """
    repo_name = f"{owner}/{repo}".lower()
    since = datetime.now(timezone.utc) - timedelta(days=days)
    
    # First try stored events
    events = store.get_events_for_repo(repo_name, since=since)
    
    # If we don't have enough events, fetch directly from GitHub
    if not events:
        events = await fetch_repo_events(owner, repo)
        # Filter by time
        events = [
            e for e in events
            if _parse_iso(e["created_at"]) >= since
        ]
    
    # Filter for PR events
    pr_events = [
        e for e in events
        if e["type"] == "PullRequestEvent" and e.get("payload", {}).get("action") == "opened"
    ]
    
    if not pr_events:
        raise HTTPException(
            status_code=404,
            detail=f"No PRs found in the last {days} days"
        )
    
    # Sort events by timestamp
    pr_events.sort(key=lambda e: e["created_at"])
    timestamps = [_parse_iso(e["created_at"]) for e in pr_events]
    
    # Create visualization
    fig = plt.figure(figsize=(15, 10))
    
    # 1. PR Timeline
    ax1 = plt.subplot(2, 2, 1)
    ax1.scatter(timestamps, range(len(timestamps)), alpha=0.6)
    ax1.set_ylabel("PR Index")
    ax1.set_title(f"PR Timeline for {owner}/{repo}")
    ax1.grid(True)
    
    # 2. Interval Distribution
    intervals = [(b - a).total_seconds()/3600 for a, b in zip(timestamps, timestamps[1:])]
    if intervals:
        ax2 = plt.subplot(2, 2, 2)
        ax2.hist(intervals, bins=20, alpha=0.7)
        ax2.set_xlabel("Hours between PRs")
        ax2.set_ylabel("Frequency")
        ax2.set_title("PR Interval Distribution")
        ax2.grid(True)
        
        # Add mean and median lines
        mean_interval = sum(intervals) / len(intervals)
        median_interval = sorted(intervals)[len(intervals)//2]
        ax2.axvline(mean_interval, color='r', linestyle='--', label=f'Mean: {mean_interval:.1f}h')
        ax2.axvline(median_interval, color='g', linestyle='--', label=f'Median: {median_interval:.1f}h')
        ax2.legend()
    
    # 3. Time of Day Analysis
    ax3 = plt.subplot(2, 1, 2)
    hours = [t.hour for t in timestamps]
    weekdays = [t.weekday() for t in timestamps]
    
    # Create 2D histogram
    plt.hist2d(hours, weekdays, bins=(24, 7), cmap='YlOrRd')
    plt.colorbar(label='Number of PRs')
    ax3.set_xlabel('Hour of Day (UTC)')
    ax3.set_ylabel('Day of Week')
    ax3.set_title('PR Creation Time Heatmap')
    ax3.set_yticks(range(7))
    ax3.set_yticklabels(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])
    
    plt.tight_layout()
    
    # Convert to PNG
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
    buf.seek(0)
    plt.close()
    
    return Response(content=buf.getvalue(), media_type="image/png")


# -------------------------------------------------
# 2) "Global" GitHub endpoints (all repos, on‐demand)
# -------------------------------------------------

@app.get(
    "/github/events",
    status_code=status.HTTP_200_OK
)
async def list_public_events(
    per_page: int = Query(30, ge=1, le=100, description="Events per page (max 100)"),
    page:     int = Query(1,  ge=1, description="Page number (starting at 1)")
):
    """
    Fetch GitHub's global public events (paginated), returning only WatchEvent,
    PullRequestEvent, and IssuesEvent. Runs in real‐time (no Dynamo involved).
    """
    url = f"{GITHUB_EVENTS_URL}?per_page={per_page}&page={page}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=_headers(), timeout=30) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise HTTPException(status_code=resp.status, detail=text)
            events = await resp.json()

    # Filter to our three event types
    filtered = [ev for ev in events if ev.get("type") in INTERESTING]
    return filtered


@app.get(
    "/events",
    status_code=status.HTTP_200_OK
)
def list_all_events():
    """
    Return every GitHub event JSON currently stored in DynamoDB,
    regardless of repo. WARNING: full table scan, can be slow if large.
    """
    try:
        all_evts = store.get_all_events()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch all events: {e}"
        )
    return all_evts


# Health check endpoint
@app.get("/health")
def health_check():
    """Check if the service is running and collecting events."""
    total_events = len(store.get_all_events())
    return {
        "status": "healthy",
        "total_events_collected": total_events,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


# from fastapi import FastAPI, HTTPException
# from .storage import store
# from .metrics import avg_pr_interval, counts_by_type
# import io, matplotlib.pyplot as plt
# from fastapi.responses import Response
# from .collectors import attach_to as _attach_collector
# from datetime import datetime, timedelta, timezone

# app = FastAPI()
# _attach_collector(app)   

# @app.get("/events", response_model=list[dict], status_code=200)
# def list_all_events():
#     try:
#         return store.get_all_events()
#     except Exception:
#         raise HTTPException(status_code=500, detail="Error fetching events.")

# @app.get("/metrics/{owner}/{repo}/avg-pr-interval")
# def get_avg_pr(owner: str, repo: str):
#     # pr_events = [e for e in store.events if e["type"] == "PullRequestEvent"
#     #              and e.get("payload", {}).get("action") == "opened" 
#     #              and e["repo"]["name"].lower() == f"{owner}/{repo}".lower()]
#     cutoff_repo = f"{owner}/{repo}"
#     pr_events = [
#         e for e in store.get_events_for_repo(cutoff_repo)
#         if e["type"] == "PullRequestEvent"
#         and e.get("payload", {}).get("action") == "opened"
#         ]
       
#     value = avg_pr_interval(pr_events)
#     if value is None:
#         raise HTTPException(status_code=404, detail="Not enough PR data yet.")
#     return {"average_seconds": value}

# @app.get("/metrics/{owner}/{repo}/event-counts")
# def get_counts(owner: str, repo: str, offset: int = 10):
#     cutoff_repo = f"{owner}/{repo}"
#     since = datetime.now(timezone.utc) - timedelta(minutes=offset)
#     recent = store.get_events_for_repo(cutoff_repo, since=since)
#     return counts_by_type(recent)

# @app.get("/viz/{owner}/{repo}/pr-intervals.png")
# def plot_pr_intervals(owner: str, repo: str, days: int = 10):
#     cutoff = datetime.now(timezone.utc) - timedelta(days=days)
#     cutoff_repo = f"{owner}/{repo}"
#     pr_events = [
#         e for e in store.get_events_for_repo(cutoff_repo, since=cutoff)
#         if e["type"] == "PullRequestEvent"
#         and e.get("payload", {}).get("action") == "opened"
#         ]
#     ts = sorted(datetime.fromisoformat(e["created_at"].replace("Z", "+00:00")).astimezone(timezone.utc) for e in pr_events)
#     if len(ts) < 1:
#         raise HTTPException(404, "Not enough data to plot.")
#     intervals = [(b - a).total_seconds()/3600 for a, b in zip(ts, ts[1:])]
#     fig, ax = plt.subplots()
#     ax.plot(ts[1:], intervals, marker="o")
#     ax.set_ylabel("Δ since previous PR (hours)")
#     ax.set_title(f"{owner}/{repo} PR cadence")
#     buf = io.BytesIO()
#     fig.savefig(buf, format="png")
#     return Response(content=buf.getvalue(), media_type="image/png")