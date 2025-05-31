from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

def _parse_iso(s: str) -> datetime:
    """
    Parse an ISO8601 string ending in Z into a UTC datetime.
    Example: "2025-05-30T12:34:56Z"
    """
    # Replace trailing 'Z' with '+00:00' so fromisoformat can parse it
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)

def avg_pr_interval(pr_events: List[Dict[str, Any]]) -> Optional[float]:
    """
    Given a list of PullRequestEvent JSONs (filtered to only those with payload.action == "opened"),
    compute the average interval (in seconds) between successive PR creation times.
    Return None if fewer than 2 events.
    """
    if len(pr_events) < 2:
        return None

    # Extract and sort the timestamps
    timestamps = sorted(_parse_iso(ev["created_at"]) for ev in pr_events)

    # Compute successive deltas
    deltas = []
    for i in range(1, len(timestamps)):
        delta = (timestamps[i] - timestamps[i-1]).total_seconds()
        if delta >= 0:
            deltas.append(delta)

    if not deltas:
        return None

    return sum(deltas) / len(deltas)

def counts_by_type(events: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Given a list of raw event JSONs, return a dict mapping each event type
    to the total count.
    E.g. { "WatchEvent": 5, "PullRequestEvent": 2, "IssuesEvent": 3 }
    """
    counts: Dict[str, int] = {}
    for ev in events:
        ev_type = ev.get("type", "Unknown")
        counts[ev_type] = counts.get(ev_type, 0) + 1
    return counts
