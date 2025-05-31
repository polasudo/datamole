"""
Background task that polls GitHub's public events feed.
Note: GitHub Events API has latency of 30s to 6h and returns max 300 events from last 30 days.
"""

from __future__ import annotations

import asyncio
import logging
from typing import List, Dict, Any
from datetime import datetime, timezone, timedelta

import aiohttp
from fastapi import HTTPException

from .config import settings
from .storage import store

__all__ = ['attach_to', 'fetch_repo_events']

log = logging.getLogger(__name__)
GITHUB_EVENTS_URL = "https://api.github.com/events"
REPO_EVENTS_URL = "https://api.github.com/repos/{owner}/{repo}/events"
INTERESTING = {"WatchEvent", "PullRequestEvent", "IssuesEvent"}

def _headers() -> dict[str, str]:
    """Get headers for GitHub API requests with proper auth."""
    hdrs = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "github-metrics-demo",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    if settings.github_token:
        hdrs["Authorization"] = f"Bearer {settings.github_token}"
    return hdrs

async def _handle_rate_limit(resp: aiohttp.ClientResponse) -> None:
    """Handle GitHub API rate limiting."""
    if resp.status == 403:
        remaining = int(resp.headers.get('X-RateLimit-Remaining', 0))
        reset_time = int(resp.headers.get('X-RateLimit-Reset', 0))
        if remaining == 0:
            now = datetime.now(timezone.utc).timestamp()
            wait_time = reset_time - now
            if wait_time > 0:
                log.warning(f"Rate limit hit. Waiting {wait_time:.2f} seconds...")
                await asyncio.sleep(wait_time + 5)

async def _fetch_page(
    session: aiohttp.ClientSession,
    url: str,
    page: int = 1,
    per_page: int = 100
) -> List[Dict[str, Any]]:
    """
    Fetch a single page of events from GitHub.
    Handles rate limiting and retries.
    """
    params = {"page": page, "per_page": per_page}
    
    while True:
        try:
            async with session.get(url, headers=_headers(), params=params, timeout=30) as resp:
                await _handle_rate_limit(resp)
                if resp.status == 404:
                    log.warning(f"Resource not found: {url}")
                    return []
                resp.raise_for_status()
                
                # Check poll interval
                poll_interval = int(resp.headers.get('X-Poll-Interval', 60))
                log.debug(f"Poll interval: {poll_interval} seconds")
                
                return await resp.json()
                
        except asyncio.TimeoutError:
            log.error(f"Timeout fetching {url}")
            return []
        except Exception as e:
            log.error(f"Error fetching {url}: {e}")
            return []

async def fetch_repo_events(owner: str, repo: str) -> List[Dict[str, Any]]:
    """
    Fetch events specifically for a repository.
    This helps get more accurate data for a specific repo.
    """
    url = REPO_EVENTS_URL.format(owner=owner, repo=repo)
    all_events = []
    
    async with aiohttp.ClientSession() as session:
        # GitHub returns max 300 events fetch 3 pages of 100
        for page in range(1, 4):
            events = await _fetch_page(session, url, page=page)
            if not events:
                break
            all_events.extend(events)
            await asyncio.sleep(1)
            
    return all_events

async def _collector_loop():
    """
    Main collector loop that runs indefinitely.
    Fetches both global events and specific repository events.
    """
    log.info("Starting event collector...")
    
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                # Fetch global public events
                events = await _fetch_page(session, GITHUB_EVENTS_URL)
                log.info(f"Fetched {len(events)} public events")
                
                # Store interesting events
                stored_count = 0
                for event in events:
                    if event.get("type") in INTERESTING:
                        try:
                            store.add(event)
                            stored_count += 1
                        except Exception as e:
                            log.error(f"Failed to store event: {e}")
                
                log.info(f"Stored {stored_count} interesting events")
                
                # Wait for next poll interval
                await asyncio.sleep(settings.poll_interval)
                
        except Exception as e:
            log.error(f"Collector error: {e}")
            await asyncio.sleep(settings.poll_interval)

def attach_to(app):
    """Attach the collector to a FastAPI application."""
    @app.on_event("startup")
    async def start_collector():
        app.state.collector_task = asyncio.create_task(_collector_loop())
        log.info("Collector task started")

    @app.on_event("shutdown")
    async def stop_collector():
        if hasattr(app.state, 'collector_task'):
            app.state.collector_task.cancel()
            try:
                await app.state.collector_task
            except asyncio.CancelledError:
                pass
            log.info("Collector task stopped")
