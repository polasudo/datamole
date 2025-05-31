import aiohttp
from app.storage import store, _parse_iso_utc

async def seed_repo(repo: str, session: aiohttp.ClientSession, n: int = 100):
    url = f"https://api.github.com/repos/{repo}/events?per_page={n}"
    async with session.get(url, timeout=30) as resp:
        resp.raise_for_status()
        events = await resp.json()
    for ev in events:
        store.add(ev)
    print(f"[seed] added {len(events)} events for {repo}")
