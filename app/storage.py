# storage.py
from datetime import datetime, timedelta, timezone
import time
import boto3
from boto3.dynamodb.conditions import Key, Attr
from collections import defaultdict
from typing import List, Dict, Any, Optional

from .config import settings

# class DynamoEventStore:
#     def __init__(self):
#         self.dynamo = boto3.resource(
#             'dynamodb',
#             region_name=settings.aws_region, 
#             aws_access_key_id="",
#             aws_secret_access_key="",
#         )
#         self.table = self.dynamo.Table(settings.dynamo_table)

#     def add(self, event: dict) -> None:
#         """Store a GitHub event in DynamoDB."""
#         try:
#             repo = event['repo']['name'].lower()
#             created_at = event['created_at']  # e.g. "2025-05-30T12:34:56Z"
#             # Use event ID as part of the key to ensure uniqueness
#             event_id = event.get('id', '')
            
#             # Create a composite key: repoName#eventId
#             composite_key = f"{repo}#{event_id}"
            
#             # DynamoDB TTL must be a UNIX epoch in seconds:
#             expire_at = int(time.time()) + settings.max_minutes * 60

#             item = {
#                 'repoName': composite_key,
#                 'createdAt': created_at,
#                 'eventData': event,
#                 'expireAt': expire_at,
#                 'eventType': event.get('type', ''),
#                 'repo': repo,
#             }
#             print(f"[storage] Storing event: {composite_key}, type: {event.get('type')}")
#             self.table.put_item(Item=item)
#         except Exception as e:
#             print(f"[storage] Error storing event: {e}")  # Debug log
#             raise

#     def get_events_for_repo(
#         self,
#         repo: str,
#         since: datetime | None = None,
#     ) -> list[dict]:
#         """Get all events for a repository."""
#         repo = repo.lower()
#         print(f"[storage] Querying events for repo: {repo}")  # Debug log
        
#         # Use a GSI to query by repo
#         filter_expr = Attr('repo').eq(repo)
        
#         if since:
#             cutoff = since.astimezone(timezone.utc)
#             cutoff_str = cutoff.isoformat().replace('+00:00', 'Z')
#             filter_expr &= Attr('createdAt').gte(cutoff_str)
#             print(f"[storage] Using cutoff: {cutoff_str}")  # Debug log

#         try:
#             # scan with filter instead of query since we need to search by repo
#             resp = self.table.scan(
#                 FilterExpression=filter_expr
#             )
#             items = resp.get('Items', [])
#             print(f"[storage] Found {len(items)} items in DynamoDB")  # Debug log
            
#             # Extract and validate events
#             events = []
#             for item in items:
#                 event_data = item.get('eventData')
#                 if event_data and isinstance(event_data, dict):
#                     events.append(event_data)
#                 else:
#                     print(f"[storage] Warning: Invalid event data in item: {item}")  # Debug log
            
#             print(f"[storage] Returning {len(events)} valid events")  # Debug log
#             return events
            
#         except Exception as e:
#             print(f"[storage] Error querying DynamoDB: {e}")  # Debug log
#             raise

#     def recent(self, since_minutes: int) -> list[dict]:
#         cutoff = datetime.now(timezone.utc) - timedelta(minutes=since_minutes)
#         cutoff_str = cutoff.isoformat().replace('+00:00', 'Z')
#         resp = self.table.scan(
#             FilterExpression=Attr('createdAt').gte(cutoff_str)
#         )
#         return [item['eventData'] for item in resp.get('Items', [])]

#     def get_all_events(self):
#         """
#         Return every stored `eventData` in DynamoDB. 
#         Skip any items that do not have an 'eventData' attribute.
#         """
#         all_events = []
#         scan_kwargs = {
#             "ProjectionExpression": "eventData"
#         }
#         done = False
#         start_key = None

#         while not done:
#             if start_key:
#                 scan_kwargs["ExclusiveStartKey"] = start_key

#             resp = self.table.scan(**scan_kwargs)
#             items = resp.get("Items", [])

#             for item in items:
#                 if "eventData" in item:
#                     all_events.append(item["eventData"])
#                 # else: skip it quietly

#             start_key = resp.get("LastEvaluatedKey")
#             done = start_key is None

#         return all_events

#     def get_all_events_page(
#         self,
#         last_evaluated_key: None,
#         limit: None
#     ):
#         """
#         Scan with pagination:
#           - If `last_evaluated_key` is provided, use it as ExclusiveStartKey.
#           - If `limit` is provided, set Limit=limit.
#         Returns a tuple (events_page, next_last_evaluated_key).
#         The client can pass next_last_evaluated_key (JSON‐encoded) to fetch the next page.
#         """
#         scan_kwargs: Dict[str, Any] = {
#             "ProjectionExpression": "eventData"
#         }
#         if limit:
#             scan_kwargs["Limit"] = limit
#         if last_evaluated_key:
#             scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

#         resp = self.table.scan(**scan_kwargs)
#         items = resp.get("Items", [])
#         events_page = [item["eventData"] for item in items]
#         next_lek = resp.get("LastEvaluatedKey")
#         return events_page, next_lek

#     def get_events_by_type(
#         self,
#         repo: str,
#         minutes: int
#     ) -> Dict[str, int]:
#         """Get event counts by type for the last N minutes."""
#         since = datetime.now(timezone.utc) - timedelta(minutes=minutes)
#         events = self.get_events_for_repo(repo, since=since)
        
#         counts = defaultdict(int)
#         for event in events:
#             counts[event['type']] += 1
#         return dict(counts)

# Create a single instance to be used throughout the app
# store = DynamoEventStore()

class InMemoryEventStore:
    def __init__(self):
        # Store events in a dictionary: repo -> list of events
        self.events: Dict[str, List[Dict]] = defaultdict(list)
        
    def add(self, event: dict) -> None:
        """Store a GitHub event in memory."""
        try:
            repo = event['repo']['name'].lower()
            # Add event to the repository's list
            self.events[repo].append(event)
            print(f"[storage] Stored event for {repo}, type: {event.get('type')}")
        except Exception as e:
            print(f"[storage] Error storing event: {e}")
            raise

    def get_events_for_repo(
        self,
        repo: str,
        since: Optional[datetime] = None
    ) -> List[Dict]:
        """Get all events for a repository, optionally filtered by time."""
        repo = repo.lower()
        print(f"[storage] Querying events for repo: {repo}")
        
        # Get all events for the repo
        events = self.events.get(repo, [])
        
        if since:
            # Filter events by timestamp if since is provided
            filtered_events = []
            for event in events:
                event_time = datetime.fromisoformat(
                    event['created_at'].replace('Z', '+00:00')
                ).astimezone(timezone.utc)
                if event_time >= since:
                    filtered_events.append(event)
            print(f"[storage] Found {len(filtered_events)} events since {since}")
            return filtered_events
        
        print(f"[storage] Found {len(events)} total events")
        return events

    def get_all_events(self) -> List[Dict]:
        """Get all stored events."""
        all_events = []
        for events in self.events.values():
            all_events.extend(events)
        return all_events

    def get_events_by_type(
        self,
        repo: str,
        minutes: int
    ) -> Dict[str, int]:
        """Get event counts by type for the last N minutes."""
        since = datetime.now(timezone.utc) - timedelta(minutes=minutes)
        events = self.get_events_for_repo(repo, since=since)
        
        counts = defaultdict(int)
        for event in events:
            counts[event['type']] += 1
        return dict(counts)

    def recent(self, since_minutes: int) -> List[Dict]:
        """Get all events from the last N minutes across all repositories."""
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=since_minutes)
        recent_events = []
        for events in self.events.values():
            for event in events:
                event_time = datetime.fromisoformat(
                    event['created_at'].replace('Z', '+00:00')
                ).astimezone(timezone.utc)
                if event_time >= cutoff:
                    recent_events.append(event)
        return recent_events

# Create a single instance to be used throughout the app
store = InMemoryEventStore()
