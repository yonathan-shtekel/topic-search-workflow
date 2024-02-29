import hashlib
import time
from dataclasses import dataclass, field
from typing import Optional, Dict, List


def search_uuid(jq_query: str, key: str, topic: str) -> str:
    query = jq_query or ""
    key = key or ""
    to_hash = f"{query}{key}{topic}"
    return hashlib.md5(to_hash.encode()).hexdigest()


@dataclass
class KafkaQuery:
    topic: str
    id: Optional[str] = None
    jq_query: Optional[str] = None
    key: Optional[str] = None
    partition_ids: Optional[List[int]] = None
    timeout: Optional[int] = 20
    start_time: Optional[int] = field(
        default_factory=lambda: int(time.time() * 1000)
    )  # Default to current time in milliseconds

    def __post_init__(self):
        # Validate that either jq_query or key is provided
        if not self.jq_query and not self.key:
            raise ValueError("Either jq_query or key must be set")

        # Validate topic
        if not isinstance(self.topic, str) or self.topic == "":
            raise ValueError("topic must be a non-empty string")

        # Set id using the search_uuid function
        self.id = search_uuid(self.jq_query, self.key, self.topic)


@dataclass
class KafkaMessage:
    key: Optional[str]
    value: Optional[Dict]
    partition_id: Optional[int]
    offset: Optional[int]
    timestamp: Optional[int]
    query: Optional[KafkaQuery]
