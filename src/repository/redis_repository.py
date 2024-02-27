import json
from typing import Dict, List
from redis import Redis


class RedisRepository:
    def __init__(self, rc: Dict) -> None:
        self.client: Redis = Redis(host=rc["host"], port=int(rc["port"]))

    def save(self, key: str, value: str) -> None:
        self.client.rpush(key, value)
        self.search_expire(key)

    def search_started(self, key: str) -> None:
        v: str = f'{{"state":"started"}}'
        self.client.rpush(key, v)
        self.search_expire(key)

    def search_validation_error(self, key: str) -> None:
        v: str = f'{{"state":"parameters are not valid."}}'
        self.client.rpush(key, v)
        self.search_expire(key)

    def time(self, key: str, dt: int) -> None:
        self.client.set(f"go-time of {key}", dt, ex=10800)  # 3 hours in seconds

    def search_finished(self, key: str) -> None:
        v: str = f'{{"state":"finished"}}'
        self.client.rpush(key, v)
        self.search_expire(key)

    def search_exception(self, key: str, err: Exception) -> None:
        v: str = f'{{"state":"{err}"}}'
        self.client.rpush(key, v)
        self.search_expire(key)

    def search_expire(self, key: str) -> None:
        self.client.expire(key, 6000)  # 10 minutes in seconds

    def get_list(self, key: str) -> List[str]:
        res_list = self.client.lrange(key, 0, -1)
        return [json.loads(x) for x in res_list]

    def list_keys(self) -> List[str]:
        return self.client.keys()

    def exists(self, key: str) -> bool:
        return self.client.exists(key)
