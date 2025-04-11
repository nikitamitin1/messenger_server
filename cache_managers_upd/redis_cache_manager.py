import json
from bson import json_util
import redis.asyncio as redis
from .base_cache_manager import CacheManager

class RedisCacheManager(CacheManager):
    def __init__(self, redis_url: str):
        self.redis = redis.StrictRedis.from_url(redis_url, decode_responses=True)

    async def load_data(self, key: str):
        cached_data = await self.redis.get(key)
        if cached_data:
            return json.loads(cached_data)
        return None

    async def get_data(self, key: str):
        return await self.load_data(key)

    async def set_data(self, key: str, data: dict):
        await self.redis.set(key, json.dumps(data, default=json_util.default))

    async def delete_data(self, key: str):
        await self.redis.delete(key)

    async def rpush_data(self, key: str, data: dict):
        await self.redis.rpush(key, json.dumps(data, default=json_util.default))

    async def lrange_data(self, key: str, start: int, end: int):
        items = await self.redis.lrange(key, start, end)
        return [json.loads(item) for item in items]

    async def lset_data(self, key: str, index: int, value: dict):
        await self.redis.lset(key, index, json.dumps(value, default=json_util.default))
