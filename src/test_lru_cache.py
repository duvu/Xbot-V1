from lru_redis_cache import LRUCache

cache = LRUCache(cache_size=5, ttl=60)

cache.set("my_key", "0")
print(cache.get("my_key"))
print(cache.get("my_key"))
print(cache.get("my_key"))
print(cache.get("my_key"))

