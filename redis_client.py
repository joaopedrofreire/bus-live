import redis
import os

REDIS_URL = os.environ.get("REDIS_URL")

if not REDIS_URL:
    raise RuntimeError("Variável de ambiente REDIS_URL não definida")

redis_client = redis.from_url(
    REDIS_URL,
    decode_responses=True
)
