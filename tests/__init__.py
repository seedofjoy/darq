import os

from arq.connections import RedisSettings

redis_settings = RedisSettings(
    host=os.environ.get('REDIS_HOST', 'localhost'),
    port=os.environ.get('REDIS_PORT', 6379),
)
