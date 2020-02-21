import os

from darq import RedisSettings

redis_settings = RedisSettings(
    host=os.environ.get('REDIS_HOST', 'localhost'),
    port=os.environ.get('REDIS_PORT', 6379),
)
