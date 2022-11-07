from .app import Darq
from .connections import RedisSettings
from .cron import cron
from .types import JobCtx
from .version import __version__
from .worker import Retry

try:
    from aioredis import __version__ as aioredis_version
except ImportError:
    raise ImportError(
        'Please, install "aioredis" (or "evo-aioredis" - for Python 3.11+)')

AIOREDIS_MAJOR_VERSION = int(aioredis_version.split('.')[0])
if AIOREDIS_MAJOR_VERSION > 1:
    raise ImportError('Darq only supports "aioredis" versions <2.0')

__all__ = ['Darq', 'JobCtx', 'RedisSettings', 'cron', 'Retry', '__version__']
