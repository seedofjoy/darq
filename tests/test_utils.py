import logging
import re
from datetime import timedelta

import pytest

import darq
from darq import utils
from darq.connections import create_pool
from darq.connections import log_redis_info
from darq.connections import RedisSettings
from . import redis_settings


def test_settings_changed():
    settings = RedisSettings(port=123)
    assert settings.port == 123
    assert (
        '<RedisSettings host=localhost port=123 database=0 password=None '
        'ssl=None conn_timeout=1 conn_retries=5 conn_retry_delay=1 '
        'sentinel=False sentinel_master=mymaster sentinel_timeout=0.2>'
    ) == str(settings)


async def test_redis_timeout(mocker):
    mocker.spy(darq.connections.asyncio, 'sleep')
    with pytest.raises(OSError):
        await create_pool(RedisSettings(port=0, conn_retry_delay=0))
    assert darq.connections.asyncio.sleep.call_count == 5


async def test_redis_sentinel_failure():
    settings = RedisSettings()
    settings.host = [(redis_settings.host, 6379), (redis_settings.host, 6379)]
    settings.sentinel = True
    try:
        pool = await create_pool(settings)
        await pool.ping('ping')
    except Exception as e:
        assert (
            'unknown command `SENTINEL`' in str(e)
            or 'Could not connect to any sentinel' in str(e)
        )


async def test_redis_success_log(caplog):
    caplog.set_level(logging.INFO)
    pool = await create_pool(redis_settings)
    assert 'redis connection successful' not in [
        r.message for r in caplog.records
    ]
    pool.close()
    await pool.wait_closed()

    pool = await create_pool(redis_settings, retry=1)
    assert 'redis connection successful' in [r.message for r in caplog.records]
    pool.close()
    await pool.wait_closed()


async def test_redis_log():
    redis = await create_pool(redis_settings)
    await redis.flushall()
    await redis.set(b'a', b'1')
    await redis.set(b'b', b'2')

    log_msgs = []

    def _log(s):
        log_msgs.append(s)

    await log_redis_info(redis, _log)
    assert len(log_msgs) == 1
    assert re.search(r'redis_version=\d\.', log_msgs[0]), log_msgs
    assert log_msgs[0].endswith(' db_keys=2')


def test_truncate():
    assert utils.truncate('123456', 4) == '123…'


def test_args_to_string():
    assert utils.args_to_string((), {'d': 4}) == 'd=4'
    assert utils.args_to_string((1, 2, 3), {}) == '1, 2, 3'
    assert utils.args_to_string((1, 2, 3), {'d': 4}) == '1, 2, 3, d=4'


@pytest.mark.parametrize('input_,output', [
    (timedelta(days=1), 86_400_000),
    (42, 42000),
    (42.123, 42123),
    (42.123_987, 42124),
    (None, None),
])
def test_to_ms(input_, output):
    assert utils.to_ms(input_) == output


@pytest.mark.parametrize('input_,output', [
    (timedelta(days=1), 86400),
    (42, 42),
    (42.123, 42.123),
    (None, None),
])
def test_to_seconds(input_, output):
    assert utils.to_seconds(input_) == output
