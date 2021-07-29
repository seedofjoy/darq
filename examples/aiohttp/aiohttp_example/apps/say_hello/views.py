from aiohttp import web

from .lib import incr_visited_count
from .tasks import say_hello_from_worker


async def say_hello_handler(request: web.Request) -> web.Response:
    name = request.match_info.get('name', 'Anonymous')
    await incr_visited_count(name)
    await say_hello_from_worker.delay(name)
    return web.Response(text='Hello, ' + name)
