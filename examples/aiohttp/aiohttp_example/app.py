from aiohttp import web

from aiohttp_example import signals
from aiohttp_example.apps.say_hello.views import say_hello_handler


def init_app(argv: list[str]) -> web.Application:
    app = web.Application()
    app.add_routes([
        web.get('/', say_hello_handler),
        web.get('/{name}', say_hello_handler),
    ])
    app.cleanup_ctx.extend([
        *signals.get_cleanup_ctx_factories(),
        signals.connect_darq,
    ])
    return app
