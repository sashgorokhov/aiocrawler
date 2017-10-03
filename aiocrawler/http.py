import inspect

import aiohttp
from aiohttp import client_reqrep


class Request:
    meta = None

    def __init__(self, url, method='GET', callback=None, meta=None, priority=0, **kwargs):
        self.url = str(url)
        self.method = method
        self.callback = callback
        if self.callback and not inspect.iscoroutinefunction(self.callback):
            raise TypeError('callback parameter must be a coroutine function, not %s' % type(self.callback))
        self.kwargs = kwargs
        self.meta = meta or dict()
        self.priority = priority


class Response(client_reqrep.ClientResponse):
    request = None
    meta = None
    callback = None


class Session(aiohttp.ClientSession):
    # noinspection PyArgumentList
    def __init__(self, **kwargs):
        kwargs.setdefault('response_class', Response)
        super(Session, self).__init__(**kwargs)
