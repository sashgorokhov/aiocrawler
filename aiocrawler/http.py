import aiohttp
from aiohttp import client_reqrep


class Request:
    meta = None

    def __init__(self, url, method='GET', callback=None, meta=None, priority=0, **kwargs):
        self.url = url
        self.method = method
        self.callback = callback
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

    async def execute_request(self, request):
        """

        :param Request request:
        :return: Response
        """
        async with self.request(request.method, request.url, **request.kwargs) as response:
            response.request = request
            response.meta = request.meta.copy()
            response.callback = request.callback
            await response.text()
            return response
