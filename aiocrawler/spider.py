from aiocrawler.http import Request


class Spider:
    """
    :param aiocrawler.engine.Engine engine:
    """
    name = None

    def __init__(self, engine):
        self.engine = engine

    def get_name(self):
        return self.name or self.__class__.__name__

    async def add_request(self, request):
        await self.engine.add_request(self, request)

    async def get(self, url, callback=None, meta=None, **kwargs):
        request = Request(url=url, method='GET', callback=callback, meta=meta, **kwargs)
        return await self.add_request(request)

    async def post(self, url, callback=None, meta=None, **kwargs):
        request = Request(url=url, method='POST', callback=callback, meta=meta, **kwargs)
        return await self.add_request(request)

    async def start(self):
        raise NotImplementedError()

    async def process_response(self, response):
        """
        :param aiocrawler.http.Response response:
        """
        raise NotImplementedError()

    def __str__(self):
        return self.get_name()

    def __repr__(self):
        return '<Spider "%s">' % self.get_name()
