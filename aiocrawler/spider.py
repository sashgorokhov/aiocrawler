class _SpiderOpenClose:
    def __init__(self, spider):
        """
        :param Spider spider:
        """
        self.spider = spider

    async def __aenter__(self):
        await self.spider.open()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.spider.close()


class Spider:
    name = None

    def __init__(self, engine):
        """
        :param aiocrawler.engine.Engine engine:
        """
        self.engine = engine

    def get_name(self):
        return self.name or self.__class__.__name__

    async def open(self):
        pass

    async def close(self):
        pass

    async def start(self):
        raise NotImplementedError()

    async def process_response(self, response):
        raise NotImplementedError()

    async def add_request(self, request):
        await self.engine.add_request(self, request)
