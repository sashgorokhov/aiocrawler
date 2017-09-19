from aiocrawler import downloader


class Spider:
    def __init__(self, engine):
        """
        :param aiocrawler.engine.Engine engine:
        """
        self.engine = engine
        self.downloader = downloader.Downloader(self)

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def start(self):
        pass

    async def process_response(self, response):
        pass

    async def close(self):
        self.downloader.stop = True
