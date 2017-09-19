import asyncio
import inspect


class Engine:
    """
    :param list[aiospider.spider.Spider] spiders:
    """

    def __init__(self, loop=None):
        """

        :param asyncio.base_events.BaseEventLoop loop:
        """
        self.loop = loop or asyncio.get_event_loop()
        self.spiders = list()

    def start(self):
        self.loop.run_until_complete(asyncio.wait(
            map(self.run_spider, self.spiders)
        ))

    async def run_spider(self, spider):
        """
        :param aiospider.spider.Spider spider:
        """

        async with spider:
            async for response in spider.downloader.loop():
                await self.process_response(spider, response)

    async def process_response(self, spider, response):
        callback = response.callback or getattr(spider, 'process_response', None)

        if not callback:
            return

        if inspect.isfunction(callback):
            return callback(response)
        elif inspect.iscoroutinefunction(callback):
            return await callback(response)

    def add_spider(self, spider):
        """

        :param aiospider.spider.Spider spider:
        """
        self.spiders.append(spider)
