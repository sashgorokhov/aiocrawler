import asyncio
import inspect
import logging

from aiocrawler.downloader import DownloadManager
from aiocrawler.spider import _SpiderOpenClose


class Engine:
    """
    :param list[aiocrawler.spider.Spider] spiders:
    """

    def __init__(self, loop=None):
        """

        :param asyncio.base_events.BaseEventLoop loop:
        """
        self.loop = loop or asyncio.get_event_loop()
        self.download_manager = DownloadManager(self)
        self.spiders = list()

    @property
    def logger(self):
        return logging.getLogger('engine')

    def start(self):
        self.loop.create_task(self.download_manager.download_loop())
        for spider in self.spiders:
            self.loop.create_task(self.run_spider(spider))
        self.loop.run_forever()

    async def run_spider(self, spider):
        """
        :param aiocrawler.spider.Spider spider:
        """
        self.logger.info('Running spider %s', spider.get_name())
        async with _SpiderOpenClose(spider):
            await spider.start()

    async def add_request(self, spider, request):
        await self.download_manager.enqueue(spider, request)

    async def process_response(self, spider, response):
        callback = response.callback or getattr(spider, 'process_response', None)
        if not callback:
            return
        if inspect.iscoroutinefunction(callback):
            self.loop.create_task(callback(response))

    def add_spider(self, spider):
        """

        :param aiocrawler.spider.Spider spider:
        """
        self.spiders.append(spider)
