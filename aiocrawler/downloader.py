import asyncio
import logging
import operator
import time

from aiocrawler.http import Session
from aiocrawler.utils.leaky_bucket import AsyncLeakyBucket


class Downloader:
    """
    :param asyncio.base_events.BaseEventLoop loop:
    :param aiocrawler.engine.Engine engine:
    """

    def __init__(self, engine):
        self.engine = engine
        self.loop = asyncio.new_event_loop()
        self._queue = asyncio.Queue(loop=self.loop)
        self._bucket = AsyncLeakyBucket(5, 1)
        self._spider_sessions = dict()

    @property
    def logger(self):
        return logging.getLogger('download_manager')

    def create_session(self, spider):
        return Session(loop=self.loop)

    def get_session(self, spider):
        if spider not in self._spider_sessions:
            self._spider_sessions[spider] = self.create_session(spider)
        return self._spider_sessions[spider]

    def _run_download_loop(self):
        self.loop.create_task(self._download_loop())

    async def _download_loop(self):
        while not self._queue.empty():
            async with self._bucket:
                spider, request = await self._queue.get()
                self.loop.create_task(self.process_request(spider, request))
        self.loop.call_later(0.5, self._run_download_loop)

    async def start_downloader(self):
        self.loop.create_task(self._download_loop())

    async def stop_downloader(self):
        pass

    def start(self):
        self.loop.create_task(self.start_downloader())
        self.loop.run_forever()

    def _add_request(self, spider, request):
        self.loop.create_task(self.add_request(spider, request))

    async def add_request(self, spider, request):
        """
        :param aiocrawler.spider.Spider spider:
        :param aiocrawler.http.Request request:
        """
        return await self._queue.put((spider, request))

    async def process_request(self, spider, request):
        """
        :param aiocrawler.spider.Spider spider:
        :param aiocrawler.http.Request request:
        """
        session = self.get_session(spider)
        response = await session.execute_request(request)
        await self.process_response(spider, response)

    async def process_response(self, spider, response):
        """
        :param aiocrawler.spider.Spider spider:
        :param aiocrawler.http.Response response:
        """
        self.engine.loop.call_soon_threadsafe(self.engine._process_response, spider, response)

    def shutdown(self):
        for session in self._spider_sessions.values():
            session.close()

        list(map(operator.methodcaller('cancel'), asyncio.Task.all_tasks(loop=self.loop)))

        while asyncio.Task.all_tasks(loop=self.loop):
            time.sleep(1)
            list(map(operator.methodcaller('cancel'), asyncio.Task.all_tasks(loop=self.loop)))

        self.loop.stop()
