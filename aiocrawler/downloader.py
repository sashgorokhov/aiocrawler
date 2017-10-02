import asyncio
import logging

from aiocrawler.http import Session
from aiocrawler.utils.leaky_bucket import AsyncLeakyBucket


class Downloader:
    """
    :param asyncio.base_events.BaseEventLoop loop:
    :param aiocrawler.engine.Engine engine:
    """
    is_shutdown = False
    _scheduled = 0
    _completed = 0

    def __init__(self, engine):
        self.engine = engine
        self.loop = engine.loop
        self._queue = asyncio.Queue(loop=self.loop)
        self._bucket = AsyncLeakyBucket(3, 1)
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

    async def _download_loop(self):
        while not self.is_shutdown:
            while not self._queue.empty():
                async with self._bucket:
                    with (await self.engine._shutdown_lock):
                        spider, request = await self._queue.get()
                        future = self.loop.create_task(self.process_request(spider, request))
                        self.engine._futures.append(future)
                        self._scheduled += 1
            await asyncio.sleep(1)

    async def start_downloader(self):
        await self._download_loop()

    async def stop_downloader(self):
        pass

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
        self.logger.info('Currently processing: %s', self._scheduled - self._completed)

        session = self.get_session(spider)
        response = await session.execute_request(request)
        await self.process_response(spider, response)

    async def process_response(self, spider, response):
        """
        :param aiocrawler.spider.Spider spider:
        :param aiocrawler.http.Response response:
        """
        try:
            await self.engine.process_response(spider, response)
        finally:
            self._completed += 1

    def shutdown(self):
        if self.is_shutdown:
            return
        self.logger.info('Shutting down')
        self.is_shutdown = True

        for session in self._spider_sessions.values():
            if not session.closed:
                session.close()

                # if self.loop.is_running():
                #    self.loop.stop()
