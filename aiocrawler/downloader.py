import asyncio
import logging

from aiocrawler.http import Session


class Downloader:
    pass


class _DownloadCounter:
    def __init__(self, download_manager):
        """

        :param DownloadManager download_manager:
        """
        self.download_manager = download_manager

    def __enter__(self):
        self.download_manager.current_downloads += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.download_manager.current_downloads -= 1


class DownloadManager:
    concurrent_downloads = 5
    current_downloads = 0
    stop = False

    def __init__(self, engine, queue=None):
        """
        :param aiocrawler.engine.Engine engine:
        :param asyncio.Queue queue:
        """
        self.engine = engine
        self.queue = queue or asyncio.Queue()
        self.spider_sessions = dict()

    @property
    def logger(self):
        return logging.getLogger('download_manager')

    def create_session(self, spider):
        return Session()

    def get_session(self, spider):
        if spider not in self.spider_sessions:
            self.spider_sessions[spider] = self.create_session(spider)
        return self.spider_sessions[spider]

    async def enqueue(self, spider, request):
        """
        :param aiocrawler.http.RequestWrapper request:
        """
        self.logger.info('Enqueued request from %s %s %s', spider.get_name(), request.method, request.url)
        await self.queue.put((spider, request))

    async def dequeue(self):
        return await self.queue.get()

    async def process_request(self, spider, request):
        """

        :param aiocrawler.http.RequestWrapper request:
        """
        session = self.get_session(spider)
        self.logger.info('Processing request %s %s %s', spider.get_name(), request.method, request.url)
        return await session.execute_request(request)

    async def process_response(self, spider, response):
        """
        :param aiocrawler.http.Response response:
        """
        return response

    async def download_loop(self):
        self.logger.info('Download loop started')
        while True:
            if self.current_downloads >= self.concurrent_downloads:
                await asyncio.sleep(0.3)
                continue
            spider, request = await self.dequeue()
            self.logger.info('Starting download of %s %s %s', spider.get_name(), request.method, request.url)
            self.engine.loop.create_task(self.start_download(spider, request))
        self.logger.info('Download loop finished')

    async def start_download(self, spider, request):
        with _DownloadCounter(self):
            response = await self.process_request(spider, request)

        self.logger.info('Got response for %s %s %s', spider.get_name(), request.method, request.url)

        response = await self.process_response(spider, response)
        self.engine.loop.create_task(self.engine.process_response(spider, response))
