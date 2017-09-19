import asyncio

from aiocrawler.http import Session


class Downloader:
    stop = False
    oneshot = True

    def __init__(self, spider, queue=None, session=None):
        """

        :param aiocrawler.spider.Spider spider:
        :param asyncio.Queue queue:
        :param Session session:
        """
        self.spider = spider
        self.queue = queue or asyncio.Queue()
        self.session = session or Session()

    async def enqueue(self, request):
        """

        :param aiocrawler.request.Request request:
        """
        await self.queue.put(request)

    async def dequeue(self):
        return await self.queue.get()

    async def process_request(self, request):
        """

        :param aiocrawler.http.RequestWrapper request:
        """
        return await self.session.execute_request(request)

    async def process_response(self, response):
        """
        :param aiocrawler.http.Response response:
        """
        return response

    async def loop(self):
        while not self.stop:
            request = await self.dequeue()
            response = await self.process_request(request)
            yield await self.process_response(response)

            if self.queue.empty() and self.oneshot:
                break
