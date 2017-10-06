import asyncio
import datetime
import logging
import operator

import aiocrawler


class Top100Spider(aiocrawler.Spider):
    name = 'top100'
    filename = 'top100.txt'
    start_time = None
    concurrent_requests_limit = 100

    @asyncio.coroutine
    def get_urls_list(self):
        with open(self.filename, 'r') as f:
            yield from map(operator.methodcaller('strip'), f)

    async def start(self):
        self.start_time = datetime.datetime.now()
        for url in self.get_urls_list():
            await self.get('http://%s' % url, timeout=10)

    async def process_response(self, response):
        await response.read()
        yield str(response.url)

    async def process_item(self, item):
        print(item)

    def close_spider(self):
        self.logger.info('Completed in %s seconds' % (datetime.datetime.now() - self.start_time).total_seconds())


if __name__ == '__main__':
    aiocrawler.configure_logging(logging.INFO)
    engine = aiocrawler.Engine()
    engine.add_spider(Top100Spider(engine))
    engine.start()
