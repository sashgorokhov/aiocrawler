import asyncio
import datetime
import logging
import operator
import os
import sys

from aiocrawler import signals, exceptions

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import aiocrawler


class FilterFieldsPipeline:
    def process_item(self, spider, item):
        if not item['title']:
            raise exceptions.DropItem('Item has empty title')
        return item


class CleanFieldsPipeline:
    def process_item(self, spider, item):
        item['title'] = list(map(operator.methodcaller('strip'), item['title']))
        item['meta'] = list(map(operator.methodcaller('strip'), item['meta']))
        return item


class Top100Spider(aiocrawler.Spider):
    name = 'top100'
    filename = os.path.join(os.path.dirname(__file__), 'top100.txt')
    start_time = None
    concurrent_requests_limit = 100

    pipelines = [
        FilterFieldsPipeline,
        CleanFieldsPipeline
    ]

    @classmethod
    def from_engine(cls, engine):
        spider = super(Top100Spider, cls).from_engine(engine)  # type: Top100Spider
        engine.signals.connect(spider.item_scraped, signals.item_scraped)
        engine.signals.connect(spider.close_spider, signals.spider_closed)
        return spider

    @asyncio.coroutine
    def get_urls_list(self):
        with open(self.filename, 'r') as f:
            yield from map(operator.methodcaller('strip'), f)

    async def start(self):
        self.start_time = datetime.datetime.now()
        for url in self.get_urls_list():
            await self.get('http://%s' % url, timeout=10)

    async def process_response(self, response):
        sel = await response.selector()
        item = dict()
        item['url'] = str(response.url)
        item['title'] = sel.css('title::text').extract()
        item['meta'] = sel.css('meta::attr(content)').extract()
        yield item

    async def item_scraped(self, spider, item):
        self.logger.info('Scraped item: %s', item)

    def close_spider(self, spider):
        self.logger.info('Completed in %s seconds' % (datetime.datetime.now() - self.start_time).total_seconds())


if __name__ == '__main__':
    aiocrawler.configure_logging(logging.INFO)
    engine = aiocrawler.Engine()
    engine.add_spider(Top100Spider)
    engine.start()
