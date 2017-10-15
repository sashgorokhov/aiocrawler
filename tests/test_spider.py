import pytest

import aiocrawler


@pytest.mark.parametrize(['spider_name', 'spider_cls_name', 'expected'], [
    ['Foo', 'Bar', 'Foo'],
    ['', 'Bar', 'Bar'],
    [None, 'Bar', 'Bar'],
])
def test_spider_name(spider_name, spider_cls_name, expected, engine):
    spider_cls = type(spider_cls_name, (aiocrawler.Spider,), {'name': spider_name})  # type: aiocrawler.Spider
    spider = spider_cls.from_engine(engine)
    assert spider.get_name() == expected
    assert str(spider) == expected


@pytest.mark.parametrize(['method', 'url'], [
    ['GET', 'http://example.com'],
    ['post', 'http://example.com'],
])
@pytest.mark.asyncio
async def test_add_request(method, url, spider: aiocrawler.Spider, engine: aiocrawler.Engine, engine_spider):
    created_request = aiocrawler.Request(url=url, method=method)

    await spider.add_request(created_request)

    received_request = await engine._spider_state[spider]['requests'].get()
    assert received_request is created_request
