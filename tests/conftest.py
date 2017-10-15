import pytest

import aiocrawler


@pytest.fixture()
def engine(event_loop):
    return aiocrawler.Engine(loop=event_loop)


@pytest.fixture()
def spider(engine):
    return aiocrawler.Spider.from_engine(engine)


@pytest.fixture()
def engine_spider(spider: aiocrawler.Spider, engine: aiocrawler.Engine):
    engine.add_spider(spider)
