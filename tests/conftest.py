from unittest import mock

import pydispatch.dispatcher
import pytest

import aiocrawler
from aiocrawler.signals import SignalManager


class MockSpider(aiocrawler.Spider):
    def __init__(self, *args, **kwargs):
        self.start_mock = mock.Mock()
        self.process_response_mock = mock.Mock()

        super(MockSpider, self).__init__(*args, **kwargs)

    async def start(self):
        return self.start_mock()

    async def process_response(self, response):
        return self.process_response_mock(response)


@pytest.fixture()
def signal_manager(event_loop):
    sender = 'Test'

    manager = SignalManager(sender=sender, loop=event_loop)
    try:
        yield manager
    finally:
        manager.disconnect_all(sender=sender, signal=pydispatch.dispatcher.Any)


@pytest.fixture()
def engine(event_loop, signal_manager):
    engine_obj = aiocrawler.Engine(loop=event_loop)
    engine_obj.signals = signal_manager
    return engine_obj


@pytest.fixture()
def spider(engine):
    return MockSpider.from_engine(engine)


@pytest.fixture()
def engine_spider(spider: aiocrawler.Spider, engine: aiocrawler.Engine):
    engine.add_spider(spider)
