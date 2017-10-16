import asyncio
import collections
from unittest import mock

import pytest
from yarl import URL

import aiocrawler
from aiocrawler import signals
from aiocrawler.http import Response
from tests.conftest import MockSpider
from tests.utils import wrap_mock, mock_execute_request


@pytest.fixture()
def configured_engine(engine, engine_spider):
    return engine


@pytest.mark.asyncio
@pytest.mark.parametrize('signal_kwargs', [
    {},
    {'return_value': 'test_add_request'},
    {'side_effect': ValueError('test_add_request')}
])
async def test_add_request(signal_kwargs, configured_engine: aiocrawler.Engine, spider):
    mock_obj = mock.Mock(name='signal mock', **signal_kwargs)
    receiver = wrap_mock(mock_obj)
    configured_engine.signals.connect(receiver, signals.request_received)
    request = aiocrawler.Request('http://example.com')

    await configured_engine.add_request(spider, request)

    mock_obj.assert_called()
    assert await configured_engine._spider_state[spider]['requests'].get() == request


@pytest.mark.parametrize('signals_to_mock', [
    ['engine_started', 'engine_stopped',
     'spider_opened', 'spider_closed']
], ids=lambda v: str(v))
def test_start(signals_to_mock):
    engine = aiocrawler.Engine(loop=asyncio.new_event_loop())
    spider = MockSpider.from_engine(engine)
    engine.add_spider(spider)

    signal_mocks = collections.defaultdict(dict)

    for signal_name in signals_to_mock:
        signal_mocks[signal_name]['mock'] = mock.Mock(name=signal_name)
        signal_mocks[signal_name]['receiver'] = wrap_mock(signal_mocks[signal_name]['mock'])
        engine.signals.connect(signal_mocks[signal_name]['receiver'], getattr(signals, signal_name))

    engine.start()

    for signal_mock in signal_mocks.values():
        signal_mock['mock'].assert_called()

    spider.start_mock.assert_called()


@pytest.mark.asyncio
async def test_process_request(configured_engine: aiocrawler.Engine, spider):
    request = aiocrawler.Request('http://example.com', 'GET')
    response = Response('GET', URL('http://example.com'))

    process_response_mock = mock.Mock(name='process_response_mock')
    response.callback = wrap_mock(process_response_mock)
    response_received_mock = mock.Mock(name='response_received_mock')
    response_received_receiver = wrap_mock(response_received_mock)
    configured_engine.signals.connect(response_received_receiver, signals.response_received)

    downloader_middleware_mock = mock.Mock(name='downloader_middleware_mock', return_value=None)

    configured_engine._spider_state[spider]['downloader_middleware']._add_method('process_request',
                                                                                 wrap_mock(downloader_middleware_mock))
    configured_engine._spider_state[spider]['downloader_middleware']._add_method('process_response',
                                                                                 wrap_mock(downloader_middleware_mock))

    with mock_execute_request(response):
        await configured_engine.process_request(spider, request)

    process_response_mock.assert_called()
    response_received_mock.assert_called()
    downloader_middleware_mock.assert_called()
    assert downloader_middleware_mock.call_count == 2
