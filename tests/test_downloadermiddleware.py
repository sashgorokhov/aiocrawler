import pytest
from yarl import URL

from aiocrawler.downloadermiddleware import DownloaderMiddleware
from aiocrawler.http import Request, Response
from tests.utils import MiddlewareMethodGenerator


@pytest.fixture()
def downloader_middleware_manager(engine):
    return DownloaderMiddleware.from_engine(engine)


@pytest.mark.asyncio
@pytest.mark.parametrize('methods', [
    [{}, {}],
    [{'kwargs': {'return_value': Request('')}}, {'called': False}],
    [{}, {'kwargs': {'return_value': Response('', URL('http://example.com'))}}, {'called': False}],
], ids=[
    'All methods called',
    'First method returns Request',
    'Second method returns Response'
])
async def test_process_request(methods, downloader_middleware_manager: DownloaderMiddleware, spider):
    methodgen = MiddlewareMethodGenerator(methods)
    methodgen.generate('process_request', downloader_middleware_manager)

    request = Request('http://example.com')
    results = await downloader_middleware_manager.process_request(spider, request)

    methodgen.assert_methods(results)


@pytest.mark.asyncio
@pytest.mark.parametrize('methods', [
    [{}, {}],
    [{'called': False}, {'kwargs': {'return_value': Request('')}}, {}],
    [{'called': False}, {'kwargs': {'return_value': Response('', URL('http://example.com'))}}, {}],
], ids=[
    'All methods called',
    'Second method returns Request',
    'First method returns Response'
])
async def test_process_response(methods, downloader_middleware_manager: DownloaderMiddleware, spider):
    methodgen = MiddlewareMethodGenerator(methods)
    methodgen.generate('process_response', downloader_middleware_manager)

    request = Request('http://example.com')
    response = Response('GET', URL('http://example.com'))

    results = await downloader_middleware_manager.process_response(spider, request, response)

    methodgen.assert_methods(results)
