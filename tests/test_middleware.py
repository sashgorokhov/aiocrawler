import asyncio
from unittest import mock

import pytest

from aiocrawler.middleware import MiddlewareManager


@pytest.fixture()
def middleware_manager(engine):
    return MiddlewareManager.from_engine(engine)


@pytest.mark.asyncio
@pytest.mark.parametrize('is_coroutine', [
    True, False
], ids=lambda v: ('is function', 'is coroutine')[int(v)])
async def test_call_method(is_coroutine, middleware_manager: MiddlewareManager):
    kwargs = {'foo': 'bar'}
    retval = object()
    mock_obj = mock.Mock(return_value=retval)

    if is_coroutine:
        async def method(*args, **kwargs):
            return mock_obj(*args, **kwargs)
    else:
        def method(*args, **kwargs):
            return mock_obj(*args, **kwargs)

    assert await middleware_manager._call_method(method, **kwargs) == retval

    mock_obj.assert_called_once_with(**kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize('method', [
    'open_spider',
    'close_spider'
])
@pytest.mark.parametrize('is_async', [
    True, False
], ids=['async', 'not async'])
@pytest.mark.parametrize('is_chain', [
    True, False
], ids=['chain', 'not chain'])
async def test_add_middleware(method, is_async, is_chain, middleware_manager: MiddlewareManager):
    mock_obj = mock.Mock(return_value=method)
    middleware_cls = type('TestMiddleware', (object,), {method: lambda *args, **kwargs: mock_obj(*args, **kwargs)})
    middleware_manager.add_middleware(middleware_cls)

    middleware_method = None

    if is_async:
        if is_chain:
            middleware_method = middleware_manager.call_methods_chain_async
        else:
            middleware_method = middleware_manager.call_methods_async
    else:
        if is_chain:
            middleware_method = middleware_manager.call_methods_chain
        else:
            middleware_method = middleware_manager.call_methods

    if is_async:
        task = await middleware_method(method)
        assert isinstance(task, asyncio.Task)
        assert method in await task
    else:
        assert method in await middleware_method(method)
