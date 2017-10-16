import asyncio
from unittest import mock

import pytest

from aiocrawler.signals import SignalManager


@pytest.fixture()
def signal_manager(event_loop):
    return SignalManager(loop=event_loop)


@pytest.mark.asyncio
@pytest.mark.parametrize('is_coroutine', [
    True, False
], ids=['is coroutine', 'is function'])
async def test_call_receiver(is_coroutine, signal_manager: SignalManager):
    kwargs = {'foo': 'bar'}
    retval = object()
    mock_obj = mock.Mock(return_value=retval)

    if is_coroutine:
        async def receiver(*args, **kwargs):
            return mock_obj(*args, **kwargs)
    else:
        def receiver(*args, **kwargs):
            return mock_obj(*args, **kwargs)

    assert await signal_manager._call_receiver(receiver, **kwargs) == retval

    mock_obj.assert_called_once_with(**kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize('catcherr', [
    True, False
], ids=['catch', 'raise'])
async def test_call_receiver_function_raises(catcherr, signal_manager: SignalManager):
    kwargs = {'foo': 'bar'}
    side_effect = ValueError('Test')
    mock_obj = mock.Mock(side_effect=side_effect)

    def receiver(*args, **kwargs):
        return mock_obj(*args, **kwargs)

    if catcherr:
        assert await signal_manager._call_receiver(receiver, catcherr=catcherr, **kwargs) is None
    else:
        with pytest.raises(type(side_effect)):
            assert await signal_manager._call_receiver(receiver, catcherr=catcherr, **kwargs)

    mock_obj.assert_called_once_with(**kwargs)


@pytest.mark.asyncio
async def test_call_receiver_unhandled_type(signal_manager):
    async def receiver(*args, **kwargs):
        yield None

    with pytest.raises(TypeError) as e:
        await signal_manager._call_receiver(receiver, catcherr=False)


@pytest.mark.asyncio
async def test_send_signal(signal_manager: SignalManager):
    signal = object()
    retval = object()

    def receiver():
        return retval

    signal_manager.connect(receiver, signal=signal)

    assert retval in await signal_manager.send(signal)


@pytest.mark.asyncio
async def test_send_async_signal(signal_manager: SignalManager):
    signal = object()
    retval = object()

    def receiver():
        return retval

    signal_manager.connect(receiver, signal=signal)

    task = await signal_manager.send_async(signal)
    assert isinstance(task, asyncio.Task)
    assert await task == [retval]
