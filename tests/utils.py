from unittest import mock

import aiocrawler
from aiocrawler.middleware import MiddlewareManager


def wrap_mock_call(mock_obj):
    def wrapper(*args, **kwargs):
        return mock_obj(*args, **kwargs)

    return wrapper


class MiddlewareMethodGenerator:
    """
    This utility class will generate middlewares and methods by given spec.

    Default spec:

        {
            'kwargs': {'return_value': None},
            'called': True,
            'name': 0, # order in methodspec list
            'mock': <MagicMock object>
        }

    `kwargs` are passed to a `unittest.mock.MagicMock` constructor, and created mock is saved to a `mock` key.
    """

    def __init__(self, methodspec):
        """
        :param list[dict] methodspec:
        """
        self.methodspec = methodspec

    def generate(self, method_name, middleware_manager: MiddlewareManager):
        for n, method_data in enumerate(self.methodspec):
            method_data.setdefault('name', str(n))
            method_data.setdefault('kwargs', {})
            method_data['kwargs'].setdefault('return_value', None)
            method_data['mock'] = mock.MagicMock(**method_data['kwargs'])

            middleware_cls = type('TestMiddleware_%s' % n, (object,),
                                  {method_name: wrap_mock_call(method_data['mock'])})
            middleware_manager.add_middleware_method(method_name, middleware_cls)

    def assert_methods(self, results, check_results=True):
        for n, method_data in enumerate(self.methodspec):
            method_mock = method_data['mock']

            if method_data.get('called', True):
                method_mock.assert_called()
            else:
                method_mock.assert_not_called()

            return_value = method_data.get('kwargs', {}).get('return_value')
            if return_value is not None and check_results:
                assert results == return_value


def wrap_mock(mock_obj):
    def wrapper(*args, **kwargs):
        return mock_obj(*args, **kwargs)

    return wrapper


class MockAsyncContextManager:
    def __init__(self, enter_mock=None, exit_mock=None):
        self.enter_mock = enter_mock or mock.Mock()
        self.exit_mock = exit_mock or mock.Mock()

    async def __aenter__(self):
        return self.enter_mock()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return self.exit_mock(exc_type, exc_val, exc_tb)


def mock_execute_request(response):
    return mock.patch('aiocrawler.http.Session.execute_request', side_effect=lambda *args, **kwargs:
    MockAsyncContextManager(enter_mock=mock.Mock(return_value=response)))


class MockSpider(aiocrawler.Spider):
    def __init__(self, *args, **kwargs):
        self.start_mock = mock.Mock()
        self.process_response_mock = mock.Mock()

        super(MockSpider, self).__init__(*args, **kwargs)

    async def start(self):
        return self.start_mock()

    async def process_response(self, response):
        return self.process_response_mock(response)
