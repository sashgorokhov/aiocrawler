from unittest import mock

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
