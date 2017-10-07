import asyncio
import inspect
from collections import defaultdict

from aiocrawler import signals


class MiddlewareManager:
    def __init__(self, engine):
        """
        :param aiocrawler.engine.Engine engine:
        """
        self.engine = engine

        self._middlewares = []
        self._methods = defaultdict(list)

    @classmethod
    def from_engine(cls, engine):
        """
        :param aiocrawler.engine.Engine engine:
        """
        middleware_manager = cls(engine)
        engine.signals.connect(middleware_manager.open_spider, signals.spider_opened)
        engine.signals.connect(middleware_manager.close_spider, signals.spider_closed)
        return middleware_manager

    def add_middleware(self, middleware):
        if isinstance(middleware, type):
            if hasattr(middleware, 'from_engine'):
                middleware = middleware.from_engine(self.engine)
            else:
                middleware = middleware()

        self._middlewares.append(middleware)
        self.add_middleware_methods(middleware)

    def add_middleware_methods(self, middleware):
        """
        Add several middleware methods
        """
        self.add_middleware_method('open_spider', middleware)
        self.add_middleware_method('close_spider', middleware)

    def add_middleware_method(self, name, middleware):
        """
        Add a middleware method, if it exists
        """
        if hasattr(middleware, name):
            self._methods[name].append(getattr(middleware, name))

    def set_middlewares(self, middlewares):
        """
        Clear internal middleware storage and set new middlewares

        :param list middlewares: list of middleware objects or classes
        """
        self._middlewares.clear()
        self._methods.clear()

        for middleware in middlewares:
            self.add_middleware(middleware)

    async def _call_method(self, method, *args, **kwargs):
        if inspect.iscoroutinefunction(method):
            return await method(*args, **kwargs)
        elif inspect.isfunction(method) or inspect.ismethod(method):
            return method(*args, **kwargs)
        else:
            raise TypeError('Unhandled middleware method %s type: %s', method.__name__, type(method))

    async def call_methods_async(self, name, *args, **kwargs):
        """
        Call middleware methods asynchronously.

        :param name: method name
        :param args: Passed to middleware method
        :param kwargs: Passed to middleware method
        :return: Task that resolves into middleware methods results
        """
        return asyncio.ensure_future(self.call_methods(name, *args, **kwargs))

    async def call_methods(self, name, *args, **kwargs):
        """
        Call middleware methods and return their results in a list

        :param name: method name
        :param args: Passed to middleware method
        :param kwargs: Passed to middleware method
        :return: middleware methods results
        """
        coros = list()
        for method in self._methods[name]:
            coros.append(self._call_method(method, *args, **kwargs))
        return await asyncio.gather(*coros)

    async def call_methods_chain(self, name, *args, **kwargs):
        """
        Call middleware methods one-by-one and return their results

        :param name: method name
        :param args: Passed to middleware method
        :param kwargs: Passed to middleware method
        :return: middleware methods results
        """
        results = list()

        for method in self._methods[name]:
            result = await self._call_method(method, *args, **kwargs)
            results.append(result)

        return results

    async def call_methods_chain_async(self, name, *args, **kwargs):
        """
        Call middleware methods one-by-one asynchronously

        :param name: method name
        :param args: Passed to middleware method
        :param kwargs: Passed to middleware method
        :return: Task that resolves into middleware methods results
        """
        return asyncio.ensure_future(self.call_methods_chain(name, *args, **kwargs))

    async def open_spider(self, spider):
        return await self.call_methods('open_spider', spider=spider)

    async def close_spider(self, spider):
        return await self.call_methods('close_spider', spider=spider)
