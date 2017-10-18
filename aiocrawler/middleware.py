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

        self._middlewares = defaultdict(list)
        self._methods = defaultdict(lambda: defaultdict(list))

    @classmethod
    def from_engine(cls, engine):
        """
        :param aiocrawler.engine.Engine engine:
        """
        middleware_manager = cls(engine)
        engine.signals.connect(middleware_manager.spider_opened, signals.spider_opened)
        engine.signals.connect(middleware_manager.spider_error, signals.spider_error)
        engine.signals.connect(middleware_manager.spider_closed, signals.spider_closed)
        engine.signals.connect(middleware_manager.engine_started, signals.engine_started)
        engine.signals.connect(middleware_manager.engine_stopped, signals.engine_stopped)
        return middleware_manager

    def add_middleware(self, spider, middleware):
        if isinstance(middleware, type):
            if hasattr(middleware, 'from_engine'):
                middleware = middleware.from_engine(self.engine)
            else:
                middleware = middleware()

        self._middlewares[spider].append(middleware)
        self.add_middleware_methods(spider, middleware)

    def add_middleware_methods(self, spider, middleware):
        """
        Add several middleware methods
        """
        self.add_middleware_method('spider_opened', spider, middleware)
        self.add_middleware_method('spider_error', spider, middleware)
        self.add_middleware_method('spider_closed', spider, middleware)
        self.add_middleware_method('engine_started', None, middleware)
        self.add_middleware_method('engine_stopped', None, middleware)

    def add_middleware_method(self, name, spider, middleware):
        """
        Add a middleware method, if it exists
        """
        if hasattr(middleware, name):
            self._add_method(name, spider, getattr(middleware, name))

    def _add_method(self, name, spider, method):
        self._methods[spider][name].append(method)

    def set_middlewares(self, spider, middlewares):
        """
        Clear internal middleware storage and set new middlewares

        :param list middlewares: list of middleware objects or classes
        """
        self._middlewares[spider].clear()
        self._methods[spider].clear()

        for middleware in middlewares:
            self.add_middleware(spider, middleware)

    async def _call_method(self, method, *args, **kwargs):
        if inspect.iscoroutinefunction(method):
            return await method(*args, **kwargs)
        elif inspect.isgeneratorfunction(method) or inspect.isasyncgenfunction(method):
            pass
        elif inspect.isfunction(method) or inspect.ismethod(method):
            return method(*args, **kwargs)

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
        for method in self._methods.get(kwargs.get('spider', None), {}).get(name, []):
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

        for method in self._methods.get(kwargs.get('spider', None), {}).get(name, []):
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

    async def spider_opened(self, spider):
        return await self.call_methods('spider_opened', spider=spider)

    async def spider_error(self, spider):
        return await self.call_methods('spider_error', spider=spider)

    async def spider_closed(self, spider):
        return await self.call_methods('spider_closed', spider=spider)

    async def engine_started(self, engine):
        return await self.call_methods('engine_started', engine=engine)

    async def engine_stopped(self, engine):
        return await self.call_methods('engine_stopped', engine=engine)
