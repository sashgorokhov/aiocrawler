from aiocrawler import middleware, http


class DownloaderMiddleware(middleware.MiddlewareManager):
    def add_middleware_methods(self, spider, middleware):
        super(DownloaderMiddleware, self).add_middleware_methods(spider, middleware)
        self.add_middleware_method('process_request', spider, middleware)
        self.add_middleware_method('process_response', spider, middleware)

    async def process_request(self, spider, request):
        for method in self._methods.get(spider, {}).get('process_request', []):
            result = await self._call_method(method, spider=spider, request=request)
            if isinstance(result, (http.Request, http.Response)):
                return result
            elif result is not None:
                raise ValueError('Middleware %s returned unsupported value: %s' % (str(method), str(result)))

    async def process_response(self, spider, request, response):
        for method in reversed(self._methods.get(spider, {}).get('process_response', [])):
            result = await self._call_method(method, spider=spider, request=request, response=response)
            if isinstance(result, (http.Request, http.Response)):
                return result
            elif result is not None:
                raise ValueError('Middleware %s returned unsupported value: %s' % (str(method), str(result)))
