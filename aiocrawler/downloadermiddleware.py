from aiocrawler import middleware, http


class DownloaderMiddleware(middleware.MiddlewareManager):
    def add_middleware_methods(self, middleware):
        super(DownloaderMiddleware, self).add_middleware_methods(middleware)
        self.add_middleware_method('process_request', middleware)
        self.add_middleware_method('process_response', middleware)

    async def process_request(self, spider, request):
        for method in self._methods['process_request']:
            result = await self._call_method(method, spider=spider, request=request)
            if isinstance(result, (http.Request, http.Response)):
                return result
            elif result is not None:
                raise ValueError('Middleware %s returned unsupported value: %s' % (str(method), str(result)))

    async def process_response(self, spider, request, response):
        for method in reversed(self._methods['process_response']):
            result = await self._call_method(method, spider=spider, request=request, response=response)
            if isinstance(result, (http.Request, http.Response)):
                return result
            elif result is not None:
                raise ValueError('Middleware %s returned unsupported value: %s' % (str(method), str(result)))
