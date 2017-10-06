import logging

from aiocrawler import http
from aiocrawler.http import Request


class Spider:
    """
    :param aiocrawler.engine.Engine engine:
    """
    name = None
    closed = False
    _session = None
    _logger = None
    concurrent_requests_limit = 20

    def __init__(self, engine):
        self.engine = engine

    def get_name(self):
        """
        Return spider name. If not set, return current class name.
        """
        return self.name or self.__class__.__name__

    @property
    def logger(self):
        """
        Return logger associated with this spider

        :rtype: logging.Logger
        """
        if self._logger is None:
            self._logger = logging.getLogger(self.get_name())
        return self._logger

    async def add_request(self, request):
        """
        Add request to be executed asynchronously.

        :param aiocrawler.http.Request request:
        :return: Added request object
        """
        await self.engine.add_request(self, request)

    async def get(self, url, callback=None, meta=None, **kwargs):
        """
        Shortcut for creating GET request.
        Creates `Request` object and adds it to a queue.
        """
        request = Request(url=url, method='GET', callback=callback, meta=meta, **kwargs)
        await self.add_request(request)

    async def post(self, url, callback=None, meta=None, **kwargs):
        """
        Shortcut for creating POST request.
        Creates `Request` object and adds it to a queue.
        """
        request = Request(url=url, method='POST', callback=callback, meta=meta, **kwargs)
        await self.add_request(request)

    async def start(self):
        """
        Main spider entrypoint.
        Implement this method to schedule requests for processing.
        """
        raise NotImplementedError()

    def close_spider(self):
        """
        Called when spider is closed
        """
        pass

    async def process_response(self, response):
        """
        Default callback for processing responses.
        Implement this to process request's responses.

        :param aiocrawler.http.Response response:
        """
        raise NotImplementedError()

    def __str__(self):
        return self.get_name()

    def __repr__(self):
        return '<Spider "%s">' % self.get_name()

    def create_session(self):
        """
        Create session object to be used for http requests.

        :rtype: aiocrawler.http.Session
        """
        return http.Session(loop=self.engine.loop)

    @property
    def session(self):
        """
        Return current spider session

        :rtype: aiocrawler.http.Session
        """
        if self._session is None:
            self._session = self.create_session()
        return self._session

    async def process_item(self, item):
        """
        Process scraped item.
        """
        pass
