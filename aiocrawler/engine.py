import asyncio
import collections
import inspect
import logging
import signal
from concurrent.futures import CancelledError

from aiocrawler import http
from aiocrawler.spider import Spider


class Engine:
    """
    :param asyncio.base_events.BaseEventLoop loop:
    """
    is_shutdown = False
    _global_concurrent_requests_limit = 20

    def __init__(self, loop=None, spiders=None):
        if spiders and isinstance(spiders, Spider):
            spiders = [spiders]
        self.spiders = spiders or []
        self._watched_futures = []

        self.loop = loop or asyncio.get_event_loop()
        self.loop.set_exception_handler(self._engine_exception_handler)
        self.loop.add_signal_handler(signal.SIGHUP, self.shutdown)
        self.loop.add_signal_handler(signal.SIGTERM, self.shutdown)

        self._queue = asyncio.Queue(loop=self.loop)
        self._requests_semaphore = asyncio.Semaphore(self._global_concurrent_requests_limit)
        self._spider_sessions = dict()

        self._shutdown_lock = asyncio.Lock(loop=self.loop)

        self._spider_state = collections.defaultdict(lambda: {
            'lock': asyncio.Lock(loop=self.loop),
        })

    async def add_request(self, spider, request):
        """
        :param aiocrawler.spider.Spider spider:
        :param aiocrawler.http.Request request:
        """
        self.logger.debug('Enqueued request from spider "%s": %s %s', spider, request.method, request.url)
        return await self._queue.put((spider, request))

    def start(self):
        for spider in self.spiders:
            self.loop.create_task(self.start_spider(spider))
        try:
            self.loop.run_until_complete(self._download_loop())
        except KeyboardInterrupt:
            self.logger.warning('Loop is cancelled by user via KeyboardInterrupt')
        except CancelledError:
            pass
        finally:
            self.logger.info('Loop stopped')
            self.shutdown()
            if not self.loop.is_closed():
                self.loop.close()

    def watch_future(self, fututre):
        """

        :param asyncio.Future|asyncio.Task fututre:
        :return:
        """
        self._watched_futures.append(fututre)

    async def _download_loop(self):
        while not self.is_shutdown:
            while not self._queue.empty():
                with (await self._shutdown_lock):
                    try:
                        await self._requests_semaphore.acquire()
                        spider, request = await self._queue.get()
                        task = self.loop.create_task(self.process_request(spider, request))
                        self.watch_future(task)
                    except:
                        self._requests_semaphore.release()
            await asyncio.sleep(1)

    async def process_request(self, spider, request):
        """
        :param aiocrawler.spider.Spider spider:
        :param aiocrawler.http.Request request:
        """
        self.logger.debug('Started processing request from spider "%s": %s %s', spider, request.method, request.url)
        try:
            session = self.get_session(spider)
            response = await session.execute_request(request)
            await self.process_response(spider, response)
        except:
            self.logger.exception('Error while processing request from spider "%s": %s %s',
                                  spider, request.method, request.url)
        finally:
            self._requests_semaphore.release()
            asyncio.ensure_future(self._attempt_close_spider(spider), loop=self.loop)

    async def process_response(self, spider, response):
        """
        :param aiocrawler.spider.Spider spider:
        :param aiocrawler.http.Response response:
        """
        callback = response.callback or spider.process_response

        try:
            if not inspect.iscoroutinefunction(callback):
                raise TypeError('%s is not a coroutine function' % str(callback))

            await callback(response)
        except:
            self.logger.exception('Error while executing callback %s, spider %s, %s %s',
                                  callback.__name__, spider, response.method, response.url)

    def add_spider(self, spider):
        """
        :param aiocrawler.spider.Spider spider:
        """
        if spider not in self.spiders:
            self.spiders.append(spider)

    async def start_spider(self, spider):
        """
        :param aiocrawler.spider.Spider spider:
        """
        self.logger.info('Starting spider "%s"', spider)
        try:
            await spider.start()
        except:
            self.logger.exception('Error while starting spider %s', str(spider))
        finally:
            asyncio.ensure_future(self._attempt_close_spider(spider), loop=self.loop)

    async def _attempt_close_spider(self, spider):
        """
        :param aiocrawler.spider.Spider spider:
        """
        with (await self._shutdown_lock):
            with (await self._spider_state[spider]['lock']):
                self._clear_watched_futures()
                if not len(self._watched_futures) and self._queue.empty():
                    self.close_spider(spider)
                    if all(spider.closed for spider in self.spiders):
                        self.loop.call_soon_threadsafe(self.shutdown)

    def close_spider(self, spider):
        """
        :param aiocrawler.spider.Spider spider:
        """
        self.logger.info('Closing spider "%s"', spider)
        spider.closed = True
        try:
            spider.close_spider()
        except:
            self.logger.exception('Error while closing spider "%s"', spider)

    def shutdown(self):
        if self.is_shutdown:
            return
        self.logger.info('Shutting down')
        self.is_shutdown = True

        for session in self._spider_sessions.values():
            if not session.closed:
                session.close()

        for task in asyncio.Task.all_tasks():
            task.cancel()

    def _engine_exception_handler(self, loop, context):
        self.logger.error('Uncaught exception: %s', str(context))

    def _clear_watched_futures(self):
        self._watched_futures = list(filter(lambda f: not f.done(), self._watched_futures))
        return self._watched_futures

    @property
    def logger(self):
        return logging.getLogger('Engine')

    def create_session(self, spider):
        """
        :param aiocrawler.spider.Spider spider:
        """
        return http.Session(loop=self.loop)

    def get_session(self, spider):
        """
        :param aiocrawler.spider.Spider spider:
        """
        if spider not in self._spider_sessions:
            self._spider_sessions[spider] = self.create_session(spider)
        return self._spider_sessions[spider]
