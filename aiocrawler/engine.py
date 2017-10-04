import asyncio
import collections
import functools
import inspect
import logging
import signal
from concurrent.futures import CancelledError


class Engine:
    """
    Runs event loop, schedules spiders and coordinates their work.

    :param asyncio.base_events.BaseEventLoop loop:
    :param bool is_shutdown: Indicates that engine has stopped
    :param int global_concurrent_requests_limit: Limit engine-wide concurrent requests
    :param list[Spider] spiders: List of spiders


    :param dict[aiocrawler.spider.Spider, aiocrawler.http.Session] _spider_sessions:
    :param dict[aiocrawler.spider.Spider, asyncio.Queue] _spider_queues:
    """
    is_shutdown = False
    global_concurrent_requests_limit = 100

    def __init__(self, loop=None):
        """
        :param asyncio.base_events.BaseEventLoop loop: custom event loop to work in
        """
        self.spiders = []

        self.loop = loop or asyncio.get_event_loop()
        self.loop.set_exception_handler(self._engine_exception_handler)
        self.loop.add_signal_handler(signal.SIGHUP, self.shutdown)
        self.loop.add_signal_handler(signal.SIGTERM, self.shutdown)

        self._watched_futures = collections.defaultdict(list)

        self._requests_semaphore = asyncio.Semaphore(self.global_concurrent_requests_limit)
        self._spider_sessions = dict()
        self._spider_state = dict()
        self._spider_queues = collections.defaultdict(functools.partial(asyncio.Queue, loop=self.loop))

        self._shutdown_lock = asyncio.Lock(loop=self.loop)

    async def add_request(self, spider, request):
        """
        Add request to queue to process it.

        :param aiocrawler.spider.Spider spider: Spider that generated that request
        :param aiocrawler.http.Request request: Request to execute
        """
        self.logger.debug('Enqueued request from spider "%s": %s %s', spider, request.method, request.url)
        await self._spider_queues[spider].put(request)
        return request

    def start(self):
        """
        Runs event loop.
        Blocks until all spiders are closed.
        """
        for spider in self.spiders:
            task = self.loop.create_task(self.start_spider(spider))
            self.watch_future(spider, task)
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

    def watch_future(self, spider, future):
        """
        Add future to track its progress. Spider wont be closed until all futures
        for that spider will complete.

        :param aiocrawler.spider.Spider spider:
        :param asyncio.Future|asyncio.Task future:
        :return:
        """
        future.add_done_callback(
            lambda future:
            self.loop.create_task(
                self._attempt_close_spider(spider)
            )
        )
        self._watched_futures[spider].append(future)

    async def _download_loop(self):
        while not self.is_shutdown:
            for spider, queue in self._spider_queues.items():
                if queue.empty() or self._spider_state[spider]['requests_semaphore'].locked():
                    continue

                try:
                    await self._requests_semaphore.acquire()
                    await self._spider_state[spider]['requests_semaphore'].acquire()

                    request = await queue.get()
                    task = self.loop.create_task(self.process_request(spider, request))
                    self.watch_future(spider, task)
                except:
                    self._spider_state[spider]['requests_semaphore'].release()
                    self._requests_semaphore.release()

            await asyncio.sleep(0.1)

    async def process_request(self, spider, request):
        """
        Executes request and spider callback

        :param aiocrawler.spider.Spider spider:
        :param aiocrawler.http.Request request:
        """
        self.logger.debug('Started processing request from spider "%s": %s %s', spider, request.method, request.url)
        try:
            session = self.get_session(spider)
            request.kwargs.setdefault('timeout', 30)
            async with session.request(request.method, request.url, **request.kwargs) as response:
                response.request = request
                response.meta = request.meta.copy()
                response.callback = request.callback
                await self.process_response(spider, response)
        except:
            self.logger.exception('Error while processing request from spider "%s": %s %s',
                                  spider, request.method, request.url)
        finally:
            self._spider_state[spider]['requests_semaphore'].release()
            self._requests_semaphore.release()

    async def process_response(self, spider, response):
        """
        Processes received response by calling associated spider callback.
        If not set, default `spider.process_response` will be used.
        Callback is expected to be a coroutine function.

        :param aiocrawler.spider.Spider spider:
        :param aiocrawler.http.Response response:
        """
        callback = response.callback or spider.process_response

        try:
            if inspect.iscoroutinefunction(callback):
                await callback(response)
            elif inspect.isfunction(callback) or inspect.ismethod(callback):
                callback(response)
        except:
            self.logger.exception('Error while executing callback %s, spider %s, %s %s',
                                  callback.__name__, spider, response.method, response.url)

    def add_spider(self, spider):
        """
        Add spider to engine's spiders list

        :param aiocrawler.spider.Spider spider:
        """
        if spider not in self.spiders:
            self.spiders.append(spider)

    async def start_spider(self, spider):
        """
        Start spider by calling spider's `start` method.

        :param aiocrawler.spider.Spider spider:
        """
        self.logger.info('Starting spider "%s"', spider)
        self._spider_state[spider] = {
            'close_lock': asyncio.Lock(loop=self.loop),
            'requests_semaphore': asyncio.Semaphore(spider.concurrent_requests_limit, loop=self.loop)
        }
        try:
            await spider.start()
        except:
            self.logger.exception('Error while starting spider %s', str(spider))

    def _is_spider_finished(self, spider):
        """
        Determine if spider has finished its work

        :param aiocrawler.spider.Spider spider:
        :rtype: bool
        """
        pending_futures = self._clear_watched_futures(spider)
        return not len(pending_futures) and self._spider_queues[spider].empty()

    async def _attempt_close_spider(self, spider):
        """
        Close spider if it has finished all the work.
        If all spiders have closed, schedule engine shutdown.

        :param aiocrawler.spider.Spider spider:
        """
        with (await self._shutdown_lock):
            with (await self._spider_state[spider]['close_lock']):
                if self._is_spider_finished(spider):
                    self.close_spider(spider)
                if all(spider.closed for spider in self.spiders):
                    self.loop.call_soon_threadsafe(self.shutdown)

    def close_spider(self, spider):
        """
        Close spider.

        :param aiocrawler.spider.Spider spider:
        """
        if spider.closed:
            return
        self.logger.info('Closing spider "%s"', spider)
        spider.closed = True
        try:
            spider.close_spider()
        except:
            self.logger.exception('Error while closing spider "%s"', spider)

    def shutdown(self):
        """
        Shutdown engine, cancel all pending tasks and perform some tear down actions,
        like closing spiders and sessions.
        Does not stops the event loop.

        :return:
        """
        if self.is_shutdown:
            return
        self.logger.info('Shutting down')
        self.is_shutdown = True

        for spider in self.spiders:
            self.close_spider(spider)

        for session in self._spider_sessions.values():
            if not session.closed:
                session.close()

        for task in asyncio.Task.all_tasks():
            task.cancel()

    def _engine_exception_handler(self, loop, context):
        self.logger.error('Uncaught exception: %s', str(context))

    def _clear_watched_futures(self, spider):
        self._watched_futures[spider] = list(filter(lambda f: not f.done(), self._watched_futures[spider]))
        return self._watched_futures[spider]

    @property
    def logger(self):
        return logging.getLogger('Engine')

    def get_session(self, spider):
        """
        Return a session for given spider.

        :param aiocrawler.spider.Spider spider:
        """
        return spider.session
