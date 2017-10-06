import asyncio
import collections
import inspect
import logging
import signal
import sys
from concurrent.futures import CancelledError

from aiocrawler import signals


class Engine:
    """
    Runs event loop, schedules spiders and coordinates their work.

    :param asyncio.base_events.BaseEventLoop loop:
    :param bool is_shutdown: Indicates that engine has stopped
    :param int global_concurrent_requests_limit: Limit engine-wide concurrent requests
    :param list[Spider] spiders: List of spiders
    :param aiocrawler.signals.SignalManager signals: Signals manager
    """
    is_shutdown = False
    global_concurrent_requests_limit = 100
    global_concurrent_pipelines_limit = 1000

    def __init__(self, loop=None):
        """
        :param asyncio.base_events.BaseEventLoop loop: custom event loop to work in
        """
        self.spiders = []

        self.loop = loop or asyncio.get_event_loop()
        self.loop.set_exception_handler(self._engine_exception_handler)
        self.loop.add_signal_handler(signal.SIGHUP, lambda: self.loop.run_until_complete(self.shutdown()))
        self.loop.add_signal_handler(signal.SIGTERM, lambda: self.loop.run_until_complete(self.shutdown()))

        self._watched_futures = collections.defaultdict(list)
        self._requests_semaphore = asyncio.Semaphore(self.global_concurrent_requests_limit)
        self._pipelines_semaphore = asyncio.Semaphore(self.global_concurrent_pipelines_limit)
        self._spider_state = dict()
        self._shutdown_lock = asyncio.Lock(loop=self.loop)

        self.signals = signals.SignalManager(self)

    async def add_request(self, spider, request):
        """
        Add request to queue to process it.

        :param aiocrawler.spider.Spider spider: Spider that generated that request
        :param aiocrawler.http.Request request: Request to execute
        """
        await self.signals.send(signals.request_received, spider=spider, request=request)
        await self._spider_state[spider]['requests'].put(request)
        self.logger.debug('Enqueued request from spider "%s": %s %s', spider, request.method, request.url)

    def start(self):
        """
        Runs event loop.
        Blocks until all spiders are closed.
        """
        self.loop.run_until_complete(self.signals.send(signals.engine_started, engine=self))

        for spider in self.spiders:
            task = self.loop.create_task(self.start_spider(spider))
            self.watch_future(spider, task)
        try:
            self.loop.run_until_complete(asyncio.gather(
                self._download_loop(),
                self._pipeline_loop(),
                loop=self.loop))
        except KeyboardInterrupt:
            self.logger.warning('Loop is cancelled by user via KeyboardInterrupt')
        except CancelledError:
            pass
        finally:
            self.logger.debug('Loop stopped')
            self.loop.run_until_complete(self.shutdown())
            self.loop.run_until_complete(self.signals.send(signals.engine_stopped, engine=self))
            for task in asyncio.Task.all_tasks():
                task.cancel()
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
            for spider in self.spiders:
                queue = self._spider_state[spider]['requests']
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

    async def _pipeline_loop(self):
        while not self.is_shutdown:
            for spider in self.spiders:
                await self._pipelines_semaphore.acquire()
                try:
                    item = await self._spider_state[spider]['items'].get()
                    task = self.loop.create_task(self.process_item(spider, item))
                    self.watch_future(spider, task)
                except:
                    self._pipelines_semaphore.release()
            await asyncio.sleep(0.1)

    async def process_item(self, spider, item):
        await self.signals.send(signals.item_scraped, spider=spider, item=item)

        try:
            # TODO: Process with item pipelines
            await spider.process_item(item)
        except:
            self.logger.exception('Error while processing item in spider "%s"')
            self.logger.debug('Failed item: %s', item)

    async def process_request(self, spider, request):
        """
        Executes request and processes response

        :param aiocrawler.spider.Spider spider:
        :param aiocrawler.http.Request request:
        """
        self.logger.debug('Started processing request from spider "%s": %s %s', spider, request.method, request.url)
        try:
            session = self.get_session(spider)
            async with session.execute_request(request) as response:
                await self.signals.send(signals.response_received, spider=spider, request=request, response=response)
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

        :param aiocrawler.spider.Spider spider:
        :param aiocrawler.http.Response response:
        """
        callback = response.callback or spider.process_response

        try:
            await self._execute_spider_callback(spider, callback, response)
        except:
            await self.signals.send_async(signals.spider_error, spider=spider, response=response,
                                          exc_info=sys.exc_info())
            self.logger.exception('Error while executing callback %s, spider %s, %s %s',
                                  callback.__name__, spider, response.method, response.url)

    async def _execute_spider_callback(self, spider, callback, *args, **kwargs):
        """
        Determines how to execute a callback and how to handle its results.

        :param aiocrawler.spider.Spider spider:
        :param callback:
        :param args: Passed to callback
        :param kwargs: Passed to callback
        """
        if inspect.iscoroutinefunction(callback):
            result = await callback(*args, **kwargs)
            await self._process_simple_callback_result(spider, result)
        elif inspect.isasyncgenfunction(callback):
            async for item in callback(*args, **kwargs):
                await self.add_item(spider, item)
        elif inspect.isgeneratorfunction(callback):
            for item in callback(*args, **kwargs):
                await self.add_item(spider, item)
        elif inspect.isfunction(callback) or inspect.ismethod(callback):
            result = callback(*args, **kwargs)
            await self._process_simple_callback_result(spider, result)
        else:
            raise TypeError('Unhandled callback type "%s": %s' % (callback.__name__, type(callback)))

    async def _process_simple_callback_result(self, spider, result):
        if result:
            if isinstance(result, collections.Iterable):
                for item in result:
                    await self.add_item(spider, item)
            else:
                await self.add_item(spider, result)

    async def add_item(self, spider, item):
        """
        Puts scraped item into internal queue to do further processing asynchronously,

        :param aiocrawler.spider.Spider spider:
        :param item:
        """
        await self._spider_state[spider]['items'].put(item)

    def add_spider(self, spider):
        """
        Add spider to engine's spiders list

        :param aiocrawler.spider.Spider spider:
        """
        if spider not in self.spiders:
            self.spiders.append(spider)

    def _init_spider_state(self, spider):
        """
        Return initial spider state

        :param spider:
        :rtype: dict[aiocrawler.spider.Spider, dict]
        """
        return {
            'close_lock': asyncio.Lock(loop=self.loop),
            'requests_semaphore': asyncio.Semaphore(spider.concurrent_requests_limit, loop=self.loop),
            'requests': asyncio.Queue(loop=self.loop),
            'items': asyncio.Queue(loop=self.loop),
        }

    async def start_spider(self, spider):
        """
        Start spider by calling spider's `start` method.

        :param aiocrawler.spider.Spider spider:
        """
        self.logger.info('Starting spider "%s"', spider)

        try:
            self._spider_state[spider] = self._init_spider_state(spider)
            await self.signals.send(signals.spider_opened, spider=spider)
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
        return not len(pending_futures) and self._spider_state[spider]['requests'].empty()

    async def _attempt_close_spider(self, spider):
        """
        Close spider if it has finished all the work.
        If all spiders have closed, schedule engine shutdown.

        :param aiocrawler.spider.Spider spider:
        """
        with (await self._shutdown_lock):
            with (await self._spider_state[spider]['close_lock']):
                if self._is_spider_finished(spider):
                    await self.close_spider(spider)
                if all(spider.closed for spider in self.spiders):
                    self.loop.create_task(self.shutdown())

    async def close_spider(self, spider):
        """
        Close spider.

        :param aiocrawler.spider.Spider spider:
        """
        if spider.closed:
            return
        self.logger.info('Closing spider "%s"', spider)
        spider.closed = True
        await self.signals.send(signals.spider_closed, spider=spider)
        try:
            spider.close_spider()
            if not spider.session.closed:
                spider.session.close()
        except:
            self.logger.exception('Error while closing spider "%s"', spider)

    async def shutdown(self):
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
            if not spider.closed:
                await self.close_spider(spider)

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
        :rtype: aiocrawler.http.Session
        """
        return spider.session
