import asyncio
import collections
import inspect
import logging
import signal
from concurrent.futures import CancelledError

from aiocrawler.downloader import Downloader


class Engine:
    """
    :param asyncio.base_events.BaseEventLoop loop:
    :param aiocrawler.downloader.Downloader downloader:
    """
    is_shutdown = False

    def __init__(self):
        self.spiders = []
        self._futures = []

        self.loop = asyncio.new_event_loop()
        self.loop.set_exception_handler(self.engine_exception)
        self.loop.add_signal_handler(signal.SIGHUP, self.shutdown)
        self.loop.add_signal_handler(signal.SIGTERM, self.shutdown)
        self.downloader = Downloader(self)

        self._shutdown_lock = asyncio.Lock(loop=self.loop)

        self._spider_state = collections.defaultdict(lambda: {
            'lock': asyncio.Lock(loop=self.loop),
            'closed': False
        })

    def _clear_futures(self):
        self._futures = list(filter(lambda f: not f.done(), self._futures))
        return self._futures

    @property
    def logger(self):
        return logging.getLogger('engine')

    def start(self):
        for spider in self.spiders:
            self.loop.create_task(self.start_spider(spider))
        try:
            self.loop.run_until_complete(self.downloader.start_downloader())
        except KeyboardInterrupt:
            self.logger.warning('Loop is cancelled by user via KeyboardInterrupt')
        except CancelledError:
            self.logger.warning('Loop is cancelled')
        finally:
            self.logger.info('Loop stopped')
            self.shutdown()
            if not self.loop.is_closed():
                self.loop.close()

    async def add_request(self, spider, request):
        return await self.downloader.add_request(spider, request)

    async def process_response(self, spider, response):
        """
        :param aiocrawler.spider.Spider spider:
        :param aiocrawler.http.Response response:
        """
        callback = response.callback or spider.process_response

        try:
            if not inspect.iscoroutinefunction(callback):
                raise TypeError('%s is not a coroutine function' % callback)

            return await callback(response)
        except:
            self.logger.exception('Error while executing callback %s, spider %s, %s %s',
                                  callback.__name__, spider, response.method, response.url)
        finally:
            asyncio.ensure_future(self._attempt_close_spider(spider), loop=self.loop)

    async def start_spider(self, spider):
        try:
            await spider.start()
        except:
            self.logger.exception('Error while starting spider %s', str(spider))
        finally:
            asyncio.ensure_future(self._attempt_close_spider(spider), loop=self.loop)

    async def _attempt_close_spider(self, spider):
        with (await self._shutdown_lock):
            with (await self._spider_state[spider]['lock']):
                self._clear_futures()
                # self.logger.debug('Futures pending: %s', self._futures)
                # self.logger.debug('Downloader queue len: %s', self.downloader._queue.qsize())
                if not len(self._futures) and self.downloader._queue.empty():
                    self.loop.call_soon_threadsafe(self.shutdown)

    def add_spider(self, spider):
        """
        :param aiocrawler.spider.Spider spider:
        """
        if spider not in self.spiders:
            self.spiders.append(spider)

    def close_spider(self, spider):
        pass

    def shutdown(self):
        if self.is_shutdown:
            return
        self.logger.info('Shutting down')
        self.is_shutdown = True

        for task in asyncio.Task.all_tasks():
            task.cancel()

        # if self.loop.is_running():
        #    self.loop.stop()

        self.downloader.shutdown()

    def engine_exception(self, loop, context):
        self.logger.error('Error in engine: %s', str(context))

        # def _attempt_shutdown(self, spider):
        #    self.logger.debug('Attempting shutdown...')
        #    with (yield from self._shutdown_lock):
        #        if self.is_shutdown:
        #            return
