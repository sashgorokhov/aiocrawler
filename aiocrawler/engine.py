import asyncio
import inspect
import logging
import operator
import signal
import time
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
        self.loop = asyncio.new_event_loop()
        self.loop.set_exception_handler(self.engine_exception)
        self.loop.add_signal_handler(signal.SIGHUP, self.shutdown)
        self.loop.add_signal_handler(signal.SIGTERM, self.shutdown)
        self.downloader = Downloader(self)

    @property
    def logger(self):
        return logging.getLogger('engine')

    def start(self):
        self.loop.run_in_executor(None, self.downloader.start)

        for spider in self.spiders:
            self.loop.create_task(self.start_spider(spider))
        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            pass
        except CancelledError:
            pass
        finally:
            self.logger.info('Shutting down...')
            self.shutdown()

    async def add_request(self, spider, request):
        self.downloader.loop.call_soon_threadsafe(self.downloader._add_request, spider, request)

    def _process_response(self, spider, response):
        self.loop.create_task(self.process_response(spider, response))

    async def process_response(self, spider, response):
        """
        :param aiocrawler.spider.Spider spider:
        :param aiocrawler.http.Response response:
        """
        callback = response.callback or spider.process_response

        if not inspect.iscoroutinefunction(callback):
            raise TypeError('%s is not a coroutine function' % callback)

        try:
            await callback(response)
        except:
            self.logger.exception('Error while executing callback %s, spider %s, %s %s',
                                  callback.__name__, spider, response.method, response.url)

    async def start_spider(self, spider):
        try:
            await spider.start()
        except:
            self.logger.exception('Error while starting spider %s', spider)

    def add_spider(self, spider):
        """
        :param aiocrawler.spider.Spider spider:
        """
        if spider not in self.spiders:
            self.spiders.append(spider)

    def shutdown(self):
        if self.is_shutdown:
            return
        self.is_shutdown = True

        list(map(operator.methodcaller('cancel'), asyncio.Task.all_tasks(loop=self.loop)))

        while asyncio.Task.all_tasks(loop=self.loop):
            time.sleep(1)
            list(map(operator.methodcaller('cancel'), asyncio.Task.all_tasks(loop=self.loop)))

        self.loop.stop()
        self.downloader.shutdown()

        try:  # TODO: Remove this shit
            self.loop._signal_handlers.clear()
        except AttributeError:
            pass

    def engine_exception(self, loop, context):
        self.logger.exception('Error in engine')
