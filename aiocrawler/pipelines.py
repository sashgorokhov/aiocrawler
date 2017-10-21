import logging

from aiocrawler import exceptions, middleware

logger = logging.getLogger()


class ItemPipelineManager(middleware.MiddlewareManager):
    def add_middleware_methods(self, spider, middleware):
        super(ItemPipelineManager, self).add_middleware_methods(spider, middleware)
        self.add_middleware_method('process_item', spider, middleware)

    async def process_item(self, spider, item):
        for method in self._methods[spider]['process_item']:
            try:
                result = await self._call_method(method, spider=spider, item=item)
            except exceptions.DropItem:
                logger.info('Dropped item from spider "%s": %s', spider, str(item))
                return
            except:
                logger.exception('Error while executing pipeline %s', str(method))
                return

            if result is None:
                logger.error('Middleware %s returned nothing (None)' % str(method))
                return

            item = result

        return item
