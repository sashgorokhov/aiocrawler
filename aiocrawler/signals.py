import asyncio
import inspect
import logging

from pydispatch import dispatcher

logger = logging.getLogger(__name__)


class SignalManager:
    def __init__(self, sender=dispatcher.Anonymous):
        self.sender = sender

    @classmethod
    def from_engine(cls, engine):
        return cls(engine)

    def connect(self, receiver, signal, **kwargs):
        """
        Connect a receiver to specified signal.
        Receiver can be either coroutine function or a regular function.

        :param kwargs: Passed to `pydispatch.dispatcher.connect`
        """
        kwargs.setdefault('sender', self.sender)
        dispatcher.connect(receiver, signal, **kwargs)

    def disconnect(self, receiver, signal, **kwargs):
        """
        This is pretty self-explanatory

        :param kwargs: Passed to `pydispatch.dispatcher.disconnect`
        """
        kwargs.setdefault('sender', self.sender)
        dispatcher.disconnect(receiver, signal, **kwargs)

    def _get_receivers(self, sender, signal):
        """
        Return receivers for specified sender and signal

        :return: generator over receivers
        """
        return dispatcher.liveReceivers(dispatcher.getAllReceivers(sender, signal))

    def _get_receiver_coros(self, sender, signal, catcherr=True, **kwargs):
        """
        Return coroutines that wrap receivers for specified sender and signal

        :return: generator over coroutines that call receiver functions
        """
        for receiver in self._get_receivers(sender, signal):
            yield self._call_receiver(receiver, catcherr=catcherr, **kwargs)

    async def send(self, signal, catcherr=True, **kwargs):
        """
        Send a signal.
        This will wait until all receivers execute and return their results as a list.

        :param bool catcherr: Suppress exceptions raised by receivers.
        :param kwargs: Passed to receiver
        :return: List of results of signal handlers
        """
        sender = kwargs.get('sender', self.sender)
        coros = self._get_receiver_coros(sender, signal, catcherr=catcherr, **kwargs)
        return await asyncio.gather(*coros)

    async def send_async(self, signal, catcherr=True, **kwargs):
        """
        Send a signal asynchronously.
        This will not wait until all receivers will complete.

        :param bool catcherr: Suppress exceptions raised by receivers
        :param kwargs: Passed to receiver
        :return: A task that resolves into list of results of signal handlers, once they are completed
        """
        return asyncio.ensure_future(self.send(signal, catcherr=catcherr, **kwargs))

    async def _call_receiver(self, receiver, catcherr=True, **kwargs):
        """
        A wrapper around receiver function for proper execution of coroutine and non-coroutine functions
        and error handling.

        :param bool catcherr: Suppress exceptions raised by receiver
        :param kwargs: Passed to receiver
        :return: Receiver results
        """
        try:
            if inspect.iscoroutinefunction(receiver):
                return await receiver(**kwargs)
            elif inspect.isfunction(receiver) or inspect.ismethod(receiver):
                return receiver(**kwargs)
            else:
                raise TypeError('Unhandled receiver %s type: %s' % (receiver.__name__, type(receiver)))
        except:
            if catcherr:
                logging.exception('Error while calling signal receiver %s', receiver.__name__)
            else:
                raise


engine_started = object()
engine_stopped = object()

spider_opened = object()
spider_error = object()
spider_closed = object()

request_received = object()
response_received = object()

item_scraped = object()
item_dropped = object()
