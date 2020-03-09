"""
:mod:`main`
===========

Defines the main ``Actor`` class and ``concurrent`` decorator for using arq from within your code.
"""
import inspect
import logging
from functools import wraps

from .jobs import Job, JobSerialisationError
from .utils import RedisMixin

__all__ = ['Actor', 'concurrent']

main_logger = logging.getLogger('arq.main')


class ActorMeta(type):
    def __new__(mcs, cls, bases, classdict):
        queues = classdict.get('queues')
        if queues and len(queues) != len(set(queues)):
            raise AssertionError('{} looks like it has duplicated queue names: {}'.format(cls, queues))
        return super().__new__(mcs, cls, bases, classdict)


class Actor(RedisMixin, metaclass=ActorMeta):
    """
    All classes which wish to use arq should inherit from Actor.

    Actor defines three default queues: HIGH_QUEUE, DEFAULT_QUEUE and LOW_QUEUE which are processed
    in that order of priority by the worker.

    Actors operate in two modes: normal mode when you initialise them and use them, and "shadow mode"
    where the actor is initialised by the worker and used to execute jobs.
    """
    #: highest priority queue, the can be overwritten by changing .queues
    HIGH_QUEUE = 'high'

    #: default queue, this is a special value as it's used in enqueue_job
    DEFAULT_QUEUE = 'dft'

    #: lowest priority queue, the can be overwritten by changing .queues
    LOW_QUEUE = 'low'

    #: prefix prepended to all queue names to create the list keys in redis
    QUEUE_PREFIX = b'arq:q:'

    #: if not none this name is used instead of the class name when encoding and referencing jobs
    name = None

    #: job class to use when encoding and decoding jobs from this actor.
    job_class = Job

    #: uses the actor cam enqueue jobs in, order is important, the first queue is highest priority.
    queues = (
        HIGH_QUEUE,
        DEFAULT_QUEUE,
        LOW_QUEUE,
    )

    def __init__(self, *, is_shadow=False, **kwargs):
        """
        :param is_shadow: whether the actor should be shadow mode, this should only be set by the work
        :param kwargs: other kwargs, see RedisMixin for all available options
        :return:
        """
        self.queue_lookup = {q: self.QUEUE_PREFIX + q.encode() for q in self.queues}
        self.name = self.name or self.__class__.__name__
        self.is_shadow = is_shadow
        self._bind_direct_methods()
        super().__init__(**kwargs)

    def _bind_direct_methods(self):
        for attr_name in dir(self.__class__):
            unbound_direct = getattr(getattr(self.__class__, attr_name), 'unbound_direct', None)
            if unbound_direct:
                name = attr_name + '_direct'
                if hasattr(self, name):
                    msg = '{} already has a method "{}", this breaks arq direct method binding of "{}"'
                    raise RuntimeError(msg.format(self.name, name, attr_name))
                setattr(self, name, unbound_direct.__get__(self, self.__class__))

    async def enqueue_job(self, func_name: str, *args, queue: str=None, **kwargs):
        """
        Enqueue a job by pushing the encoded job information into the redis list specified by the queue
        :param func_name: name of the function executing the job
        :param args: arguments to pass to the function
        :param queue: name of the queue to use, if None DEFAULT_QUEUE is used.
        :param kwargs: key word arguments to pass to the function
        """
        queue = queue or self.DEFAULT_QUEUE
        try:
            data = self.job_class.encode(class_name=self.name, func_name=func_name, args=args, kwargs=kwargs)
        except TypeError as e:
            raise JobSerialisationError(str(e)) from e
        main_logger.debug('%s.%s ▶ %s', self.name, func_name, queue)

        # use the pool directly rather than get_redis_conn to avoid one extra await
        pool = self._redis_pool or await self.get_redis_pool()

        queue_list = self.queue_lookup[queue]
        async with pool.get() as redis:
            await redis.rpush(queue_list, data)

    def __repr__(self):
        return '<{self.__class__.__name__}({self.name}) at 0x{id:02x}>'.format(self=self, id=id(self))


def concurrent(func_or_queue):
    """
    Decorating which defines a functions as concurrent, eg. it should be executed on the worker.

    If you wish to call the function directly you can access the original function at "*_direct".

    The decorator can optionally be used with one argument: the queue to use by default for the job.
    """
    dec_queue = None

    def _func_wrapper(func):
        func_name = func.__name__

        if not inspect.iscoroutinefunction(func):
            raise TypeError('{} is not a coroutine function'.format(func.__qualname__))
        main_logger.debug('registering concurrent function %s', func.__qualname__)

        @wraps(func)
        async def _enqueuer(obj, *args, queue_name=None, **kwargs):
            await obj.enqueue_job(func_name, *args, queue=queue_name or dec_queue, **kwargs)

        # NOTE: direct is (and has to be) unbound: http://stackoverflow.com/a/7891681/949890
        _enqueuer.unbound_direct = func
        return _enqueuer

    if isinstance(func_or_queue, str):
        dec_queue = func_or_queue
        return _func_wrapper
    else:
        return _func_wrapper(func_or_queue)
