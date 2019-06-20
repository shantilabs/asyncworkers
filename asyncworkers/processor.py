import asyncio
import logging
from traceback import format_exc

from .redis_conn import RedisConn
from .workers import LocalWorker, RemoteWorker


class BaseProcessor:
    logger = logging.getLogger('asyncworkers')

    redis_host = 'localhost'
    redis_port = 6379
    redis_db = 0
    redis_password = None
    redis_pool_size = 50

    def __init__(self, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self._servers = []
        self.redis = RedisConn(
            host=self.redis_host,
            port=self.redis_port,
            db=self.redis_db,
            password=self.redis_password,
            maxsize=self.redis_pool_size,
        )
        self._sutting_down = False

    def __str__(self):
        return self.__class__.__qualname__

    def start(self):
        self.logger.info('%s: started', self)
        try:
            self.loop.run_until_complete(self._strict(self.setup()))
            assert self.redis.pool
            self.loop.run_forever()
        except KeyboardInterrupt:
            self.loop.run_until_complete(self._shutdown('KeyboardInterrupt'))
        except SystemExit:
            self.logger.info('%s: system exit', self)
        finally:
            self.logger.debug('%s: close loop', self)
            self.loop.close()

    async def setup(self):
        self.logger.debug('%s: setup...', self)
        await self.redis.open()
        self.logger.debug('%s: redis ping: %s', self, await self.redis.ping())

    async def teardown(self):
        self.logger.debug('%s: teardown...', self)
        await self.redis.close()

    async def on_fail(self, exc):
        self.logger.warning('%s: on_fail: %s', self, format_exc())

    def new_worker(self, worker_class, n=1, **extra):
        worker = worker_class(
            loop=self.loop,
            logger=self.logger,
            redis=self.redis,
            **extra
        )
        for _ in range(n):
            self.loop.create_task(self._strict(worker.run()))
        return worker

    async def add_server(self, coro):
        if coro is not None:
            self._servers.append(await coro)

    def touch_every(self, worker_or_class, *, seconds):
        if isinstance(worker_or_class, LocalWorker):
            worker = worker_or_class
            self.logger.debug('%s: local touch %s every %s', self, worker, seconds)  # noqa
            coro = self._local_touch_every(worker, seconds)
        elif issubclass(worker_or_class, RemoteWorker):
            worker_class = worker_or_class
            self.logger.debug('%s: remote touch %s every %s', self, worker_class, seconds)  # noqa
            coro = self._remote_touch_every(worker_class, seconds)
        else:
            self.logger.warning('%s: incorrect worker: %s', self, worker_or_class)  # noqa
            raise ValueError(worker_or_class)
        self.loop.create_task(self._strict(coro))

    def die(self, reason):
        self.loop.create_task(self._shutdown(reason))

    async def _remote_touch_every(self, remote_worker_class, seconds):
        while True:
            self.logger.debug('%s: touch: %s', self, remote_worker_class)
            await remote_worker_class.put(self.redis, remote_worker_class.Pack())  # noqa
            await asyncio.sleep(seconds)

    async def _local_touch_every(self, local_worker, seconds):
        while True:
            await asyncio.sleep(seconds)
            self.logger.debug('%s: touch: %s', self, local_worker)
            await local_worker.put(local_worker.Pack())

    async def _strict(self, coro):
        try:
            return await coro
        except asyncio.CancelledError:
            if not self._sutting_down:
                self.logger.warning('%s: CancelledError in %s', self, coro)
            return 'cancelled'
        except Exception as exc:
            await self.on_fail(exc)
            message = str(exc) or exc.__class__.__qualname__
            self.die('{} failed: {}'.format(coro, message))
            return message

    async def _shutdown(self, reason):
        if self._sutting_down:
            self.logger.info('%s: shutdown: already starts', self)
            return
        self._sutting_down = True
        self.logger.info('%s: shutdown: because %s', self, reason)
        try:
            for server in self._servers:
                self.logger.debug('%s: shutdown: close server: %s', self, server)  # noqas
                server.close()
                await server.wait_closed()
            current_task = asyncio.tasks.Task.current_task()
            for task in asyncio.Task.all_tasks():
                if task is current_task:
                    continue
                task.cancel()
                res = await task
                self.logger.debug('%s: shutdown: cancel: %s: %r', self, task, res)  # noqa
            await self.teardown()
        except Exception as e:
            self.logger.warning('%s: shutdown error: %s', self, e)
        finally:
            raise SystemExit()
