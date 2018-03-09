import asyncio
import time


class BaseWorker:
    class Pack:
        start = None

        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
            self.__dict__.pop('self', None)

        def __repr__(self):
            return f'Pack(**{self.__dict__})'

        def __str__(self):
            return repr(self)

    def __init__(
        self,
        logger,
        redis,
        loop=None,
        **extra,
    ):
        self.logger = logger
        self.redis = redis
        self.loop = loop or asyncio.get_event_loop()
        self.__dict__.update(extra)

    def __str__(self):
        return self.__class__.__qualname__

    async def run(self):
        self.logger.debug('%s: run: %s', self, id(asyncio.Task.current_task()))
        while True:
            await self._wait_for_pack()

    async def _wait_for_pack(self):
        raise NotImplementedError()

    async def _got_pack(self, pack):
        await self.on_pack(pack)

    async def on_pack(self, pack, *args, **kwargs):
        raise NotImplementedError()


class LocalWorker(BaseWorker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._inbox = asyncio.Queue(loop=self.loop)

    async def _wait_for_pack(self):
        pack = await self._inbox.get()
        if pack:
            await self._got_pack(pack)

    async def put(self, pack):
        pack.start = time.time()
        await self._inbox.put(pack)


class RemoteWorker(BaseWorker):
    @classmethod
    def _get_key(cls):
        return cls.__qualname__

    async def _wait_for_pack(self):
        await self._do_wait_for_pack(self._get_key())

    async def _do_wait_for_pack(self, key):
        pack_data = await self.redis.pop(key)
        if pack_data:
            start = pack_data.pop('start')
            pack = self.Pack(**pack_data)
            pack.start = start
            await self._got_pack(pack)

    @classmethod
    async def put(cls, redis, packs, timeout=None):
        await cls._do_put(redis, cls._get_key(), packs, timeout=timeout)

    @classmethod
    async def _do_put(cls, redis, key, packs, *, timeout):
        if not packs:
            return
        if isinstance(packs, cls.Pack):
            packs = [packs]
        data = []
        for pack in packs:
            assert isinstance(pack, cls.Pack), pack
            data.append(dict(pack.__dict__, start=time.time()))
        await redis.push_multi(key, data, timeout=timeout)


class RemoteNodesWorker(RemoteWorker):
    def __init__(self, server, node_id, **extra):
        super().__init__(server, **extra)
        self.node_id = node_id

    async def _wait_for_pack(self):
        key = '{}@{}'.format(self._get_key(), self.node_id)
        await self._do_wait_for_pack(key)

    @classmethod
    async def put(cls, redis, packs, timeout=None):
        raise ValueError('Use {}.put_to_node()'.format(cls.__name__))

    @classmethod
    async def put_to_node(cls, node_id, redis, packs, timeout=None):
        key = '{}@{}'.format(cls._get_key(), node_id)
        await cls._do_put(redis, key, packs, timeout=timeout)
