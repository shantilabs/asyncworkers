import asyncio

from leoorm import LeoORM

from .workers import RemoteNodesWorker, RemoteWorker, LocalWorker


class DbMixin:
    db_pool = None
    use_db = True
    const_db_connection = False

    async def run(self):
        self.logger.debug('%s: run: %s', self, id(asyncio.Task.current_task()))
        if self.use_db and self.const_db_connection:
            async with self.db_pool.acquire() as db_conn:
                orm = LeoORM(db_conn)
                while True:
                    await self._wait_for_pack(orm)
        else:
            while True:
                await self._wait_for_pack()

    async def _got_pack(self, pack, **kwargs):
        if not self.use_db:
            return await self.on_pack(None, self.redis, pack)

        orm = kwargs.pop('orm', None)
        if orm:
            return await self.on_pack(orm, self.redis, pack)

        async with self.db_pool.acquire() as db_conn:
            orm = LeoORM(db_conn)
            return await self.on_pack(orm, self.redis, pack)


class DbLocalWorker(DbMixin, LocalWorker):
    async def on_pack(self, orm, redis, pack):
        raise NotImplementedError()


class DbRemoteWorker(DbMixin, RemoteWorker):
    async def on_pack(self, orm, redis, pack):
        raise NotImplementedError()


class DbRemoteNodesWorker(DbMixin, RemoteNodesWorker):
    async def on_pack(self, orm, redis, pack):
        raise NotImplementedError()
