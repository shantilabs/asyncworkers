from leoorm.utils import create_db_pool

from .processor import BaseProcessor


class DbProcessor(BaseProcessor):
    db_pool = None

    async def setup(self):
        await super().setup()
        self.db_pool = await create_db_pool(
            loop=self.loop,
            max_size=self.pool_size,
        )

    async def teardown(self):
        await super().teardown()
        await self.db_pool.close()

    def new_worker(self, worker_class, n=1, **extra):
        if getattr(worker_class, 'use_db'):
            extra['db_pool'] = self.db_pool
        return super().new_worker(worker_class, n, **extra)
