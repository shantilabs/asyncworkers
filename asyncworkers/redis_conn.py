import json
import logging
import time

import aioredis

logger = logging.getLogger(__name__)


class RedisConn:
    def __init__(
        self,
        host='localhost',
        db=0,
        port=6379,
        maxsize=100,
    ):
        self.pool = None
        self.host = host
        self.db = db
        self.port = port
        self.maxsize = maxsize

    def dumps(self, val):
        return json.dumps(val)

    def loads(self, val):
        return json.loads(val)

    async def ping(self):
        with await self.pool as conn:
            res = await conn.execute('PING')
        logger.debug('RedisConn.ping: %s', res)
        return res.decode()

    async def push_multi(self, name, data: list, timeout=None):
        if not data:
            raise ValueError('empty data')
        vals = [self.dumps(x) for x in data]
        logger.debug('RedisConn.push_multi: %s', name)
        with await self.pool as conn:
            res = await conn.execute('RPUSH', name, *vals)
            if timeout is not None:  # TODO: test it
                logger.debug('RedisConn.expire: %s', name)
                await conn.execute('EXPIRE', name, timeout)

    async def pop(self, name):
        with await self.pool as conn:
            try:
                val = await conn.execute('BLPOP', name, 0)
            except RuntimeError:
                return None
        return self.loads(val[1]) if val else None

    async def len(self, name):
        with await self.pool as conn:
            val = await conn.execute('LLEN', name)
        logger.debug('RedisConn.len of %s = %s', name, val)
        return int(val)

    async def set_expired(self, name, val, timeout):
        val = self.dumps(val)
        with await self.pool as conn:
            await conn.execute('SETEX', name, int(timeout), val)
        logger.debug('RedisConn.set_expired: %s = %s', name, val)

    async def get(self, name):
        with await self.pool as conn:
            val = await conn.execute('GET', name)
        logger.debug('RedisConn.get: %s', name)
        if val:
            val = self.loads(val)
        return val

    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def open(self):
        self.pool = await aioredis.create_pool(
            (self.host, self.port),
            db=self.db,
            maxsize=self.maxsize,
        )

    async def close(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
