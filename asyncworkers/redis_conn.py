import asyncio
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

    async def ping(self):
        res = await self._exec(self.pool.execute('PING'))
        logger.debug('RedisConn.ping: %s', res)
        return res.decode()

    async def push_multi(self, name, data: list, timeout=None):
        if not data:
            raise ValueError('empty data')
        vals = [json.dumps(x) for x in data]
        logger.debug('RedisConn.push_multi: %s', name)
        res = await self._exec(self.pool.execute('RPUSH', name, *vals))
        if timeout is not None:  # TODO: test it
            logger.debug('RedisConn.expire: %s', name)
            await self._exec(self.pool.execute('EXPIRE', name, timeout))

    async def pop(self, name):
        val = await self._exec(self.pool.execute('BLPOP', name, 0))
        return json.loads(val[1]) if val else None

    async def len(self, name):
        val = await self._exec(self.pool.execute('LLEN', name))
        logger.debug('RedisConn.len of %s = %s', name, val)
        return int(val)

    async def set_expired(self, name, val, timeout):
        val = json.dumps(val)
        await self._exec(self.pool.execute('SETEX', name, int(timeout), val))
        logger.debug('RedisConn.set_expired: %s = %s', name, val)

    async def inc_rate(self, name):
        name = f'{name}_{int(time.time() / 60) * 60}'
        val = await self._exec(self.pool.execute('INCR', name))
        await self._exec(self.pool.execute('EXPIRE', name, 61))
        logger.debug('RedisConn.inc_rate: %s = %s', name, val)
        return val or 0

    async def get(self, name):
        val = await self._exec(self.pool.execute('GET', name))
        logger.debug('RedisConn.get: %s', name)
        if val:
            val = json.loads(val.decode())
        return val

    async def __aenter__(self):
        self.pool = await self.open()

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def open(self):
        return await aioredis.create_pool(
            (self.host, self.port),
            db=self.db,
            maxsize=self.maxsize,
        )

    async def close(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

    async def _exec(self, coro):
        try:
            return await coro
        except (
            aioredis.PoolClosedError,
            ConnectionRefusedError,
        ) as e:
            if not await self._try_reconnect(timeout=5.0):
                raise
            return await coro

    async def _try_reconnect(self, timeout):
        deadline = time.monotonic() + timeout
        sleep = 0.1
        while time.monotonic() < deadline:
            try:
                await self.pool.execute('PING')
            except (
                aioredis.PoolClosedError,
                ConnectionRefusedError,
            ) as e:
                logger.warning('RedisConn._try_reconnect: %s', e)
                await asyncio.sleep(sleep)
                sleep *= 2
                try:
                    await self.close()
                except Exception as e:  # noqa
                    logger.warning('RedisConn._try_reconnect: close: %s', e)
                try:
                    self.pool = await self.open()
                except Exception as e:
                    logger.warning('RedisConn._try_reconnect: open: %s', e)
            else:
                return True
