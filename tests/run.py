import random
import sys

import asyncio

sys.path.insert(0, '..')

# import os
# os.environ['PYTHONASYNCIODEBUG'] = '1'

import datetime

from asyncworkers.processor import BaseProcessor
from asyncworkers.workers import LocalWorker, RemoteWorker, RemoteNodesWorker


class EchoWorker(LocalWorker):
    async def on_pack(self, pack):
        print('{}: kuku!'.format(datetime.datetime.now()))


class RemoteEchoWorker(RemoteWorker):
    async def on_pack(self, pack):
        print('{}: remote kuku!'.format(datetime.datetime.now()))


class RemoteNodeEchoWorker(RemoteNodesWorker):
    debug = []

    async def on_pack(self, pack):
        print('{}: remote kuku from node {}'.format(
            datetime.datetime.now(),
            self.node_id,
        ))
        self.debug.append(pack)


class CrashWorker(LocalWorker):
    async def on_pack(self, pack):
        print('ready?')
        print(1 / 0)
        print('impossible message')


class SumWorker(LocalWorker):
    class Pack(LocalWorker.Pack):
        def __init__(self, *, a, b, delay):
            self.__dict__.update(vars())
            self.__dict__.pop('self', None)

    async def on_pack(self, pack):
        await asyncio.sleep(pack.delay)
        self.logger.info(
            '%s: %r+%r=%r (with delay %.03f s)',
            self,
            pack.a,
            pack.b,
            pack.a + pack.b,
            pack.delay,
        )


class RemoteSumWorker(RemoteWorker):
    class Pack(RemoteWorker.Pack):
        def __init__(self, *, a, b, delay=0):
            self.__dict__.update(vars())
            self.__dict__.pop('self', None)

    async def on_pack(self, pack):
        if pack.delay:
            await asyncio.sleep(pack.delay)
        self.logger.info(
            '%s: %r+%r=%r (with delay %.03f s)',
            self,
            pack.a,
            pack.b,
            pack.a + pack.b,
            pack.delay,
        )


class TestProcessor(BaseProcessor):
    async def setup(self):
        await super().setup()

        # local worker
        sum_worker = self.new_worker(SumWorker, n=5)
        for i in range(10):
            for j in range(10):
                self.logger.info('%s: PUT: %s %s', self, i, j)
                pack = sum_worker.Pack(a=i, b=j, delay=random.random())
                await sum_worker.put(pack)

        # local worker: interval job
        self.touch_every(self.new_worker(EchoWorker), seconds=1)

        # local worker: interval job with exception (processor fails)
        self.touch_every(self.new_worker(CrashWorker), seconds=10)

        # remote worke
        # server A
        self.new_worker(RemoteSumWorker, n=5)
        for i in range(10):
            for j in range(10):
                pack = RemoteSumWorker.Pack(a=i, b=j)
                # server B
                await RemoteSumWorker.put(self.redis, pack)

        # remote worker: interval job
        self.new_worker(RemoteEchoWorker)  # server A
        self.touch_every(RemoteEchoWorker, seconds=1)  # server B

        # named remote worker
        # server A:
        self.new_worker(RemoteNodeEchoWorker, node_id=1)
        # server B:
        self.new_worker(RemoteNodeEchoWorker, node_id='beta')
        pack = RemoteNodeEchoWorker.Pack()
        # server C:
        await RemoteNodeEchoWorker.put_to_node(1, self.redis, pack)
        await RemoteNodeEchoWorker.put_to_node('beta', self.redis, pack)


def test():
    import logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(levelname)-8s [%(asctime)s] %(message)s',
    )
    test_processor = TestProcessor()
    try:
        test_processor.start()
    finally:
        print('=' * 100)
        assert len(RemoteNodeEchoWorker.debug) == 2


if __name__ == '__main__':
    test()
