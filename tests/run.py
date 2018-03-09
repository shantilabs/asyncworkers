import sys
sys.path.insert(0, '..')

# import os
# os.environ['PYTHONASYNCIODEBUG'] = '1'

import datetime

from asyncworkers.processor import BaseProcessor
from asyncworkers.workers import LocalWorker


class EchoWorker(LocalWorker):
    async def on_pack(self, pack):
        print('{}: kuku!'.format(datetime.datetime.now()))


class BadWorker(LocalWorker):
    async def on_pack(self, pack):
        print('ready?')
        print(1 / 0)
        print('impossible message')


class TestProcessor(BaseProcessor):
    async def setup(self):
        await super().setup()
        self.touch_every(self.new_worker(EchoWorker), seconds=1)
        self.touch_every(self.new_worker(BadWorker), seconds=10)


def test():
    import logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(levelname)-8s [%(asctime)s] %(message)s',
    )
    test_processor = TestProcessor()
    test_processor.start()


if __name__ == '__main__':
    test()
