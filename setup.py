from distutils.core import setup

setup(
    name='asyncworkers',
    version='1.9',
    author='Maxim Oransky',
    author_email='maxim.oransky@gmail.com',
    packages=[
        'asyncworkers',
    ],
    url='https://github.com/shantilabs/asyncworkers',
    install_requires=[
        'aioredis>=1.1.0',
    ],
)
