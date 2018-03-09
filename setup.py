from distutils.core import setup

from pip.req import parse_requirements


setup(
    name='asyncworkers',
    version='1.0',
    author='Maxim Oransky',
    author_email='maxim.oransky@gmail.com',
    packages=[
        'asyncworkers',
    ],
    url='https://github.com/shantilabs/asyncworkers',
    install_reqs=parse_requirements('requirements.txt')
)
