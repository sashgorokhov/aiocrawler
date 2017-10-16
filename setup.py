from setuptools import setup, find_packages

with open('README.rst', 'r') as f:
    long_description = f.read()

VERSION = '0.1'

setup(
    install_requires=[
        'aiohttp',
        'aiodns',
        'parsel',
        'PyDispatcher',
    ],
    name='aiocrawler',
    version=VERSION,
    packages=find_packages(),
    url='https://github.com/sashgorokhov/aiocrawler',
    download_url='https://github.com/sashgorokhov/aiocrawler/archive/master.zip',
    keywords=['asyncio', 'web', 'scraping', 'crawler'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    long_description=long_description,
    license='MIT License',
    author='sashgorokhov',
    author_email='sashgorokhov@gmail.com',
    description='Asynchronous web scraping',
)
