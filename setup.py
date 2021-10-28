# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import os
from setuptools import find_packages, setup

version = "0.0.7"

if os.path.exists("README.rst"):
    with open("README.rst") as fp:
        long_description = fp.read()
else:
    long_description = ''

setup(
    name='syncany',
    version=version,
    url='https://github.com/snower/syncany',
    author='snower',
    author_email='sujian199@gmail.com',
    license='MIT',
    packages=find_packages(),
    zip_safe=False,
    install_requires=[
        "pytz>=2018.5",
        "tzlocal>=1.5.1",
    ],
    extras_require={
        "pymongo": ['pymongo>=3.6.1'],
        "pymysql": ['PyMySQL>=0.8.1'],
        "openpyxl": ["openpyxl>=2.5.0"],
        "postgresql": ["psycopg2>=2.8.6"],
        "elasticsearch": ["elasticsearch>=6.3.1"],
        "influxdb": ["influxdb>=5.3.1"],
        "clickhouse": ["clickhouse_driver>=0.1.5"],
        "rich": ["rich>=9.11.1"],
        "redis": ["redis>=3.5.3"]
    },
    package_data={
        '': ['README.md'],
    },
    entry_points={
        'console_scripts': [
            'syncany = syncany.main:main',
        ],
    },
    description='简单快捷数据同步导出框架',
    long_description=long_description,
)
