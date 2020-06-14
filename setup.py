import os
import setuptools
from pathlib import Path
from setuptools import find_packages

DEFAULT_DIRECTORY_NAME = '.metaframe'

setuptools.setup(
    name='metaframe-databuilder',
    version='0.0.0a27',
    author='Robert Yi',
    author_email='robert@ryi.me',
    description="A pared-down metadata ETL library, based off amundsen-databuilder.",
    url='https://github.com/rsyi/metaframe',
    python_requires='>=3.6',
    packages=find_packages(),
    install_requires=[
        'amundsen-databuilder>=2.0.0',
        'pyhocon>=0.3.42',
        'tabulate',
        'pyaml',
    ],
    include_package_data=True,
)
