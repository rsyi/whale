import os
import setuptools
from pathlib import Path
from setuptools import find_packages

DEFAULT_DIRECTORY_NAME = '.metaframe'

setuptools.setup(
    name='metaframe',
    version='0.0.1a1',
    author='Robert Yi',
    author_email='robert@ryi.me',
    description="A wrapper around amundsen's databuilder to enable easier extractions into text files.",
    url='https://github.com/rsyi/metaframe',
    python_requires='>=3.6',
    packages=find_packages(),
    install_requires=[
        'amundsen-databuilder>=2.0.0',
    ],
    include_package_data=True,
    entry_points = {
        'console_scripts': ['__metaframe_pull=metaframe:main']
    }
)
