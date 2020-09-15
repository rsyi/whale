import setuptools
from setuptools import find_packages

DEFAULT_DIRECTORY_NAME = '.metaframe'

setuptools.setup(
    name='metaframe-databuilder',
    version='0.0.0a32',
    author='Robert Yi',
    author_email='robert@ryi.me',
    description="A pared-down metadata ETL library.",
    url='https://github.com/rsyi/metaframe',
    python_requires='>=3.6',
    packages=find_packages(),
    install_requires=[
        'amundsen-databuilder>=2.0.0',
        'pyhocon>=0.3.42',
        'tabulate',
        'pyaml',
        'google-api-python-client'
    ],
    include_package_data=True,
)
