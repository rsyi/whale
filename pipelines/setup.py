from os import path
import setuptools
from setuptools import find_packages

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setuptools.setup(
    name="whale-pipelines",
    version="1.1.4",
    author="Robert Yi",
    author_email="robert@ryi.me",
    description="A pared-down metadata scraper + SQL runner.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/dataframehq/whale",
    python_requires=">=3.6",
    packages=find_packages(),
    install_requires=[
        "amundsen-databuilder>=2.0.0",
        "pandas",
        "pyyaml",
        "pyhocon>=0.3.42",
        "SQLAlchemy>=1.3.19",
    ],
    include_package_data=True,
)
