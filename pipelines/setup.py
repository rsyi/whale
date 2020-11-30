from os import path
import setuptools
from setuptools import find_packages

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

# TODO: this is a hack until I get around to specifying exact requirements
with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setuptools.setup(
    name="whale-pipelines",
    version="1.2.1",
    author="Robert Yi",
    author_email="robert@ryi.me",
    description="A pared-down metadata scraper + SQL runner.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/dataframehq/whale",
    python_requires=">=3.6",
    packages=find_packages(exclude=["tests", "tests.*"]),
    install_requires=requirements,
    include_package_data=True,
)
