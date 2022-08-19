import os

from setuptools import setup


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="test_package",
    version="0.0.4",
    author="test_package",
    description=("An example of how to set up pytest"),
    license="GNU",
    keywords="example pytest tutorial sphinx",
    packages=["src", "test"],
    long_description=read("README.md"),
)
