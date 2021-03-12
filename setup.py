import os

from setuptools import setup

curdir = os.path.abspath(os.path.dirname(__file__))
readme = os.path.join(curdir, "README.md")


def version():
    with open(os.path.join(curdir, "goingnats.py")) as f:
        for line in f:
            if line.startswith("__version__ = "):
                break
    version = line.partition(" = ")[-1]
    return version.replace('"', '').strip()


setup(
    name="goingnats",
    version=version(),
    description="a Python NATS client",
    long_description=open(readme).read(),
    long_description_content_type="text/markdown",
    author="Eugene Van den Bulke",
    author_email="eugene.vandenbulke@gmail.com",
    url="http://github.com/3kwa/goingnats",
    py_modules=["goingnats"],
    license="Unlicensed",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries",
    ],
)
