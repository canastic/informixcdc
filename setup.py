from setuptools import setup, find_packages

setup(
    name="informixcdc",
    version="0.0.1-alpha",
    packages=find_packages(exclude=["docs", "tests"]),
    python_requires=">=3.6, <4",
    install_requires=[
        "typing-extensions",
    ],
)
