from setuptools import setup, find_packages

setup(
    name="chen_quant",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "dolphindb",
        "akshare",
        "pandas",
    ],
    author="z7-god",
    description="A futures data downloading and storage tool using Akshare and DolphinDB.",
)
