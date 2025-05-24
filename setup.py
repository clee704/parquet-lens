#!/usr/bin/env python3
"""
Setup file for parquet-lens
"""

from setuptools import setup, find_packages

setup(
    name="parquet-lens",
    version="0.1.0",
    description="A tool for analyzing Parquet files at the byte level",
    author="",
    author_email="",
    packages=find_packages(),
    python_requires=">=3.6",
    install_requires=[
        "thrift",
    ],
    extras_require={
        "dev": [
            "pytest",
            "coverage",
            "pandas",
            "pyarrow",
        ],
    },
    entry_points={
        "console_scripts": [
            "parquet-lens=parquet_lens:main",
        ],
    },
)
