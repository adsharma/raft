#!/usr/bin/env python3

"""The setup script."""

from setuptools import find_packages, setup

with open("README.md") as readme_file:
    readme = readme_file.read()

requirements = []

setup_requirements = []

test_requirements = []

setup(
    author="Sean Reed",
    author_email="m3t4w0rm@googlemail.com",
    python_requires=">=3.5",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    description="A implementation of Raft in pure Python.",
    install_requires=requirements,
    license="MIT license",
    long_description=readme + "\n\n",
    include_package_data=True,
    keywords="raft",
    name="raft",
    packages=find_packages(include=["raft", "raft.*"]),
    setup_requires=setup_requirements,
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/adsharma/raft",
    version="0.1.0",
    zip_safe=False,
)
