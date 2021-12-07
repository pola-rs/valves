from setuptools import setup, find_packages


pandas_packages = ["pandas>=1.0.0"]
polars_packages = ["polars>=0.10.24"]
dask_packages = ["dask>=2021.11.2"]
base_packages = polars_packages

all_dep_packages = base_packages + dask_packages + pandas_packages

test_packages = [
    "pytest>=5.4.3",
    "black>=19.10b0",
    "flake8>=3.8.3",
    "mktestdocs>=0.1.0",
    "interrogate>=1.2.0",
    "pre-commit>=2.15.0",
    "pyarrow>=6.0.1",
] + all_dep_packages

docs_packages = [
    "mkdocs>=1.1",
    "mkdocs-material>=4.6.3",
    "mkdocstrings>=0.8.0",
]

dev_packages = list(set(base_packages + test_packages + docs_packages + dask_packages))

setup(
    name="valves",
    version="0.1.0",
    packages=find_packages(),
    install_requires=base_packages,
    extras_require={
        "pandas": pandas_packages,
        "dask": dask_packages,
        "all": all_dep_packages,
        "dev": dev_packages,
        "test": test_packages,
    },
)
