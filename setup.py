from setuptools import find_packages, setup

setup(
    name="dagster_data_gov_lv",
    packages=find_packages(exclude=["dagster_data_gov_lv_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "ckanapi",
        "Unidecode",
        "pandas",
        "requests",
        "duckdb",
        "progressbar",
        "filelock",
    ],
    extras_require={"dev": [
        "dagster-webserver",
        "pytest",
        "isort",
    ]},
)
