import codecs
from setuptools import setup, find_packages

with codecs.open('README.md', 'r', 'utf-8') as file:
    long_description = file.read()

setup(
    name = 'airflow_dbt',
    version = '1.0.0',
    packages = find_packages(exclude=['tests']),
    install_requires = ['apache-airflow == 1.10.6'],
    author = 'GoCardless',
    author_email = 'engineering@gocardless.com',
    description = 'Apache Airflow integration for dbt',
    long_description = long_description,
    long_description_content_type='text/markdown',
    license = 'MIT',
    url = 'https://github.com/gocardless/airflow-dbt',
)