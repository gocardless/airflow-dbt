import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

# Load the package's __version__.py module as a dictionary.
about = {}
with open(os.path.join(here, 'airflow_dbt', '__version__.py')) as f:
    exec(f.read(), about)

with io.open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name = 'airflow_dbt',
    version = about['__version__'],
    packages = find_packages(exclude=['tests']),
    install_requires = ['apache-airflow >= 1.10.3'],
    author = 'GoCardless',
    author_email = 'engineering@gocardless.com',
    description = 'Apache Airflow integration for dbt',
    long_description = long_description,
    long_description_content_type='text/markdown',
    license = 'MIT',
    url = 'https://github.com/gocardless/airflow-dbt',
    classifiers=[
        'Development Status :: 5 - Production/Stable',

        'License :: OSI Approved :: MIT License',

        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',

        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
)