import io
import os
import sys
from shutil import rmtree
from setuptools import setup, find_packages, Command

here = os.path.abspath(os.path.dirname(__file__))

# Load the package's __version__.py module as a dictionary.
about = {}
with open(os.path.join(here, 'airflow_dbt', '__version__.py')) as f:
    exec(f.read(), about)

with io.open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()


class UploadCommand(Command):
    """Support setup.py upload."""

    description = 'Build and publish the package.'
    user_options = []

    @staticmethod
    def status(s):
        """Prints things in bold."""
        print('\033[1m{0}\033[0m'.format(s))

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        try:
            self.status('Removing previous builds…')
            rmtree(os.path.join(here, 'dist'))
        except OSError:
            pass

        self.status('Building Source and Wheel (universal) distribution…')
        os.system('{0} setup.py sdist bdist_wheel --universal'.format(sys.executable))

        self.status('Uploading the package to PyPI via Twine…')
        os.system('twine upload dist/*')

        self.status('Pushing git tags…')
        os.system('git tag v{0}'.format(about['__version__']))
        os.system('git push --tags')

        sys.exit()


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

        'Programming Language :: Python :: 3.7',
    ],
    # $ setup.py upload support.
    cmdclass={
        'upload': UploadCommand,
    },
)
