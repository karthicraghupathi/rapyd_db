import os
from setuptools import setup, find_packages

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='rapyd_db',
    version='0.0.3',
    description='An opinionated lightweight wrapper around various SQL backend drivers.',
    long_description=readme,
    author='Karthic Raghupathi',
    author_email='karthicr@gmail.com',
    url='https://github.com/karthicraghupathi/rapyd_db',
    license=license,
    packages=find_packages(exclude=('tests', 'docs')),
    extras_require={
        'mysql': [
            'mysqlclient'
        ],
        'mongo': [
            'pymongo'
        ],
        'mssql': [
            'Cython',
            'pymssql'
        ]
    },
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3'
    ]
)
