import os
from setuptools import setup

with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as f:
    readme_rst = f.read()

setup(
    name='sqlitent',
    url='https://github.com/digitalmensch/sqlitent',
    download_url='https://pypi.python.org/pypi/sqlitent',
    description='namedtuples inside set-like sqlite databases',
    long_description=readme_rst,
    keywords='sqlite, persistent set, namedtuple',
    platforms='any',
    license="MIT",
    version='0.0',
    author='Tobias Ammann',
    py_modules=['sqlitent'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Topic :: Database :: Front-Ends',
    ],
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
)
