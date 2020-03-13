import pathlib
from importlib.machinery import SourceFileLoader

import setuptools

version = SourceFileLoader('version', 'darq/version.py').load_module()


def readfile(filename: str) -> str:
    return pathlib.Path(filename).read_text('utf-8').strip()


long_description = '\n\n'.join((
    readfile('README.rst'),
    readfile('CHANGES.rst'),
))

setuptools.setup(
    name='darq',
    version=str(version.VERSION),
    author='Igor Mozharovsky',
    author_email='igor.mozharovsky@gmail.com',
    description='A small wrapper around arq',
    long_description=long_description,
    long_description_content_type='text/x-rst',
    url='https://github.com/seedofjoy/darq',
    packages=['darq'],
    package_data={'darq': ['py.typed']},
    classifiers=[
        'Environment :: Console',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Typing :: Typed',
    ],
    entry_points={
        'console_scripts': ['darq = darq.cli:cli'],
    },
    install_requires=[
        'async-timeout>=3.0.0',
        'aioredis>=1.1.0',
        'click>=6.7',
        'pydantic>=0.20',
        'dataclasses>=0.6; python_version=="3.6"',
        'typing_extensions>=3.7.4; python_version<"3.8"',
    ],
    extras_require={
        'watch': ['watchgod>=0.4'],
    },
    python_requires='>=3.6',
    license='MIT',
)
