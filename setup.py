import pathlib

import setuptools


def readfile(filename: str) -> str:
    return pathlib.Path(filename).read_text('utf-8').strip()


long_description = '\n\n'.join((
    readfile('README.md'),
    readfile('CHANGES.md'),
))

setuptools.setup(
    name='darq',
    version='0.6.0',
    author='Igor Mozharovsky',
    author_email='igor.mozharovsky@gmail.com',
    description='A small wrapper around arq',
    long_description=long_description,
    long_description_content_type='text/markdown',
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
