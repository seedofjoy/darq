import setuptools

with open('README.md', 'r') as f:
    long_description = f.read()

setuptools.setup(
    name='darq',
    version='0.0.2',
    author='Igor Mozharovsky',
    author_email='igor.mozharovsky@gmail.com',
    description='A small wrapper around arq',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/seedofjoy/darq',
    packages=['darq'],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    install_requires=[
        'arq>=0.18,<0.19',
        'typing_extensions>=3.7.4; python_version<"3.8"',
    ],
    python_requires='>=3.6',
    license='MIT',
)
