# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
import os
import sys

sys.path.insert(0, os.path.abspath('..'))

from darq.version import __version__



# -- Project information -----------------------------------------------------

project = 'darq'
copyright = '2020, Igor Mozharovsky'
author = 'Igor Mozharovsky'

release = f'v{__version__}'


# -- General configuration ---------------------------------------------------
master_doc = 'index'
extensions = [
    'sphinx.ext.autodoc',
]

templates_path = ['_templates']

exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']


# -- Options for HTML output -------------------------------------------------
html_theme = 'alabaster'
html_theme_options = {
    'description': 'Async task manager with Celery-like features',
    'fixed_sidebar': True,
    'github_type': 'star',
    'github_user': 'seedofjoy',
    'github_repo': 'darq',
}

html_static_path = ['_static']
html_sidebars = {
    '**': [
        'about.html',
        'localtoc.html',
        'searchbox.html',
    ]
}
