from __future__ import annotations

import importlib.metadata
import os
import sys
from datetime import date

# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
# -- Path setup --------------------------------------------------------------
# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
sys.path.insert(0, os.path.abspath('../..'))

# -- Project information -----------------------------------------------------

project = 'd2r-api'
copyright = f'2024 - {date.today().year}, RUBclim'
author = 'D2R Team (Jonas Kittner)'

release = importlib.metadata.version(project)
version = '.'.join(release.split('.')[:2])


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.intersphinx',
    'sphinx_autodoc_typehints',
    'myst_parser',
    'sphinx_copybutton',
    'sphinx.ext.viewcode',
    'sphinx_toolbox.decorators',
    'sphinx_sqlalchemy',
    'sphinxcontrib.autodoc_pydantic',
]
# autodoc_typehints = 'both'
typehints_fully_qualified = False
# always_document_param_types = True
always_use_bars_union = True
typehints_defaults = 'braces-after'
simplify_optional_unions = False
typehints_use_signature = False
typehints_use_signature_return = True

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns: list[str] = []
intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    'pandas': ('https://pandas.pydata.org/docs', None),
    'thermal_comfort': ('https://rubclim.github.io/thermal-comfort', None),
    'sqlalchemy': ('https://docs.sqlalchemy.org/en/20/', None),
    'flask': ('https://flask.palletsprojects.com/en/3.0.x/', None),
    'fastapi': ('https://fastapi.tiangolo.com/', None),
    'celery': ('https://docs.celeryproject.org/en/stable/', None),
}

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'furo'

html_title = 'd2r-api Documentation'
html_short_title = f'd2r-api-{release}'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# add has to css
html_static_path = ['css']

html_css_files = ['custom.css']

source_suffix = {
    '.rst': 'restructuredtext',
    '.txt': 'markdown',
    '.md': 'markdown',
}
