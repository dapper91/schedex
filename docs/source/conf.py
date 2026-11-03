# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import sys
from pathlib import Path

sys.path.insert(0, str(Path("..", "..").resolve()))


# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "schedex"
copyright = "2026, Dmitry Pershin"
author = "Dmitry Pershin"
release = "1.0.0"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx_copybutton",
]

templates_path = ["_templates"]
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "furo"
html_static_path = ["_static"]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}
autodoc_typehints = "description"

_link_color_light = "#024bb0"
_link_color_dark = "#5192d2"


html_theme_options = {
    "light_css_variables": {
        "color-brand-primary": "black",
        "color-brand-content": _link_color_light,
        "color-foreground-muted": "#808080",
        "color-highlight-on-target": "inherit",
        "color-highlighted-background": "#ffffcc",
        "color-sidebar-link-text": "black",
        "color-sidebar-link-text--top-level": "black",
        "color-link": _link_color_light,
        "color-link--hover": _link_color_light,
        "color-link-underline": "transparent",
        "color-link-underline--hover": _link_color_light,
    },
    "dark_css_variables": {
        "color-brand-primary": "#ffffff",
        "color-brand-content": _link_color_dark,
        "color-highlight-on-target": "inherit",
        "color-highlighted-background": "#333300",
        "color-sidebar-link-text": "#ffffffcc",
        "color-sidebar-link-text--top-level": "#ffffffcc",
        "color-link": _link_color_dark,
        "color-link--hover": _link_color_dark,
        "color-link-underline": "transparent",
        "color-link-underline--hover": _link_color_dark,
    },
    "sidebar_hide_name": True,
}
