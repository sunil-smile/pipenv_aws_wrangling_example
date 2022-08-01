# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "aws_wrangling_utilities"
copyright = "2022, Sunilprasath Elangovan"
author = "Sunilprasath Elangovan"
release = "1.0.0"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration


# Location of the python files
import os
import sys

# currently we are in source folder , so we are going to the root folder where we have the vbp_python_utils module is present
sys.path.insert(0, os.path.abspath(os.path.join("..", "..")))

# what level document details we have to add
# https://www.sphinx-doc.org/en/master/usage/extensions/index.html
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx.ext.ifconfig",
    "sphinx.ext.viewcode",
    "sphinx.ext.githubpages",
]
pygments_style = "sphinx"
include_patterns = ["**"]
master_doc = "index"

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "classic"
html_static_path = ["_static"]

# we can add custom css like this , but classic theme not offer customer css
# html_style = "css/custom.css"

# to add custom themes to the classic
html_theme_options = {"rightsidebar": False, "footerbgcolor": "#eb3464"}
