.PHONY: clean clean-test clean-pyc clean-build docs help
.DEFAULT_GOAL := help

define BROWSER_PYSCRIPT
import os, webbrowser, sys

from urllib.request import pathname2url

webbrowser.open("file://" + pathname2url(os.path.abspath(sys.argv[1])))
endef
export BROWSER_PYSCRIPT

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

BROWSER := python -c "$BROWSER_PYSCRIPT"

help:
	@echo "Available commands:"
	@echo "  clean              remove all build, test, coverage and Python artifacts"
	@echo "  clean-build        remove build artifacts"
	@echo "  clean-pyc          remove Python file artifacts"
	@echo "  clean-test         remove test and coverage artifacts"
	@echo "  lint               check style with flake8"
	@echo "  test               run tests quickly with the default Python"
	@echo "  test-all           run tests on every Python version with tox"
	@echo "  coverage           check code coverage quickly with the default Python"
	@echo "  docs               generate Sphinx HTML documentation, including API docs"
	@echo "  servedocs          compile the docs watching for changes"
	@echo "  release            package and upload a release"
	@echo "  dist               builds source and wheel package"
	@echo "  install            install the package to the active Python's site-packages"
	@echo "  install-deps       install dependencies using uv"
	@echo "  dev                install dependencies and set up development environment"
	@echo "  run                run the main application"

clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

lint: ## check style with flake8
	uv run flake8 raft tests

test: ## run tests quickly with the default Python
	uv run python -m pytest tests/

test-all: ## run tests on every Python version with tox
	uv run tox

coverage: ## check code coverage quickly with the default Python
	uv run coverage run --source raft -m pytest tests/
	uv run coverage report -m
	uv run coverage html
	$(BROWSER) htmlcov/index.html

docs: ## generate Sphinx HTML documentation, including API docs
	uv run rm -f docs/raft.rst
	uv run rm -f docs/modules.rst
	uv run sphinx-apidoc -o docs/ raft
	$(MAKE) -C docs clean
	$(MAKE) -C docs html
	$(BROWSER) docs/_build/html/index.html

servedocs: docs ## compile the docs watching for changes
	uv run watchmedo shell-command -p '*.rst' -c '$(MAKE) -C docs html' -R -D .

release: dist ## package and upload a release
	uv run twine upload dist/*

dist: clean ## builds source and wheel package
	uv build
	ls -l dist

install: clean ## install the package in development mode
	uv pip install -e .

install-deps: ## install dependencies using uv
	uv sync --group dev

dev: install-deps ## install dependencies and set up development environment
	uv sync --group dev

run: ## run the main application
	uv run python main.py
