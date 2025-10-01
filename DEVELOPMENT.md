# Development Workflow with uv

This project now uses [uv](https://github.com/astral-sh/uv) for fast Python packaging and dependency management.

## Setup

1. **Install uv** (if not already installed):
   ```bash
   # Using the official installer
   curl -LsSf https://astral.sh/uv/install.sh | sh

   # Or using pip
   pip install uv
   ```

2. **Install dependencies**:
   ```bash
   uv sync --group dev
   ```

3. **Activate the virtual environment** (optional):
   ```bash
   source .venv/bin/activate
   ```

## Common Commands

| Action | Command |
|--------|---------|
| Install dependencies | `make install-deps` or `uv sync --group dev` |
| Run tests | `make test` or `uv run python -m pytest tests/` |
| Lint code | `make lint` or `uv run flake8 raft tests` |
| Run the application | `make run` or `uv run python main.py` |
| Build the package | `make dist` or `uv build` |
| Check code coverage | `make coverage` or `uv run coverage run --source raft -m pytest tests/` |

## Using Make Commands

This project provides convenient Make commands:

- `make help` - Show all available commands
- `make test` - Run tests
- `make lint` - Lint the code
- `make clean` - Remove build artifacts
- `make install-deps` - Install dependencies
- `make dev` - Set up development environment
- `make run` - Run the application
- `make dist` - Build source and wheel packages

## Dependency Management

- Dependencies are defined in `pyproject.toml`
- Development dependencies are in the `[dependency-groups].dev` section
- The lock file `uv.lock` ensures reproducible builds
- To add a dependency: edit `pyproject.toml` and run `uv sync`
- To update dependencies: run `uv sync --upgrade`

## Virtual Environment

- uv automatically creates and manages a virtual environment in `.venv`
- To run commands in the environment: `uv run <command>`
- To enter the environment: `source .venv/bin/activate`

## Running Tests

Tests can be run in several ways:

```bash
# Using make
make test

# Using uv directly
uv run python -m pytest tests/

# Using pytest directly after activating environment
source .venv/bin/activate
pytest tests/
```
