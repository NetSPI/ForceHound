# Contributing to ForceHound

Thanks for your interest in contributing to ForceHound. Here's how to get started.

## Reporting Issues

When opening an issue, please include:

- ForceHound version (`pip show forcehound` or check `pyproject.toml`)
- Python version (`python --version`)
- Collector mode used (`api`, `aura`, or `both`)
- Steps to reproduce the issue
- Full error output or traceback
- Any relevant CLI flags used

Do not include session IDs, access tokens, credentials, or org-identifying information in issue reports.

## Development Setup

```bash
git clone https://github.com/NetSPI/ForceHound.git
cd ForceHound
pip install -e ".[dev]"
```

## Running Tests

```bash
python -m pytest tests/ -x -q
```

## Pull Requests

- Open an issue first to discuss the change
- Create a feature branch from `main`
- Include tests for new functionality
- Make sure all existing tests pass before submitting
- Keep PRs focused on a single change

## Code Style

- Python 3.9+ compatible
- Type hints on function signatures
- Docstrings on public classes and methods
