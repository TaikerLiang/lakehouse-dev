# Makefile for data-scripts project
# Provides convenient commands for testing, linting, and development

.PHONY: help test test-v test-cov lint format check install clean all

# Default target
help:
	@echo "Available commands:"
	@echo "  make test      - Run tests"
	@echo "  make test-v    - Run tests with verbose output"
	@echo "  make test-cov  - Run tests with coverage report"
	@echo "  make lint      - Run linting checks (ruff, black, mypy)"
	@echo "  make format    - Format code (black, ruff --fix)"
	@echo "  make check     - Run linting without fixing"
	@echo "  make install   - Install dependencies with poetry"
	@echo "  make clean     - Clean cache files"
	@echo "  make all       - Run format, lint, and test with coverage"

# Test commands
test:
	poetry run python -m pytest

test-v:
	poetry run python -m pytest -v

test-cov:
	poetry run python -m pytest --cov=. --cov-report=term-missing --cov-report=html

test-file:
	@if [ -z "$(FILE)" ]; then \
		echo "Usage: make test-file FILE=test_filename.py"; \
	else \
		poetry run python -m pytest tests/$(FILE) -v; \
	fi

# Linting and formatting
lint:
	poetry run python -m ruff check .
	poetry run python -m black --check .
	poetry run python -m mypy .

format:
	poetry run python -m black .
	poetry run python -m ruff --fix .

check:
	poetry run python -m ruff check .
	poetry run python -m black --check --diff .

# Development commands
install:
	poetry install

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf htmlcov/ 2>/dev/null || true
	rm -f .coverage 2>/dev/null || true

# Run everything
all: format lint test-cov
	@echo "✅ All checks completed successfully!"

# Specific test patterns
test-create-iceberg:
	poetry run python -m pytest tests/test_create_iceberg_table.py -v

test-settings:
	poetry run python -m pytest tests/test_settings.py -v

# Development workflow
dev-setup: install
	@echo "🚀 Development environment setup complete!"
	@echo "Run 'make test' to verify everything works"

# CI/CD friendly command
ci: check test-cov
	@echo "✅ CI checks completed successfully!"