# Tests Directory

This directory contains unit tests for the data-scripts package.

## Test Structure

- `test_settings.py` - Tests for the settings configuration module
- `test_create_iceberg_table.py` - Tests for the Iceberg table creation and data insertion script
- `__init__.py` - Makes this directory a Python package

## Running Tests

### Using Poetry (Recommended)

```bash
# Run all tests
poetry run python -m pytest

# Run with verbose output
poetry run python -m pytest -v

# Run with coverage
poetry run python -m pytest --cov=. --cov-report=term-missing

# Run specific test file
poetry run python -m pytest tests/test_create_iceberg_table.py

# Run specific test class or function
poetry run python -m pytest tests/test_create_iceberg_table.py::TestGenerateSampleData
poetry run python -m pytest -k "test_generate_sample_data"
```

### Using Make Commands

```bash
# Run all tests
make test

# Run with verbose output  
make test-v

# Run with coverage report
make test-cov

# Run specific test file
make test-file FILE=test_create_iceberg_table.py

# Run complete test suite with linting
make all
```

### Using Test Runner Script

```bash
# Run tests
python run_tests.py test

# Run with coverage and verbose output
python run_tests.py test -c -v

# Run specific test file
python run_tests.py test -f test_create_iceberg_table.py

# Run tests matching pattern
python run_tests.py test -k "generate_sample_data"

# Run complete suite (format + lint + test)
python run_tests.py all
```

## Test Categories

### Unit Tests
- Test individual functions in isolation
- Use mocks for external dependencies (TrinoClient, database connections)
- Focus on logic, data validation, and error handling

### Integration Tests  
- Test interactions between multiple components
- Verify data flow between functions
- Test end-to-end scenarios with mocked external services

## Test Fixtures

The tests use several fixtures and utilities:

- `sample_dataframe` - Provides sample pandas DataFrame for testing
- `mock_trino_client` - Mock TrinoClient for database operations
- Parameterized tests for testing multiple scenarios efficiently

## Coverage

Run tests with coverage to ensure comprehensive test coverage:

```bash
poetry run python -m pytest --cov=. --cov-report=html
```

This generates an HTML coverage report in `htmlcov/index.html`.

## Best Practices

1. **Isolation**: Each test should be independent and not rely on other tests
2. **Mocking**: Mock external dependencies (databases, APIs, file systems)
3. **Descriptive Names**: Use clear, descriptive test names that explain what is being tested
4. **Edge Cases**: Test both happy paths and edge cases (empty data, errors, etc.)
5. **Documentation**: Document complex test logic and assumptions

## Adding New Tests

When adding new functionality:

1. Create corresponding test file following the naming pattern `test_<module_name>.py`
2. Organize tests into classes by functionality
3. Include both positive and negative test cases
4. Add integration tests for complex workflows
5. Update this README if adding new test patterns or utilities