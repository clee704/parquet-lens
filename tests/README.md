# Parquet-Lens Tests

This directory contains the test suite for parquet-lens, a tool for analyzing Parquet files.

## Test Files

- `test_parquet_lens_working.py`: Main test suite with basic functionality tests
- `test_coverage_push.py`: Additional tests to improve coverage
- `test_final_push.py`: Final tests to reach >80% coverage
- `test_performance.py`: Performance benchmarks

## Running Tests

To run all tests with coverage:

```bash
python -m tests.run_tests
```

To generate an HTML coverage report:

```bash
python -m tests.run_tests --html
```

## Coverage

Current test coverage: **84%**
