# Parquet Lens

A developer tool for deep inspection of Parquet files. Provides a detailed byte-by-byte mapping of Parquet file structures, accounting for every byte and explaining Thrift structs and data segments.

## Features

*   Sequential, non-overlapping segment analysis of a Parquet file.
*   Detailed information for each segment:
    *   Byte range (`[start, end]`)
    *   Type (e.g., `magic_number`, `thrift`, `data`, `padding`, `unknown`, `error`)
    *   For Thrift segments:
        *   Thrift type (e.g., `struct`, `enum`, `i32`)
        *   Name (e.g., `PageHeader`, `FileMetaData`)
        *   Field details (range, type, name, value, required/optional status)
    *   For data segments:
        *   Encoding
        *   Compression
        *   Statistics (if available)
*   Error tolerance: Identifies and reports erroneous segments.
*   Scalable output for large files (focus on metadata and ranges, not necessarily full data dumps).

## Installation

```bash
pip install -e .
```

## Usage

```bash
python parquet_lens.py <parquet_file> [options]
```

### Options
- `--debug`: Enable debug mode for detailed output
- `--show-list-headers`: Show headers for list fields
- `--show-undefined-optional`: Show undefined optional fields
- `--output <file>`: Write output to specified file

## Testing

This project includes a comprehensive test suite with >84% code coverage.

### Running Tests

```bash
# Run all tests with coverage
python -m pytest tests/ --cov=parquet_lens

# Run tests and generate HTML coverage report
python -m pytest tests/ --cov=parquet_lens --cov-report=html

# Run specific test files
python -m pytest tests/test_parquet_lens_working.py
```

### Test Structure

- `tests/test_parquet_lens_working.py`: Core functionality tests
- `tests/test_coverage_push.py`: Additional tests for coverage improvement  
- `tests/test_final_push.py`: Final tests to reach 80%+ coverage
- `tests/test_performance.py`: Performance benchmarks
- `tests/test_coverage_expansion.py`: Extended coverage tests

The test suite covers:
- Offset recording protocol functionality
- Thrift type system handling
- Parquet file analysis
- Command line interface
- Error handling and edge cases
- Enum processing
- List and struct handling

### Test Runners

- `tests/run_tests.py`: Main test runner with coverage reporting
- `tests/consolidated_test_runner.py`: Alternative test runner
- `tests/simple_test.py`: Minimal test runner

## Development

For development setup:

1. Install dependencies: `pip install -r requirements.txt`
2. Run tests: `python -m pytest tests/`
3. Check coverage: `python -m pytest tests/ --cov=parquet_lens --cov-report=html`

## Coverage

Current test coverage: **84%** (352/417 lines covered)

The test suite prevents breaking changes during development and ensures code quality.
