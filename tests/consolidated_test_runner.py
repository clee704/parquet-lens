#!/usr/bin/env python3
"""
Consolidated test runner for parquet-lens

This script collects all the tests that contribute to the 84% coverage and runs them.
"""

import os
import sys
import unittest
import coverage
import importlib.util

# Add the parent directory to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import parquet_lens directly
import parquet_lens

def load_test_module(file_path):
    """Load a test module from file path"""
    module_name = os.path.basename(file_path).replace('.py', '')
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

# Define test files to load
test_files = [
    os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_parquet_lens_working.py'),
    os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_coverage_push.py'), 
    os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_final_push.py'),
    os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_coverage_expansion.py')
]

# Start coverage
cov = coverage.Coverage(source=["parquet_lens"])
cov.start()

# Load test modules
test_modules = [load_test_module(file) for file in test_files]

# Create test suite
suite = unittest.TestSuite()

# Add all test classes from all modules
for module in test_modules:
    for item in dir(module):
        item_value = getattr(module, item)
        if isinstance(item_value, type) and issubclass(item_value, unittest.TestCase) and item_value != unittest.TestCase:
            suite.addTest(unittest.makeSuite(item_value))

if __name__ == "__main__":
    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Stop coverage and report
    cov.stop()
    cov.save()
    print("\nCoverage Report:")
    cov.report()
    
    # Generate HTML report if requested
    if "--html" in sys.argv:
        cov.html_report(directory="htmlcov")
        print("\nHTML report generated in 'htmlcov' directory")
    
    # Exit with appropriate status code
    sys.exit(0 if result.wasSuccessful() else 1)
