#!/usr/bin/env python3
"""
Simplified test runner for parquet-lens
"""

import os
import sys
import unittest
import coverage

# Add the parent directory to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Start coverage
cov = coverage.Coverage(source=["parquet_lens"])
cov.start()

# Import the test modules directly
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.test_parquet_lens_working import TestOffsetRecordingProtocol  # Just try one class first

def create_test_suite():
    """Create a simplified test suite"""
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestOffsetRecordingProtocol))
    return suite

if __name__ == "__main__":
    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(create_test_suite())
    
    # Stop coverage and report
    cov.stop()
    cov.save()
    print("\nCoverage Report:")
    cov.report()
    
    # Exit with appropriate status code
    sys.exit(0 if result.wasSuccessful() else 1)
