#!/usr/bin/env python3
"""
Main test runner for parquet-lens

This script runs all the tests for parquet-lens and reports the coverage.
"""

import os
import sys
import unittest
import coverage

# Add the parent directory to sys.path to import parquet_lens
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the test modules
from tests.test_parquet_lens_working import (
    TestOffsetRecordingProtocol,
    TestThriftTypeSystem,
    TestThriftToDictConversion,
    TestAnalyzeParquetFile,
    TestCommandLineInterface,
    TestEnumHandling,
    TestErrorHandling,
    TestRegressionTests,
    TestIntegrationTests,
    TestCoverageExpansion,
    TestFinalCoverageTarget,
    TestTargetedCoverage80Percent
)
from tests.test_coverage_push import TestTargeted80PercentCoverage
from tests.test_final_push import TestFinalCoveragePush
# Import test classes from test_coverage_expansion.py
from tests.test_coverage_expansion import (
    TestTransportErrorHandling,
    TestNestedStructHandling,
    TestListHandling,
    TestEnumHandlingExpanded,
    TestComplexTypeHandling,
    TestByteHandling,
    TestStructWithoutThriftSpec,
    TestComplexListProcessing,
    TestNestedStructOffsets,
    TestFileAnalysisEdgeCases,
    TestFieldSpecResolution
)


def create_test_suite():
    """Create a comprehensive test suite"""
    suite = unittest.TestSuite()
    
    # Add tests from test_parquet_lens_working
    for test_class in [
        TestOffsetRecordingProtocol,
        TestThriftTypeSystem,
        TestThriftToDictConversion,
        TestAnalyzeParquetFile,
        TestCommandLineInterface,
        TestEnumHandling,
        TestErrorHandling,
        TestRegressionTests,
        TestIntegrationTests,
        TestCoverageExpansion,
        TestFinalCoverageTarget,
        TestTargetedCoverage80Percent
    ]:
        suite.addTest(unittest.makeSuite(test_class))
    
    # Add tests from test_coverage_push
    suite.addTest(unittest.makeSuite(TestTargeted80PercentCoverage))
    
    # Add tests from test_final_push
    suite.addTest(unittest.makeSuite(TestFinalCoveragePush))
    
    # Add tests from test_coverage_expansion
    for test_class in [
        TestTransportErrorHandling,
        TestNestedStructHandling,
        TestListHandling,
        TestEnumHandlingExpanded,
        TestComplexTypeHandling,
        TestByteHandling,
        TestStructWithoutThriftSpec,
        TestComplexListProcessing,
        TestNestedStructOffsets,
        TestFileAnalysisEdgeCases,
        TestFieldSpecResolution
    ]:
        suite.addTest(unittest.makeSuite(test_class))
    
    return suite


if __name__ == "__main__":
    # Start coverage
    cov = coverage.Coverage(source=["parquet_lens"])
    cov.start()
    
    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(create_test_suite())
    
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
