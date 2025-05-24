#!/usr/bin/env python3
"""
Test Runner for parquet-lens Test Suite

This script provides a comprehensive test runner for the parquet-lens tool,
supporting multiple test frameworks and providing detailed reporting.

Usage:
    python run_tests.py [options]
    
Options:
    --framework {unittest,pytest,all}  Test framework to use (default: all)
    --category {unit,integration,performance,all}  Test category (default: all)
    --verbose                          Verbose output
    --coverage                         Run with coverage reporting
    --html-report                      Generate HTML coverage report
    --benchmark                        Run performance benchmarks
    --clean                           Clean up test artifacts before running
    --help                            Show this help message
"""

import argparse
import os
import sys
import subprocess
import time
from pathlib import Path


class TestRunner:
    """Main test runner class"""
    
    def __init__(self):
        self.project_root = Path(__file__).parent
        self.test_results = {}
        
    def run_unittest_suite(self, verbose=False, category='all'):
        """Run tests using unittest framework"""
        print("🔬 Running unittest suite...")
        
        test_files = []
        if category in ['all', 'unit', 'integration']:
            test_files.append('test_parquet_lens.py')
        if category in ['all', 'performance']:
            test_files.append('test_performance.py')
            
        results = {}
        for test_file in test_files:
            if os.path.exists(test_file):
                print(f"  Running {test_file}...")
                cmd = [sys.executable, test_file]
                if verbose:
                    cmd.append('--verbose')
                    
                try:
                    result = subprocess.run(
                        cmd, 
                        capture_output=True, 
                        text=True, 
                        timeout=300
                    )
                    results[test_file] = {
                        'returncode': result.returncode,
                        'stdout': result.stdout,
                        'stderr': result.stderr
                    }
                    
                    if result.returncode == 0:
                        print(f"  ✅ {test_file} PASSED")
                    else:
                        print(f"  ❌ {test_file} FAILED")
                        if verbose:
                            print(f"     Error: {result.stderr}")
                            
                except subprocess.TimeoutExpired:
                    print(f"  ⏰ {test_file} TIMEOUT")
                    results[test_file] = {'returncode': -1, 'timeout': True}
                    
            else:
                print(f"  ⚠️  {test_file} not found")
                
        return results
    
    def run_pytest_suite(self, verbose=False, category='all', coverage=False):
        """Run tests using pytest framework"""
        print("🧪 Running pytest suite...")
        
        if not self._check_pytest_available():
            print("  ⚠️  pytest not available, skipping")
            return {'error': 'pytest not available'}
            
        cmd = [sys.executable, '-m', 'pytest']
        
        # Add test selection based on category
        if category == 'unit':
            cmd.extend(['-m', 'unit'])
        elif category == 'integration':
            cmd.extend(['-m', 'integration'])
        elif category == 'performance':
            cmd.extend(['-m', 'performance'])
        elif category != 'all':
            cmd.extend(['-k', category])
            
        if verbose:
            cmd.append('-v')
        else:
            cmd.append('-q')
            
        if coverage:
            if self._check_pytest_cov_available():
                cmd.extend(['--cov=parquet_lens', '--cov-report=term-missing'])
            else:
                print("  ⚠️  pytest-cov not available, running without coverage")
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.returncode == 0:
                print("  ✅ pytest PASSED")
            else:
                print("  ❌ pytest FAILED")
                if verbose:
                    print(f"     Output: {result.stdout}")
                    print(f"     Error: {result.stderr}")
                    
            return {
                'returncode': result.returncode,
                'stdout': result.stdout,
                'stderr': result.stderr
            }
            
        except subprocess.TimeoutExpired:
            print("  ⏰ pytest TIMEOUT")
            return {'returncode': -1, 'timeout': True}
    
    def run_coverage_report(self):
        """Generate coverage reports"""
        print("📊 Generating coverage reports...")
        
        if not self._check_coverage_available():
            print("  ⚠️  coverage.py not available")
            return False
            
        # Run tests with coverage
        cmd = [
            sys.executable, '-m', 'coverage', 'run',
            '--source=parquet_lens',
            'test_parquet_lens.py'
        ]
        
        try:
            subprocess.run(cmd, check=True, timeout=300)
            
            # Generate reports
            subprocess.run([sys.executable, '-m', 'coverage', 'report'], check=True)
            subprocess.run([sys.executable, '-m', 'coverage', 'html'], check=True)
            
            print("  ✅ Coverage reports generated")
            print("  📁 HTML report: htmlcov/index.html")
            return True
            
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            print(f"  ❌ Coverage generation failed: {e}")
            return False
    
    def run_benchmarks(self):
        """Run performance benchmarks"""
        print("⚡ Running performance benchmarks...")
        
        # Simple benchmark of core functions
        import parquet_lens
        
        # Create a small test file for benchmarking
        test_file = self._create_benchmark_file()
        if not test_file:
            print("  ⚠️  Could not create benchmark file")
            return
            
        try:
            # Benchmark the main analysis function
            start_time = time.time()
            for _ in range(10):  # Run 10 times
                result = parquet_lens.analyze_parquet_file(test_file)
            end_time = time.time()
            
            avg_time = (end_time - start_time) / 10
            print(f"  📈 Average analysis time: {avg_time:.4f}s")
            
            # Cleanup
            os.unlink(test_file)
            
        except Exception as e:
            print(f"  ❌ Benchmark failed: {e}")
    
    def clean_artifacts(self):
        """Clean up test artifacts"""
        print("🧹 Cleaning up test artifacts...")
        
        artifacts = [
            '.coverage',
            'htmlcov',
            '__pycache__',
            '.pytest_cache',
            '*.pyc',
            '*.pyo',
            'test_*.parquet'
        ]
        
        for pattern in artifacts:
            try:
                if pattern.startswith('.') or not '*' in pattern:
                    # Directory or specific file
                    path = Path(pattern)
                    if path.exists():
                        if path.is_dir():
                            import shutil
                            shutil.rmtree(path)
                        else:
                            path.unlink()
                else:
                    # Glob pattern
                    for path in Path('.').glob(pattern):
                        if path.is_file():
                            path.unlink()
            except Exception as e:
                print(f"  ⚠️  Could not clean {pattern}: {e}")
                
        print("  ✅ Cleanup completed")
    
    def _check_pytest_available(self):
        """Check if pytest is available"""
        try:
            subprocess.run([sys.executable, '-m', 'pytest', '--version'], 
                         capture_output=True, check=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False
    
    def _check_pytest_cov_available(self):
        """Check if pytest-cov is available"""
        try:
            subprocess.run([sys.executable, '-c', 'import pytest_cov'], 
                         capture_output=True, check=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False
    
    def _check_coverage_available(self):
        """Check if coverage.py is available"""
        try:
            subprocess.run([sys.executable, '-m', 'coverage', '--version'], 
                         capture_output=True, check=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False
    
    def _create_benchmark_file(self):
        """Create a small test file for benchmarking"""
        try:
            import tempfile
            import pandas as pd
            import pyarrow as pa
            import pyarrow.parquet as pq
            
            # Create small test data
            data = {'id': [1, 2, 3], 'name': ['a', 'b', 'c']}
            df = pd.DataFrame(data)
            table = pa.Table.from_pandas(df)
            
            fd, temp_file = tempfile.mkstemp(suffix='.parquet')
            os.close(fd)
            
            pq.write_table(table, temp_file)
            return temp_file
            
        except ImportError:
            return None
    
    def print_summary(self, results):
        """Print test results summary"""
        print("\n" + "="*60)
        print("🏁 TEST RESULTS SUMMARY")
        print("="*60)
        
        total_tests = 0
        passed_tests = 0
        
        for framework, result in results.items():
            if isinstance(result, dict) and 'returncode' in result:
                total_tests += 1
                if result['returncode'] == 0:
                    passed_tests += 1
                    status = "✅ PASS"
                else:
                    status = "❌ FAIL"
                print(f"{framework:20} {status}")
            elif isinstance(result, dict) and 'error' in result:
                print(f"{framework:20} ⚠️  SKIP ({result['error']})")
        
        if total_tests > 0:
            success_rate = (passed_tests / total_tests) * 100
            print(f"\nOverall: {passed_tests}/{total_tests} ({success_rate:.1f}%)")
            
            if success_rate == 100:
                print("🎉 All tests passed!")
            elif success_rate >= 80:
                print("⚠️  Most tests passed, but some issues found")
            else:
                print("❌ Multiple test failures detected")
        
        print("="*60)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Test runner for parquet-lens",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--framework',
        choices=['unittest', 'pytest', 'all'],
        default='all',
        help='Test framework to use'
    )
    
    parser.add_argument(
        '--category',
        choices=['unit', 'integration', 'performance', 'all'],
        default='all',
        help='Test category to run'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Verbose output'
    )
    
    parser.add_argument(
        '--coverage',
        action='store_true',
        help='Run with coverage reporting'
    )
    
    parser.add_argument(
        '--html-report',
        action='store_true',
        help='Generate HTML coverage report'
    )
    
    parser.add_argument(
        '--benchmark',
        action='store_true',
        help='Run performance benchmarks'
    )
    
    parser.add_argument(
        '--clean',
        action='store_true',
        help='Clean up test artifacts before running'
    )
    
    args = parser.parse_args()
    
    # Initialize test runner
    runner = TestRunner()
    
    print("🚀 parquet-lens Test Suite Runner")
    print("="*50)
    
    # Clean artifacts if requested
    if args.clean:
        runner.clean_artifacts()
        print()
    
    # Run tests based on framework selection
    results = {}
    
    if args.framework in ['unittest', 'all']:
        results['unittest'] = runner.run_unittest_suite(
            verbose=args.verbose,
            category=args.category
        )
    
    if args.framework in ['pytest', 'all']:
        results['pytest'] = runner.run_pytest_suite(
            verbose=args.verbose,
            category=args.category,
            coverage=args.coverage
        )
    
    # Generate coverage report if requested
    if args.coverage or args.html_report:
        runner.run_coverage_report()
    
    # Run benchmarks if requested
    if args.benchmark:
        runner.run_benchmarks()
    
    # Print summary
    runner.print_summary(results)
    
    # Exit with appropriate code
    success = all(
        r.get('returncode') == 0 
        for r in results.values() 
        if isinstance(r, dict) and 'returncode' in r
    )
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
