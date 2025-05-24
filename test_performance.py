#!/usr/bin/env python3
"""
Performance and Large File Test Suite for parquet-lens

Tests the performance characteristics and memory usage of the tool
with various file sizes and complexity levels.
"""

import unittest
import tempfile
import os
import sys
import time
import gc
import tracemalloc
from unittest.mock import patch

# Add the parent directory to sys.path so we can import parquet_lens
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import parquet_lens


class TestPerformance(unittest.TestCase):
    """Performance tests for parquet-lens"""
    
    def setUp(self):
        """Set up performance test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        tracemalloc.start()
    
    def tearDown(self):
        """Clean up performance test fixtures"""
        tracemalloc.stop()
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_memory_usage(self):
        """Test memory usage during analysis"""
        try:
            import pandas as pd
            import pyarrow as pa
            import pyarrow.parquet as pq
            
            # Create a moderately sized dataset
            data = {
                'id': list(range(1000)),
                'name': [f'name_{i}' for i in range(1000)],
                'value': [float(i) * 1.1 for i in range(1000)]
            }
            df = pd.DataFrame(data)
            table = pa.Table.from_pandas(df)
            
            test_file = os.path.join(self.temp_dir, 'perf_test.parquet')
            pq.write_table(table, test_file)
            
            # Measure memory before
            gc.collect()
            snapshot1 = tracemalloc.take_snapshot()
            
            # Run analysis
            result = parquet_lens.analyze_parquet_file(test_file)
            
            # Measure memory after
            snapshot2 = tracemalloc.take_snapshot()
            top_stats = snapshot2.compare_to(snapshot1, 'lineno')
            
            # Memory usage should be reasonable (less than 50MB for this test)
            total_memory = sum(stat.size for stat in top_stats)
            self.assertLess(total_memory, 50 * 1024 * 1024, "Memory usage too high")
            
            # Should produce valid results
            self.assertIsInstance(result, list)
            
        except ImportError:
            self.skipTest("pandas/pyarrow not available for performance testing")
    
    def test_analysis_speed(self):
        """Test analysis speed"""
        try:
            import pandas as pd
            import pyarrow as pa
            import pyarrow.parquet as pq
            
            # Create test data
            data = {
                'id': list(range(100)),
                'name': [f'name_{i}' for i in range(100)],
            }
            df = pd.DataFrame(data)
            table = pa.Table.from_pandas(df)
            
            test_file = os.path.join(self.temp_dir, 'speed_test.parquet')
            pq.write_table(table, test_file)
            
            # Time the analysis
            start_time = time.time()
            result = parquet_lens.analyze_parquet_file(test_file)
            end_time = time.time()
            
            analysis_time = end_time - start_time
            
            # Should complete in reasonable time (less than 5 seconds for small file)
            self.assertLess(analysis_time, 5.0, f"Analysis took too long: {analysis_time:.2f}s")
            
            # Should produce valid results
            self.assertIsInstance(result, list)
            
        except ImportError:
            self.skipTest("pandas/pyarrow not available for speed testing")


class TestLargeFiles(unittest.TestCase):
    """Test handling of large Parquet files"""
    
    def setUp(self):
        """Set up large file test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up large file test fixtures"""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_large_metadata(self):
        """Test handling of files with large metadata sections"""
        try:
            import pandas as pd
            import pyarrow as pa
            import pyarrow.parquet as pq
            
            # Create data with many columns to increase metadata size
            data = {}
            for i in range(50):  # 50 columns
                data[f'col_{i}'] = [f'value_{i}_{j}' for j in range(100)]
            
            df = pd.DataFrame(data)
            table = pa.Table.from_pandas(df)
            
            test_file = os.path.join(self.temp_dir, 'large_metadata.parquet')
            pq.write_table(table, test_file)
            
            # Should handle large metadata without issues
            result = parquet_lens.analyze_parquet_file(test_file)
            self.assertIsInstance(result, list)
            
            # Should have metadata blob
            metadata_segments = [s for s in result if s.get('type') == 'thrift_metadata_blob']
            self.assertTrue(len(metadata_segments) > 0)
            
        except ImportError:
            self.skipTest("pandas/pyarrow not available for large file testing")
    
    def test_complex_schema(self):
        """Test handling of files with complex nested schemas"""
        try:
            import pandas as pd
            import pyarrow as pa
            import pyarrow.parquet as pq
            
            # Create nested data structure
            data = {
                'id': [1, 2, 3],
                'nested': [
                    {'a': 1, 'b': 'x'},
                    {'a': 2, 'b': 'y'}, 
                    {'a': 3, 'b': 'z'}
                ],
                'list_col': [[1, 2], [3, 4], [5, 6]]
            }
            df = pd.DataFrame(data)
            table = pa.Table.from_pandas(df)
            
            test_file = os.path.join(self.temp_dir, 'complex_schema.parquet')
            pq.write_table(table, test_file)
            
            # Should handle complex schemas
            result = parquet_lens.analyze_parquet_file(test_file)
            self.assertIsInstance(result, list)
            
        except ImportError:
            self.skipTest("pandas/pyarrow not available for complex schema testing")


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and boundary conditions"""
    
    def setUp(self):
        """Set up edge case test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up edge case test fixtures"""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_minimal_file_size(self):
        """Test analysis of minimally sized Parquet files"""
        # Create the smallest possible valid Parquet file
        test_file = os.path.join(self.temp_dir, 'minimal.parquet')
        
        try:
            import pandas as pd
            import pyarrow as pa
            import pyarrow.parquet as pq
            
            # Single row, single column
            data = {'col': [1]}
            df = pd.DataFrame(data)
            table = pa.Table.from_pandas(df)
            pq.write_table(table, test_file)
            
            result = parquet_lens.analyze_parquet_file(test_file)
            self.assertIsInstance(result, list)
            
        except ImportError:
            self.skipTest("pandas/pyarrow not available for minimal file testing")
    
    def test_zero_size_file(self):
        """Test handling of zero-byte files"""
        test_file = os.path.join(self.temp_dir, 'zero.parquet')
        open(test_file, 'w').close()  # Create empty file
        
        result = parquet_lens.analyze_parquet_file(test_file)
        self.assertIsInstance(result, dict)
        self.assertIn('segments', result)
        # Should handle gracefully without crashing
    
    def test_truncated_file(self):
        """Test handling of truncated Parquet files"""
        try:
            import pandas as pd
            import pyarrow as pa
            import pyarrow.parquet as pq
            
            # Create a normal file first
            data = {'col': [1, 2, 3]}
            df = pd.DataFrame(data)
            table = pa.Table.from_pandas(df)
            
            full_file = os.path.join(self.temp_dir, 'full.parquet')
            pq.write_table(table, full_file)
            
            # Truncate it
            truncated_file = os.path.join(self.temp_dir, 'truncated.parquet')
            with open(full_file, 'rb') as src:
                data = src.read()
                # Truncate to half size
                truncated_data = data[:len(data)//2]
            
            with open(truncated_file, 'wb') as dst:
                dst.write(truncated_data)
            
            # Should handle truncated files gracefully
            result = parquet_lens.analyze_parquet_file(truncated_file)
            self.assertIsInstance(result, list)
            
        except ImportError:
            self.skipTest("pandas/pyarrow not available for truncation testing")
    
    def test_unicode_handling(self):
        """Test handling of Unicode data in Parquet files"""
        try:
            import pandas as pd
            import pyarrow as pa
            import pyarrow.parquet as pq
            
            # Create data with Unicode characters
            data = {
                'unicode_col': ['hello', 'world', '你好', '世界', '🌍', 'café'],
                'normal_col': [1, 2, 3, 4, 5, 6]
            }
            df = pd.DataFrame(data)
            table = pa.Table.from_pandas(df)
            
            test_file = os.path.join(self.temp_dir, 'unicode.parquet')
            pq.write_table(table, test_file)
            
            result = parquet_lens.analyze_parquet_file(test_file)
            self.assertIsInstance(result, list)
            
            # Should be JSON serializable despite Unicode
            import json
            json_str = json.dumps(result, ensure_ascii=False)
            self.assertIsInstance(json_str, str)
            
        except ImportError:
            self.skipTest("pandas/pyarrow not available for Unicode testing")


if __name__ == '__main__':
    print("Running Performance and Edge Case Tests for parquet-lens...")
    unittest.main(verbosity=2)
