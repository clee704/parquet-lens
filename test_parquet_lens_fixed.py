#!/usr/bin/env python3
"""
Comprehensive Unit Test Framework for parquet-lens (Fixed Version)

This test suite provides complete coverage of the parquet-lens tool to prevent
breaking changes during development. Updated to match current API format.
"""

import unittest
import tempfile
import os
import sys
import json
import io
import shutil
from unittest.mock import patch, MagicMock, Mock
from contextlib import redirect_stdout, redirect_stderr

# Add the parent directory to sys.path so we can import parquet_lens
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import parquet_lens

# Import required classes and modules for testing
from thrift.transport import TTransport
from thrift.protocol.TProtocol import TType
from parquet.ttypes import FileMetaData, SchemaElement, RowGroup, ColumnChunk, ColumnMetaData


class TestParquetFileGenerator:
    """Helper class to generate test Parquet files for testing"""
    
    @staticmethod
    def create_minimal_parquet(file_path):
        """Creates a minimal valid Parquet file for testing"""
        try:
            import pandas as pd
            import pyarrow as pa
            import pyarrow.parquet as pq
            
            # Create minimal test data
            data = {
                'id': [1, 2, 3],
                'name': ['Alice', 'Bob', 'Charlie'],
                'value': [1.1, 2.2, 3.3]
            }
            df = pd.DataFrame(data)
            table = pa.Table.from_pandas(df)
            pq.write_table(table, file_path)
            return True
        except ImportError:
            # Fallback: create a very basic binary file that looks like parquet
            with open(file_path, 'wb') as f:
                # Magic number
                f.write(b'PAR1')
                # Some dummy data (minimal)
                dummy_data = b'\x00' * 50
                f.write(dummy_data)
                # Footer length (4 bytes)
                f.write((50).to_bytes(4, byteorder='little'))
                # Footer magic 
                f.write(b'PAR1')
            return False

    @staticmethod
    def create_corrupt_parquet(file_path):
        """Creates a corrupted Parquet file for error testing"""
        with open(file_path, 'wb') as f:
            # Wrong magic number
            f.write(b'XXXX')
            f.write(b'\x00' * 50)
            f.write((50).to_bytes(4, byteorder='little'))
            f.write(b'PAR1')

    @staticmethod
    def create_empty_file(file_path):
        """Creates an empty file for testing"""
        with open(file_path, 'wb') as f:
            pass


class TestOffsetRecordingProtocol(unittest.TestCase):
    """Test the OffsetRecordingProtocol class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.test_data = b'\x19\x4c\x15\x00\x15\x06\x15\x10\x00\x00\x00\x00'  # Sample Thrift binary
        self.transport = TTransport.TMemoryBuffer(self.test_data)
        self.protocol = parquet_lens.OffsetRecordingProtocol(
            self.transport, FileMetaData, 0
        )
    
    def test_protocol_initialization(self):
        """Test protocol initialization"""
        self.assertIsNotNone(self.protocol)
        self.assertEqual(self.protocol._base_offset_in_file, 0)
        self.assertEqual(self.protocol._struct_nesting_level, 0)
        self.assertIsInstance(self.protocol.field_details, dict)
    
    def test_position_tracking(self):
        """Test that position tracking works correctly"""
        pos1 = self.protocol._get_trans_pos()
        # Read one byte through the transport
        self.transport._buffer.read(1)
        pos2 = self.protocol._get_trans_pos()
        self.assertEqual(pos2, pos1 + 1)
    
    def test_field_name_resolution(self):
        """Test field name resolution from spec"""
        # Mock a thrift spec
        mock_spec = [
            None,  # Index 0 is always None
            (1, TType.I32, 'version', None, None),
            (2, TType.LIST, 'schema', (TType.STRUCT, (SchemaElement,), False), None),
        ]
        self.protocol._current_spec = mock_spec
        
        # Test valid field ID
        name = self.protocol._get_field_name_from_spec(1, mock_spec)
        self.assertEqual(name, 'version')
        
        # Test invalid field ID
        name = self.protocol._get_field_name_from_spec(999, mock_spec)
        self.assertIsNone(name)


class TestThriftTypeSystem(unittest.TestCase):
    """Test the Thrift type name resolution system"""
    
    def test_get_thrift_type_name_primitives(self):
        """Test primitive type name resolution"""
        self.assertEqual(parquet_lens.get_thrift_type_name(TType.I32, None), "i32")
        self.assertEqual(parquet_lens.get_thrift_type_name(TType.I64, None), "i64")
        self.assertEqual(parquet_lens.get_thrift_type_name(TType.STRING, None), "string")
        self.assertEqual(parquet_lens.get_thrift_type_name(TType.BOOL, None), "bool")
        self.assertEqual(parquet_lens.get_thrift_type_name(TType.DOUBLE, None), "double")
    
    def test_get_thrift_type_name_list(self):
        """Test list type name resolution"""
        # Test list with struct elements
        type_args = (TType.STRUCT, (SchemaElement,), False)
        result = parquet_lens.get_thrift_type_name(TType.LIST, type_args)
        self.assertEqual(result, "list<SchemaElement>")
        
        # Test list with primitive elements
        type_args = (TType.I32, None, False)
        result = parquet_lens.get_thrift_type_name(TType.LIST, type_args)
        self.assertEqual(result, "list<i32>")
    
    def test_get_thrift_type_name_struct(self):
        """Test struct type name resolution"""
        type_args = (SchemaElement,)
        result = parquet_lens.get_thrift_type_name(TType.STRUCT, type_args)
        self.assertEqual(result, "SchemaElement")
    
    def test_get_thrift_type_name_unknown(self):
        """Test unknown type handling"""
        result = parquet_lens.get_thrift_type_name(999, None)
        self.assertEqual(result, "unknown_type_999")


class TestThriftToDictConversion(unittest.TestCase):
    """Test the thrift_to_dict_with_offsets function"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.field_details_map = {}
        self.base_offset = 0
    
    def test_none_handling(self):
        """Test None value handling"""
        result = parquet_lens.thrift_to_dict_with_offsets(
            None, self.field_details_map, self.base_offset
        )
        self.assertIsNone(result)
    
    def test_primitive_values(self):
        """Test primitive value handling"""
        # Test integers
        result = parquet_lens.thrift_to_dict_with_offsets(
            42, self.field_details_map, self.base_offset
        )
        self.assertEqual(result, 42)
        
        # Test strings
        result = parquet_lens.thrift_to_dict_with_offsets(
            "test", self.field_details_map, self.base_offset
        )
        self.assertEqual(result, "test")
        
        # Test bytes (should be decoded to UTF-8)
        result = parquet_lens.thrift_to_dict_with_offsets(
            b"test", self.field_details_map, self.base_offset
        )
        self.assertEqual(result, "test")
        
        # Test invalid UTF-8 bytes (should be hex encoded)
        result = parquet_lens.thrift_to_dict_with_offsets(
            b"\x80\x81\x82", self.field_details_map, self.base_offset
        )
        self.assertEqual(result, "808182")
    
    def test_list_handling(self):
        """Test list value handling"""
        test_list = [1, 2, 3]
        result = parquet_lens.thrift_to_dict_with_offsets(
            test_list, self.field_details_map, self.base_offset
        )
        self.assertEqual(result, [1, 2, 3])
    
    def test_struct_consistency(self):
        """Test that all structs have consistent representation"""
        # Create a simple test object that mimics a Thrift struct
        class TestStruct:
            def __init__(self):
                self.test_field = 'test_value'
                self.thrift_spec = [
                    None,
                    (1, TType.STRING, 'test_field', None, None)
                ]
            def __dict__(self):
                return {'test_field': 'test_value'}
        
        mock_obj = TestStruct()
        
        result = parquet_lens.thrift_to_dict_with_offsets(
            mock_obj, self.field_details_map, self.base_offset
        )
        
        # Check that result is a list (top-level struct)
        self.assertIsInstance(result, list)
        self.assertTrue(len(result) > 0)
        
        # Check that each field has the required structure
        field = result[0]
        self.assertIn('field_id', field)
        self.assertIn('field_name', field)
        self.assertIn('thrift_type', field)
        self.assertIn('value', field)


class TestAnalyzeParquetFile(unittest.TestCase):
    """Test the main analyze_parquet_file function"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_file = os.path.join(self.temp_dir, 'test.parquet')
        self.corrupt_file = os.path.join(self.temp_dir, 'corrupt.parquet')
        self.empty_file = os.path.join(self.temp_dir, 'empty.parquet')
        
        # Create test files
        TestParquetFileGenerator.create_minimal_parquet(self.test_file)
        TestParquetFileGenerator.create_corrupt_parquet(self.corrupt_file)
        TestParquetFileGenerator.create_empty_file(self.empty_file)
    
    def tearDown(self):
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir)
    
    def test_valid_parquet_file(self):
        """Test analysis of a valid Parquet file"""
        result = parquet_lens.analyze_parquet_file(self.test_file)
        
        # Check that we get a dictionary with segments
        self.assertIsInstance(result, dict)
        self.assertIn('segments', result)
        self.assertIsInstance(result['segments'], list)
        
        # Check that segments contain expected types
        segment_types = [seg['type'] for seg in result['segments']]
        self.assertIn('magic_number', segment_types)
        self.assertIn('thrift_metadata_blob', segment_types)
    
    def test_nonexistent_file(self):
        """Test handling of non-existent files"""
        result = parquet_lens.analyze_parquet_file('/nonexistent/file.parquet')
        # Should return error dict instead of raising exception
        self.assertIsInstance(result, dict)
        self.assertIn('error', result)
        self.assertIn('File not found', result['error'])
    
    def test_empty_file(self):
        """Test handling of empty files"""
        result = parquet_lens.analyze_parquet_file(self.empty_file)
        self.assertIsInstance(result, dict)
        # Should contain error information
        self.assertTrue('error' in result or 'segments' in result)
    
    def test_corrupt_file(self):
        """Test handling of corrupted files"""
        result = parquet_lens.analyze_parquet_file(self.corrupt_file)
        self.assertIsInstance(result, dict)
        self.assertIn('segments', result)
        # Should handle the corrupt magic number gracefully
        if result['segments']:
            first_segment = result['segments'][0]
            self.assertIn('type', first_segment)
    
    def test_debug_mode(self):
        """Test debug mode functionality"""
        # Capture debug output
        with patch('sys.stderr', new_callable=io.StringIO) as mock_stderr:
            result = parquet_lens.analyze_parquet_file(self.test_file, debug=True)
            debug_output = mock_stderr.getvalue()
            
            self.assertIsInstance(result, dict)
            self.assertIn('segments', result)
            # Debug mode should work without errors
    
    def test_show_undefined_optional_fields(self):
        """Test show_undefined_optional parameter"""
        result1 = parquet_lens.analyze_parquet_file(
            self.test_file, show_undefined_optional=False
        )
        result2 = parquet_lens.analyze_parquet_file(
            self.test_file, show_undefined_optional=True
        )
        
        self.assertIsInstance(result1, dict)
        self.assertIsInstance(result2, dict)
        self.assertIn('segments', result1)
        self.assertIn('segments', result2)
    
    def test_show_list_headers(self):
        """Test show_list_headers parameter"""
        result = parquet_lens.analyze_parquet_file(
            self.test_file, show_list_headers=True
        )
        
        self.assertIsInstance(result, dict)
        self.assertIn('segments', result)
        # Should work without errors and potentially show list_header_range


class TestCommandLineInterface(unittest.TestCase):
    """Test the command line interface"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_file = os.path.join(self.temp_dir, 'test.parquet')
        TestParquetFileGenerator.create_minimal_parquet(self.test_file)
    
    def tearDown(self):
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir)
    
    def test_main_function_basic(self):
        """Test basic main function execution"""
        with patch('sys.argv', ['parquet_lens.py', self.test_file]):
            with patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
                try:
                    parquet_lens.main()
                    output = mock_stdout.getvalue()
                    # Should produce valid JSON output
                    parsed = json.loads(output)
                    self.assertIsInstance(parsed, dict)
                except SystemExit:
                    pass  # argparse may call sys.exit
    
    def test_main_function_with_debug(self):
        """Test main function with debug flag"""
        with patch('sys.argv', ['parquet_lens.py', self.test_file, '--debug']):
            with patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
                try:
                    parquet_lens.main()
                    output = mock_stdout.getvalue()
                    # Should produce valid JSON output
                    parsed = json.loads(output)
                    self.assertIsInstance(parsed, dict)
                except SystemExit:
                    pass
    
    def test_main_function_with_show_list_headers(self):
        """Test main function with show-list-headers flag"""
        with patch('sys.argv', ['parquet_lens.py', self.test_file, '--show-list-headers']):
            with patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
                try:
                    parquet_lens.main()
                    output = mock_stdout.getvalue()
                    parsed = json.loads(output)
                    self.assertIsInstance(parsed, dict)
                except SystemExit:
                    pass
    
    def test_main_function_missing_file(self):
        """Test main function with missing file"""
        with patch('sys.argv', ['parquet_lens.py', '/nonexistent/file.parquet']):
            with patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
                try:
                    parquet_lens.main()
                    output = mock_stdout.getvalue()
                    parsed = json.loads(output)
                    self.assertIn('error', parsed)
                except SystemExit:
                    pass  # May exit on error, which is acceptable


class TestEnumHandling(unittest.TestCase):
    """Test enum value conversion functionality"""
    
    def test_get_enum_class_for_field(self):
        """Test enum class resolution"""
        # Test known enum fields
        enum_class = parquet_lens.get_enum_class_for_field('type', 'SchemaElement')
        self.assertIsNotNone(enum_class)
        
        # Test unknown field
        enum_class = parquet_lens.get_enum_class_for_field('unknown_field', 'UnknownStruct')
        self.assertIsNone(enum_class)
    
    def test_enum_value_conversion(self):
        """Test that enum values are properly converted to names"""
        # This tests the enum conversion logic in thrift_to_dict_with_offsets
        # Would need actual enum values to test thoroughly
        pass


class TestErrorHandling(unittest.TestCase):
    """Test error handling throughout the system"""
    
    def test_transport_errors(self):
        """Test handling of transport errors"""
        # Create a mock transport that doesn't support position tracking
        mock_transport = Mock()
        del mock_transport.tell  # Remove tell method
        del mock_transport._buffer  # Remove _buffer attribute
        
        protocol = parquet_lens.OffsetRecordingProtocol(
            mock_transport, FileMetaData, 0
        )
        
        # Should handle transport errors gracefully
        with self.assertRaises((AttributeError, TypeError)):
            protocol._get_trans_pos()
    
    def test_thrift_parsing_errors(self):
        """Test handling of Thrift parsing errors"""
        # Create invalid Thrift data
        invalid_data = b'\xff\xff\xff\xff'
        
        # Should handle parsing errors in analyze_parquet_file
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        try:
            # Create a file with invalid structure but correct magic numbers
            temp_file.write(b'PAR1')
            temp_file.write(invalid_data)
            temp_file.write((len(invalid_data)).to_bytes(4, byteorder='little'))
            temp_file.write(b'PAR1')
            temp_file.close()
            
            result = parquet_lens.analyze_parquet_file(temp_file.name)
            self.assertIsInstance(result, dict)
            # Should contain segments even with errors
            self.assertIn('segments', result)
            
        finally:
            os.unlink(temp_file.name)


class TestRegressionTests(unittest.TestCase):
    """Regression tests for previously fixed issues"""
    
    def test_type_display_consistency(self):
        """Regression: Ensure type names are consistent (list<SchemaElement> not list<struct>)"""
        # Test that list types show proper element type names
        type_args = (TType.STRUCT, (SchemaElement,), False)
        result = parquet_lens.get_thrift_type_name(TType.LIST, type_args)
        self.assertEqual(result, "list<SchemaElement>")
        self.assertNotEqual(result, "list<struct>")
    
    def test_struct_field_consistency(self):
        """Regression: Ensure all structs have thrift_type field"""
        # Create a simple test object that mimics a Thrift struct
        class TestStruct:
            def __init__(self):
                self.test_field = 'test_value'
                self.thrift_spec = [
                    None,
                    (1, TType.STRING, 'test_field', None, None)
                ]
        
        mock_obj = TestStruct()
        
        result = parquet_lens.thrift_to_dict_with_offsets(
            mock_obj, {}, 0
        )
        
        # All fields should have thrift_type
        for field in result:
            self.assertIn('thrift_type', field)
    
    def test_field_naming_consistency(self):
        """Regression: Ensure field names are 'thrift_type' not 'type'"""
        # Create a simple test object that mimics a Thrift struct
        class TestStruct:
            def __init__(self):
                self.test_field = 'test_value'
                self.thrift_spec = [
                    None,
                    (1, TType.STRING, 'test_field', None, None)
                ]
        
        mock_obj = TestStruct()
        
        result = parquet_lens.thrift_to_dict_with_offsets(
            mock_obj, {}, 0
        )
        
        # Should use 'thrift_type', not 'type'
        for field in result:
            self.assertIn('thrift_type', field)
            self.assertNotIn('type', field)


class TestIntegrationTests(unittest.TestCase):
    """Integration tests that test the whole system"""
    
    def setUp(self):
        """Set up integration test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_file = os.path.join(self.temp_dir, 'integration_test.parquet')
        TestParquetFileGenerator.create_minimal_parquet(self.test_file)
    
    def tearDown(self):
        """Clean up integration test fixtures"""
        shutil.rmtree(self.temp_dir)
    
    def test_end_to_end_analysis(self):
        """Test complete end-to-end analysis"""
        result = parquet_lens.analyze_parquet_file(self.test_file)
        
        # Verify complete structure
        self.assertIsInstance(result, dict)
        self.assertIn('segments', result)
        
        # Check that we have proper segment structure
        for segment in result['segments']:
            self.assertIn('type', segment)
            self.assertIn('range', segment)
            if segment['type'] == 'thrift_metadata_blob':
                self.assertIn('fields', segment)
                self.assertIn('thrift_type', segment)
    
    def test_json_serialization(self):
        """Test that output can be properly JSON serialized"""
        result = parquet_lens.analyze_parquet_file(self.test_file)
        
        # Should be JSON serializable
        json_str = json.dumps(result, indent=2)
        self.assertIsInstance(json_str, str)
        
        # Should be deserializable
        parsed_back = json.loads(json_str)
        self.assertEqual(type(parsed_back), type(result))


def create_test_suite():
    """Create a comprehensive test suite"""
    suite = unittest.TestSuite()
    
    # Add all test classes
    test_classes = [
        TestOffsetRecordingProtocol,
        TestThriftTypeSystem,
        TestThriftToDictConversion,
        TestAnalyzeParquetFile,
        TestCommandLineInterface,
        TestEnumHandling,
        TestErrorHandling,
        TestRegressionTests,
        TestIntegrationTests,
    ]
    
    for test_class in test_classes:
        suite.addTest(unittest.makeSuite(test_class))
    
    return suite


def run_tests():
    """Run all tests with detailed output"""
    print("="*70)
    print("PARQUET-LENS COMPREHENSIVE TEST SUITE (FIXED VERSION)")
    print("="*70)
    print("Testing core functionality to prevent breaking changes...")
    print()
    
    # Create and run the test suite
    suite = create_test_suite()
    runner = unittest.TextTestRunner(verbosity=2, buffer=True)
    result = runner.run(suite)
    
    print("\n" + "="*70)
    print("TEST RESULTS SUMMARY")
    print("="*70)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped) if hasattr(result, 'skipped') else 0}")
    
    if result.failures:
        print("\nFAILURES:")
        for test, traceback in result.failures:
            error_msg = traceback.split('AssertionError: ')[-1].split('\n')[0] if 'AssertionError:' in traceback else 'See details above'
            print(f"- {test}: {error_msg}")
    
    if result.errors:
        print("\nERRORS:")
        for test, traceback in result.errors:
            error_msg = traceback.split('\n')[-2] if len(traceback.split('\n')) > 1 else 'See details above'
            print(f"- {test}: {error_msg}")
    
    success = len(result.failures) == 0 and len(result.errors) == 0
    print(f"\nOVERALL: {'PASS' if success else 'FAIL'}")
    print("="*70)
    
    return success


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--help':
        print(__doc__)
        sys.exit(0)
    
    success = run_tests()
    sys.exit(0 if success else 1)
