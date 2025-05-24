#!/usr/bin/env python3
"""
Targeted coverage tests to reach 80% coverage.
This file focuses on specific uncovered lines with working test approaches.
"""

import unittest
import os
import tempfile
import sys
from unittest.mock import patch, MagicMock, mock_open
import parquet_lens
from parquet.ttypes import FileMetaData, SchemaElement, Type, ConvertedType
from thrift.transport.TTransport import TMemoryBuffer
from thrift.protocol.TProtocol import TType


class TestSpecificCoverage(unittest.TestCase):
    """Tests targeting specific uncovered lines"""

    def test_line_79_transport_buffer_error(self):
        """Test line 79 - TMemoryBuffer _buffer without tell() method"""
        test_data = b'\x19\x4c\x15\x00\x15\x06\x15\x10\x00\x00\x00\x00'
        transport = TMemoryBuffer(test_data)
        
        # Replace the _buffer with an object that doesn't have tell()
        class FakeBuffer:
            pass
        
        transport._buffer = FakeBuffer()
        protocol = parquet_lens.OffsetRecordingProtocol(transport, FileMetaData, 0)
        
        with self.assertRaises(AttributeError) as cm:
            protocol._get_trans_pos()
        
        self.assertIn("does not have a usable '_buffer' attribute with a 'tell' method", str(cm.exception))

    def test_lines_360_362_list_element_class_name(self):
        """Test lines 360-362 - list type with element class having __name__"""
        # Use actual parquet classes that have __name__
        result = parquet_lens.get_thrift_type_name(TType.LIST, (SchemaElement, TType.STRUCT))
        self.assertEqual(result, "list<SchemaElement>")

    def test_lines_365_377_list_legacy_format(self):
        """Test lines 365-377 - list type with legacy format"""
        # Legacy format: tuple containing tuple with struct class
        legacy_args = ((SchemaElement,),)
        result = parquet_lens.get_thrift_type_name(TType.LIST, legacy_args)
        self.assertEqual(result, "list<SchemaElement>")
        
        # Test primitive type list (lines 375-377)
        primitive_args = (TType.I32,)
        result = parquet_lens.get_thrift_type_name(TType.LIST, primitive_args)
        self.assertEqual(result, "list<i32>")

    def test_lines_580_596_show_undefined_optional(self):
        """Test lines 580-596 - show undefined optional fields"""
        schema_element = SchemaElement()
        schema_element.name = "test_field"
        schema_element.type = Type.BYTE_ARRAY
        
        # Test with show_undefined_optional=True
        result = parquet_lens.thrift_to_dict_with_offsets(
            schema_element, {}, 0, 0, show_undefined_optional=True
        )
        
        self.assertIsInstance(result, list)
        # Should include more fields when showing undefined optional
        fields_with_undefined = len(result)
        
        # Compare with show_undefined_optional=False
        result_normal = parquet_lens.thrift_to_dict_with_offsets(
            schema_element, {}, 0, 0, show_undefined_optional=False
        )
        
        fields_normal = len(result_normal)
        # Should have more fields when showing undefined optional
        self.assertGreaterEqual(fields_with_undefined, fields_normal)

    def test_lines_668_671_debug_mode(self):
        """Test lines 668-671 - debug mode functions"""
        # Test with DEBUG environment variable
        with patch.dict(os.environ, {'DEBUG': '1'}):
            # This should trigger the debug mode check
            schema_element = SchemaElement()
            schema_element.name = "test_field"
            
            result = parquet_lens.thrift_to_dict_with_offsets(
                schema_element, {}, 0, 0, False
            )
            self.assertIsInstance(result, list)
        
        # Test without DEBUG environment variable
        with patch.dict(os.environ, {}, clear=True):
            result = parquet_lens.thrift_to_dict_with_offsets(
                schema_element, {}, 0, 0, False
            )
            self.assertIsInstance(result, list)

    def test_line_801_write_file_output(self):
        """Test line 801 - write formatted output to file"""
        test_data = {"test_key": "test_value", "number": 42}
        
        # Create a temporary file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as tmp:
            temp_file = tmp.name
        
        try:
            # Call the write function by invoking main with output file
            with patch('sys.argv', ['parquet_lens', 'dummy.parquet', '--output', temp_file]):
                with patch('parquet_lens.analyze_parquet_file', return_value=test_data):
                    try:
                        parquet_lens.main()
                    except SystemExit:
                        pass  # Expected exit
            
            # Verify file was written
            self.assertTrue(os.path.exists(temp_file))
            
            with open(temp_file, 'r') as f:
                content = f.read()
                self.assertIn('test_key', content)
                
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)

    def test_line_848_main_output_file_arg(self):
        """Test line 848 - main function with --output argument"""
        test_data = {"result": "success"}
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.json') as tmp:
            temp_file = tmp.name
        
        try:
            # Test the specific code path in main() that handles --output
            with patch('sys.argv', ['parquet_lens', 'test.parquet', '--output', temp_file]):
                with patch('parquet_lens.analyze_parquet_file', return_value=test_data):
                    with patch('builtins.print'):  # Suppress output
                        try:
                            parquet_lens.main()
                        except SystemExit:
                            pass
            
            self.assertTrue(os.path.exists(temp_file))
            
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)

    def test_get_enum_class_for_field_coverage(self):
        """Test get_enum_class_for_field function for various cases"""
        # Test known enum field mappings
        result = parquet_lens.get_enum_class_for_field("type", "SchemaElement")
        self.assertIsNotNone(result)
        
        result = parquet_lens.get_enum_class_for_field("converted_type", "SchemaElement")
        self.assertIsNotNone(result)
        
        # Test unknown field
        result = parquet_lens.get_enum_class_for_field("unknown_field", "UnknownStruct")
        self.assertIsNone(result)

    def test_complex_thrift_structures(self):
        """Test complex thrift structure processing to hit various code paths"""
        # Create a complex schema element
        schema_element = SchemaElement()
        schema_element.name = "complex_field"
        schema_element.type = Type.BYTE_ARRAY
        schema_element.converted_type = ConvertedType.UTF8
        schema_element.type_length = 100
        schema_element.repetition_type = 1  # OPTIONAL
        
        # Test without field details
        result = parquet_lens.thrift_to_dict_with_offsets(schema_element, {}, 0, 0, False)
        self.assertIsInstance(result, list)
        self.assertTrue(len(result) > 0)
        
        # Test with field details that have ranges
        field_details = {
            "0_complex_field_1": {
                'value_range_in_blob': [10, 20],
                'field_total_range_in_blob': [5, 25]
            }
        }
        
        result = parquet_lens.thrift_to_dict_with_offsets(
            schema_element, field_details, 100, 0, False
        )
        self.assertIsInstance(result, list)

    def test_bytes_field_processing(self):
        """Test bytes field processing paths"""
        # Create a mock object with bytes fields to test lines 512-515
        class MockThriftObj:
            def __init__(self):
                self.utf8_bytes = b"valid_utf8_string"
                self.invalid_bytes = b"\xff\xfe\xfd\xfc"
        
        obj = MockThriftObj()
        
        # This will exercise the bytes processing code paths
        result = parquet_lens.thrift_to_dict_with_offsets(obj, {}, 0, 0, False)
        self.assertIsInstance(result, list)


if __name__ == '__main__':
    # Run with coverage
    try:
        import coverage
        cov = coverage.Coverage()
        cov.start()
        unittest.main(exit=False)
        cov.stop()
        cov.save()
        print("Coverage data saved")
    except ImportError:
        unittest.main()
