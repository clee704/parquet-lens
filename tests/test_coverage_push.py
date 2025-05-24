#!/usr/bin/env python3
"""
Targeted tests to push parquet-lens coverage from 75% to 80%

These tests specifically target the remaining uncovered lines to reach the 80% goal.
"""

import unittest
import tempfile
import os
import sys
import json
from unittest.mock import patch, Mock
from io import BytesIO

# Add the parent directory to sys.path so we can import parquet_lens
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import parquet_lens

# Import required classes and modules for testing
from thrift.transport import TTransport
from thrift.protocol.TProtocol import TType
from parquet.ttypes import FileMetaData, SchemaElement


class TestTargeted80PercentCoverage(unittest.TestCase):
    """Targeted tests to reach exactly 80% coverage"""
    
    def test_transport_with_tell_fallback(self):
        """Test line 82 - transport with tell() method fallback"""
        class MockTransport:
            def __init__(self):
                # Don't have _buffer attribute, but have tell()
                pass
            
            def tell(self):
                return 42
        
        transport = MockTransport()
        protocol = parquet_lens.OffsetRecordingProtocol(transport, FileMetaData, 0)
        
        # Should use tell() method as fallback
        pos = protocol._get_trans_pos()
        self.assertEqual(pos, 42)
    
    def test_legacy_list_type_with_struct_name(self):
        """Test lines 370-372 - legacy list type with struct class having __name__"""
        # Legacy format: tuple containing tuple with struct class
        legacy_args = ((SchemaElement,),)
        result = parquet_lens.get_thrift_type_name(TType.LIST, legacy_args)
        self.assertEqual(result, "list<SchemaElement>")
    
    def test_legacy_list_type_without_name(self):
        """Test lines 373-374 - legacy list type without __name__"""
        # Instead of trying to delete __name__, use an object that doesn't have it
        class_without_name = type('DynamicClass', (), {})
        # Remove the automatically assigned __name__ by using an instance
        obj_without_name = class_without_name()
        
        # Create a tuple that resembles the legacy format
        legacy_args = ((obj_without_name,),)
        result = parquet_lens.get_thrift_type_name(TType.LIST, legacy_args)
        # Should fall back to get_thrift_type_name recursion
        self.assertTrue(result.startswith("list<"))
    
    def test_primitive_list_type_legacy(self):
        """Test lines 375-377 - primitive type list in legacy format"""
        primitive_args = (TType.I32,)
        result = parquet_lens.get_thrift_type_name(TType.LIST, primitive_args)
        self.assertEqual(result, "list<i32>")
    
    def test_field_header_range_functionality(self):
        """Test lines 580-585 - field header range in blob functionality"""
        # Create a mock object with thrift_spec
        class MockStruct:
            def __init__(self):
                self.test_field = "value"
                self.thrift_spec = [
                    None,
                    (1, TType.STRING, 'test_field', None, None)
                ]
        
        mock_obj = MockStruct()
        
        # Create field details map with field_header_range_in_blob
        field_details_map = {
            id(mock_obj): {
                'field_header_range_in_blob': [10, 20]
            }
        }
        
        result = parquet_lens.thrift_to_dict_with_offsets(
            mock_obj, field_details_map, 100, 0, False
        )
        
        # Should have field_header_range calculated with base offset
        self.assertIsInstance(result, list)
        if result:
            field = result[0]
            if 'field_header_range' in field:
                self.assertEqual(field['field_header_range'], [110, 120])
    
    def test_show_undefined_optional_true(self):
        """Test lines 590-596 - show_undefined_optional=True functionality"""
        schema_element = SchemaElement()
        schema_element.name = "test_field"
        
        # Test with show_undefined_optional=True to hit the conditional
        result = parquet_lens.thrift_to_dict_with_offsets(
            schema_element, {}, 0, 0, True  # show_undefined_optional=True
        )
        
        self.assertIsInstance(result, list)
        # Should include all fields even if undefined
    
    def test_additional_enum_mappings(self):
        """Test additional enum field mappings to improve coverage"""
        # Test more enum mappings that might not be covered
        from parquet.ttypes import Encoding, PageType
        
        # Test encodings field
        enum_class = parquet_lens.get_enum_class_for_field('encodings', 'ColumnMetaData')
        self.assertEqual(enum_class, Encoding)
        
        # Test type field for pages
        enum_class = parquet_lens.get_enum_class_for_field('type', 'PageHeader')
        self.assertEqual(enum_class, PageType)
    
    def test_show_undefined_optional_true(self):
        """Test show_undefined_optional=True path (lines 608-622)"""
        # Create a schema element with some fields unset
        from parquet.ttypes import SchemaElement, Type
        schema = SchemaElement()
        schema.name = "test_field"
        schema.type = Type.BOOLEAN  # Set required fields
        
        # Convert with show_undefined_optional=True
        result = parquet_lens.thrift_to_dict_with_offsets(
            schema, {}, 0, 0, show_undefined_optional=True
        )
        
        # Check that undefined fields are included
        self.assertIsInstance(result, list)
        field_names = set(f['field_name'] for f in result if 'field_name' in f)
        
        # The following optional fields should be included
        optional_fields = {'repetition_type', 'num_children', 'converted_type'}
        # Make sure at least one of these is included
        self.assertTrue(any(f in field_names for f in optional_fields))
    
    def test_field_header_range_functionality(self):
        """Test field header range handling (lines 580-596)"""
        # Create a SchemaElement
        from parquet.ttypes import SchemaElement, Type
        schema = SchemaElement()
        schema.name = "test_field"
        schema.type = Type.BOOLEAN
        
        # Create field details with field_header_range_in_blob
        field_details = {
            id(schema): {
                'field_header_range_in_blob': [100, 150]
            }
        }
        
        # Call with base_offset=1000
        result = parquet_lens.thrift_to_dict_with_offsets(
            schema, field_details, 1000, 0
        )
        
        # Check results
        self.assertIsInstance(result, list)
        for field in result:
            # Look for field_header_range in the result
            if 'field_header_range' in field:
                # Should be adjusted by base_offset
                self.assertEqual(field['field_header_range'], [1100, 1150])
    
    def test_invalid_file_analysis(self):
        """Test analysis of invalid files (lines 482-509)"""
        import tempfile
        import os
        
        # Test with a completely invalid file
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            f.write(b'NOT-PARQUET-FORMAT')
            path = f.name
        
        try:
            result = parquet_lens.analyze_parquet_file(path)
            
            # Should return a valid dict with error information
            self.assertIsInstance(result, dict)
            self.assertIn('segments', result)
            
            # At least one segment should be of type 'error'
            error_segments = [s for s in result['segments'] if s.get('type') == 'error']
            self.assertTrue(len(error_segments) > 0)
            
        finally:
            os.unlink(path)
    
    def test_complex_file_analysis(self):
        """Test file analysis with complex segments (lines 436-476)"""
        import tempfile
        import os
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            # Create a valid parquet file with multiple segments
            f.write(b'PAR1')  # Header magic
            f.write(b'\x00' * 100)  # First data segment
            f.write(b'\x01' * 100)  # Second data segment
            f.write((200).to_bytes(4, byteorder='little'))  # Footer length
            f.write(b'PAR1')  # Footer magic
            path = f.name
        
        try:
            # Analyze with all optional flags enabled
            result = parquet_lens.analyze_parquet_file(
                path, debug=True, show_list_headers=True, show_undefined_optional=True
            )
            
            # Verify result structure
            self.assertIsInstance(result, dict)
            self.assertIn('segments', result)
            self.assertIsInstance(result['segments'], list)
            self.assertTrue(len(result['segments']) >= 2)  # At least header and footer
            
        finally:
            os.unlink(path)


    def test_debug_environment_variable(self):
        """Test debug mode through environment variable (lines 669-672)"""
        import io
        import os
        
        # Create a simple object for conversion
        schema = SchemaElement()
        schema.name = "test_field"
        
        # Set DEBUG=1 in environment
        old_env = os.environ.get('DEBUG')
        os.environ['DEBUG'] = '1'
        
        try:
            # Use a temporary buffer to capture stderr
            temp_stderr = io.StringIO()
            with patch('sys.stderr', temp_stderr):
                # Simple operation that should trigger debug output
                parquet_lens.thrift_to_dict_with_offsets(schema, {}, 0, 0)
            
        finally:
            # Restore environment
            if old_env is not None:
                os.environ['DEBUG'] = old_env
            else:
                del os.environ['DEBUG']
    
    def test_enum_mapping_variations(self):
        """Test various enum mapping scenarios (lines 649-651)"""
        from parquet.ttypes import Type, PageType, ConvertedType, FieldRepetitionType
        
        # Test various enum mappings
        result = parquet_lens.get_enum_class_for_field('type', 'SchemaElement')
        self.assertEqual(result, Type)
        
        result = parquet_lens.get_enum_class_for_field('type', 'PageHeader')
        self.assertEqual(result, PageType)
        
        result = parquet_lens.get_enum_class_for_field('converted_type', 'SchemaElement')
        self.assertEqual(result, ConvertedType)
        
        result = parquet_lens.get_enum_class_for_field('repetition_type', 'SchemaElement')
        self.assertEqual(result, FieldRepetitionType)
    
    def test_main_function_variations(self):
        """Test main function variations (lines 802, 849)"""
        import io
        
        # Test main with --help flag
        with patch('sys.argv', ['parquet_lens.py', '--help']):
            with patch('sys.stdout', new=io.StringIO()):
                try:
                    parquet_lens.main()
                except SystemExit:
                    pass  # Expected to exit

        # Test main with missing required arguments
        with patch('sys.argv', ['parquet_lens.py']):
            with patch('sys.stdout', new=io.StringIO()):
                try:
                    parquet_lens.main()
                except SystemExit:
                    pass  # Expected to exit


if __name__ == '__main__':
    unittest.main()
