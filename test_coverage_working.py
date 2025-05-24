#!/usr/bin/env python3
"""
Working Coverage Expansion Tests for parquet-lens

This file contains additional tests to increase coverage to 80%+, focusing on
uncovered code paths and edge cases.
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
from parquet.ttypes import Type, CompressionCodec, ConvertedType, FieldRepetitionType, PageType, Encoding


class TestTransportEdgeCases(unittest.TestCase):
    """Test transport error conditions and edge cases"""
    
    def test_transport_without_tell_and_buffer(self):
        """Test transport that doesn't have tell() or _buffer"""
        mock_transport = Mock()
        # Remove both tell and _buffer
        del mock_transport.tell
        del mock_transport._buffer
        
        protocol = parquet_lens.OffsetRecordingProtocol(
            mock_transport, FileMetaData, 0
        )
        
        with self.assertRaises(AttributeError):
            protocol._get_trans_pos()
    
    def test_transport_with_tell_fallback(self):
        """Test transport that uses tell() as fallback"""
        mock_transport = Mock()
        mock_transport.tell.return_value = 42
        # Remove _buffer to force tell() usage
        del mock_transport._buffer
        
        protocol = parquet_lens.OffsetRecordingProtocol(
            mock_transport, FileMetaData, 0
        )
        
        pos = protocol._get_trans_pos()
        self.assertEqual(pos, 42)


class TestByteValueHandling(unittest.TestCase):
    """Test byte value processing and encoding edge cases"""
    
    def test_valid_utf8_bytes(self):
        """Test UTF-8 decoding of valid byte values"""
        test_bytes = "Hello, 世界".encode('utf-8')
        result = parquet_lens.thrift_to_dict_with_offsets(test_bytes, {}, 0)
        self.assertEqual(result, "Hello, 世界")
    
    def test_invalid_utf8_bytes(self):
        """Test hex encoding of invalid UTF-8 bytes"""
        invalid_bytes = b'\x80\x81\x82\x83'
        result = parquet_lens.thrift_to_dict_with_offsets(invalid_bytes, {}, 0)
        self.assertEqual(result, "80818283")
    
    def test_mixed_bytes(self):
        """Test handling of bytes with mixed valid/invalid UTF-8"""
        mixed_bytes = b'Hello\x80\x81World'
        result = parquet_lens.thrift_to_dict_with_offsets(mixed_bytes, {}, 0)
        # Should fall back to hex encoding for safety
        self.assertEqual(result, mixed_bytes.hex())


class TestEnumExpansion(unittest.TestCase):
    """Test expanded enum handling scenarios"""
    
    def test_enum_field_mappings(self):
        """Test various enum field mappings"""
        # Test Type enum
        enum_class = parquet_lens.get_enum_class_for_field('type', 'ColumnMetaData')
        self.assertEqual(enum_class, Type)
        
        # Test CompressionCodec enum
        enum_class = parquet_lens.get_enum_class_for_field('codec', 'ColumnMetaData')
        self.assertEqual(enum_class, CompressionCodec)
        
        # Test ConvertedType enum
        enum_class = parquet_lens.get_enum_class_for_field('converted_type', 'SchemaElement')
        self.assertEqual(enum_class, ConvertedType)
        
        # Test FieldRepetitionType enum
        enum_class = parquet_lens.get_enum_class_for_field('repetition_type', 'SchemaElement')
        self.assertEqual(enum_class, FieldRepetitionType)
        
        # Test list field enum
        enum_class = parquet_lens.get_enum_class_for_field('encodings', 'ColumnMetaData')
        self.assertEqual(enum_class, Encoding)
        
        # Test unknown field
        enum_class = parquet_lens.get_enum_class_for_field('unknown_field', 'UnknownStruct')
        self.assertIsNone(enum_class)


class TestTypeNameResolution(unittest.TestCase):
    """Test complex type name resolution edge cases"""
    
    def test_list_type_edge_cases(self):
        """Test list type name resolution edge cases"""
        # Test with None type_args
        result = parquet_lens.get_thrift_type_name(TType.LIST, None)
        self.assertEqual(result, "list")
        
        # Test with empty tuple
        result = parquet_lens.get_thrift_type_name(TType.LIST, ())
        self.assertEqual(result, "list")
        
        # Test with nested tuple format
        type_args = ((TType.STRING,),)
        result = parquet_lens.get_thrift_type_name(TType.LIST, type_args)
        self.assertIn("list<", result)
    
    def test_struct_type_edge_cases(self):
        """Test struct type name resolution edge cases"""
        # Test with None type_args
        result = parquet_lens.get_thrift_type_name(TType.STRUCT, None)
        self.assertEqual(result, "struct")
        
        # Test with empty tuple
        result = parquet_lens.get_thrift_type_name(TType.STRUCT, ())
        self.assertEqual(result, "struct")
        
        # Test with normal class
        class TestClass:
            pass
        
        result = parquet_lens.get_thrift_type_name(TType.STRUCT, (TestClass,))
        self.assertEqual(result, "TestClass")


class TestStructWithoutSpec(unittest.TestCase):
    """Test handling of objects without thrift_spec"""
    
    def test_object_without_thrift_spec(self):
        """Test fallback handling for objects without thrift_spec"""
        class SimpleObject:
            def __init__(self):
                self.field1 = "value1"
                self.field2 = 42
                self._private_field = "should_be_ignored"
        
        obj = SimpleObject()
        result = parquet_lens.thrift_to_dict_with_offsets(obj, {}, 0)
        
        # Should return dict format for objects without thrift_spec
        self.assertIsInstance(result, dict)
        self.assertIn('field1', result)
        self.assertIn('field2', result)
        self.assertNotIn('_private_field', result)  # Private fields ignored
        self.assertEqual(result['field1'], "value1")
        self.assertEqual(result['field2'], 42)


class TestListProcessingEdgeCases(unittest.TestCase):
    """Test complex list processing scenarios"""
    
    def test_list_with_enum_elements(self):
        """Test list processing with enum elements"""
        class MockObjectWithEnumList:
            def __init__(self):
                self.encodings = [0, 1, 2]  # List of Encoding enum values
                self.thrift_spec = [
                    None,
                    (1, TType.LIST, 'encodings', (TType.I32, None, False), None),
                ]
        
        mock_obj = MockObjectWithEnumList()
        result = parquet_lens.thrift_to_dict_with_offsets(mock_obj, {}, 0)
        
        # Find the encodings field
        encodings_field = next((f for f in result if f['field_name'] == 'encodings'), None)
        self.assertIsNotNone(encodings_field)
        self.assertIsInstance(encodings_field['value'], list)
    
    def test_list_with_struct_elements(self):
        """Test list processing with struct elements"""
        class MockStruct:
            def __init__(self, value):
                self.test_field = value
                self.thrift_spec = [
                    None,
                    (1, TType.STRING, 'test_field', None, None),
                ]
        
        class MockObjectWithStructList:
            def __init__(self):
                self.struct_list = [MockStruct("item1"), MockStruct("item2")]
                self.thrift_spec = [
                    None,
                    (1, TType.LIST, 'struct_list', (TType.STRUCT, (MockStruct,), False), None),
                ]
        
        mock_obj = MockObjectWithStructList()
        result = parquet_lens.thrift_to_dict_with_offsets(mock_obj, {}, 0)
        
        # Find the struct_list field
        struct_list_field = next((f for f in result if f['field_name'] == 'struct_list'), None)
        self.assertIsNotNone(struct_list_field)
        self.assertIsInstance(struct_list_field['value'], list)


class TestFileAnalysisErrors(unittest.TestCase):
    """Test file analysis error conditions"""
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        shutil.rmtree(self.temp_dir)
    
    def test_file_wrong_footer_magic(self):
        """Test file with wrong footer magic"""
        test_file = os.path.join(self.temp_dir, 'wrong_footer.parquet')
        with open(test_file, 'wb') as f:
            f.write(b'PAR1')  # Correct header
            f.write(b'\x00' * 50)  # Dummy data
            f.write((50).to_bytes(4, byteorder='little'))  # Footer length
            f.write(b'XXXX')  # Wrong footer magic
        
        result = parquet_lens.analyze_parquet_file(test_file)
        
        self.assertIsInstance(result, dict)
        self.assertIn('segments', result)
        # Should handle the error gracefully
        error_segments = [s for s in result['segments'] if s['type'] == 'error']
        self.assertTrue(len(error_segments) > 0)
    
    def test_file_too_short(self):
        """Test file that's too short for footer"""
        test_file = os.path.join(self.temp_dir, 'too_short.parquet')
        with open(test_file, 'wb') as f:
            f.write(b'PAR1')  # Only header
        
        result = parquet_lens.analyze_parquet_file(test_file)
        
        self.assertIsInstance(result, dict)
        self.assertIn('segments', result)
    
    def test_corrupt_metadata(self):
        """Test file with corrupt metadata"""
        test_file = os.path.join(self.temp_dir, 'corrupt_metadata.parquet')
        with open(test_file, 'wb') as f:
            f.write(b'PAR1')  # Header
            f.write(b'\xff' * 10)  # Invalid metadata
            f.write((10).to_bytes(4, byteorder='little'))  # Footer length
            f.write(b'PAR1')  # Footer
        
        result = parquet_lens.analyze_parquet_file(test_file)
        
        self.assertIsInstance(result, dict)
        self.assertIn('segments', result)


class TestFieldSpecMethods(unittest.TestCase):
    """Test field specification resolution methods"""
    
    def test_field_spec_resolution(self):
        """Test _get_field_spec_from_id method"""
        test_data = b'\x19\x4c\x15\x00\x15\x06\x15\x10\x00\x00\x00\x00'
        transport = TTransport.TMemoryBuffer(test_data)
        protocol = parquet_lens.OffsetRecordingProtocol(transport, FileMetaData, 0)
        
        # Create a test spec
        spec = [
            None,
            (1, TType.I32, 'version', None, None),
            (2, TType.LIST, 'schema', (TType.STRUCT, (SchemaElement,), False), None),
        ]
        
        # Test existing field ID
        field_spec = protocol._get_field_spec_from_id(1, spec)
        self.assertIsNotNone(field_spec)
        self.assertEqual(field_spec[0], 1)
        self.assertEqual(field_spec[2], 'version')
        
        # Test non-existing field ID
        field_spec = protocol._get_field_spec_from_id(999, spec)
        self.assertIsNone(field_spec)
        
        # Test with None spec
        field_spec = protocol._get_field_spec_from_id(1, None)
        self.assertIsNone(field_spec)


class TestOffsetTrackingEdgeCases(unittest.TestCase):
    """Test edge cases in offset tracking"""
    
    def test_nested_offset_calculation(self):
        """Test offset calculation with nested structures"""
        # Create nested struct
        class InnerStruct:
            def __init__(self):
                self.inner_field = "inner_value"
                self.thrift_spec = [
                    None,
                    (1, TType.STRING, 'inner_field', None, None),
                ]
        
        class OuterStruct:
            def __init__(self):
                self.outer_field = "outer_value"
                self.nested_struct = InnerStruct()
                self.thrift_spec = [
                    None,
                    (1, TType.STRING, 'outer_field', None, None),
                    (2, TType.STRUCT, 'nested_struct', (InnerStruct,), None),
                ]
        
        # Create field details with offset information
        field_details_map = {
            "0_outer_field_1": {
                'field_header_range_in_blob': [10, 15],
                'value_range_in_blob': [15, 25]
            },
            "0_nested_struct_2": {
                'field_header_range_in_blob': [25, 30],
                'value_range_in_blob': [30, 50]
            },
            "1_inner_field_1": {
                'field_header_range_in_blob': [30, 35],
                'value_range_in_blob': [35, 45]
            }
        }
        
        outer_obj = OuterStruct()
        result = parquet_lens.thrift_to_dict_with_offsets(
            outer_obj, field_details_map, 100, show_undefined_optional=True
        )
        
        # Check that results include proper offset information
        self.assertIsInstance(result, list)
        for field in result:
            if 'field_header_range' in field:
                # Should properly convert blob offsets to file offsets
                self.assertTrue(all(offset >= 100 for offset in field['field_header_range']))


class TestDebugOutput(unittest.TestCase):
    """Test debug output functionality"""
    
    def test_debug_mode_output(self):
        """Test that debug mode produces expected output"""
        # Create a test parquet file
        temp_dir = tempfile.mkdtemp()
        try:
            test_file = os.path.join(temp_dir, 'test_debug.parquet')
            with open(test_file, 'wb') as f:
                f.write(b'PAR1')  # Header
                f.write(b'\x19\x4c\x15\x00\x15\x06\x15\x10\x00\x00\x00\x00')  # Some data
                f.write((12).to_bytes(4, byteorder='little'))  # Footer length
                f.write(b'PAR1')  # Footer
            
            # Capture debug output
            with patch('sys.stderr', new_callable=io.StringIO) as mock_stderr:
                result = parquet_lens.analyze_parquet_file(test_file, debug=True)
                debug_output = mock_stderr.getvalue()
                
                # Should contain debug information
                self.assertIn("DEBUG:", debug_output)
                
                # Result should still be valid
                self.assertIsInstance(result, dict)
                self.assertIn('segments', result)
        
        finally:
            shutil.rmtree(temp_dir)


if __name__ == '__main__':
    unittest.main(verbosity=2)
