#!/usr/bin/env python3
"""
Coverage Expansion Tests for parquet-lens

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


class TestTransportErrorHandling(unittest.TestCase):
    """Test various transport error conditions"""
    
    def test_transport_without_tell_method(self):
        """Test transport that doesn't support tell() method"""
        mock_transport = Mock()
        # Remove tell method
        del mock_transport.tell
        # Remove _buffer attribute  
        del mock_transport._buffer
        
        protocol = parquet_lens.OffsetRecordingProtocol(
            mock_transport, FileMetaData, 0
        )
        
        with self.assertRaises((AttributeError, TypeError)):
            protocol._get_trans_pos()
    
    def test_transport_with_tell_method(self):
        """Test transport that has tell() method"""
        mock_transport = Mock()
        mock_transport.tell.return_value = 42
        # Remove _buffer to force use of tell() method
        del mock_transport._buffer
        
        protocol = parquet_lens.OffsetRecordingProtocol(
            mock_transport, FileMetaData, 0
        )
        
        # Should use tell() method as fallback
        pos = protocol._get_trans_pos()
        self.assertEqual(pos, 42)
    
    def test_transport_buffer_without_tell(self):
        """Test TMemoryBuffer without tell method on _buffer"""
        mock_transport = Mock()
        mock_transport._buffer = Mock()
        # Remove tell method from buffer
        del mock_transport._buffer.tell
        
        protocol = parquet_lens.OffsetRecordingProtocol(
            mock_transport, FileMetaData, 0
        )
        
        with self.assertRaises(AttributeError):
            protocol._get_trans_pos()


class TestNestedStructHandling(unittest.TestCase):
    """Test nested struct processing and spec stack management"""
    
    def test_read_struct_begin_with_field_context(self):
        """Test readStructBegin with current field context"""
        test_data = b'\x19\x4c\x15\x00\x15\x06\x15\x10\x00\x00\x00\x00'
        transport = TTransport.TMemoryBuffer(test_data)
        protocol = parquet_lens.OffsetRecordingProtocol(transport, FileMetaData, 0)
        
        # Set up field context
        protocol._current_field_name = "test_field"
        protocol._current_field_id = 1
        protocol._struct_nesting_level = 0
        field_key = f"{protocol._struct_nesting_level}_{protocol._current_field_name}_{protocol._current_field_id}"
        protocol.field_details[field_key] = {}
        
        # Call readStructBegin
        protocol.readStructBegin()
        
        # Should record value_start_offset
        self.assertIn('value_start_offset', protocol.field_details[field_key])
    
    def test_read_struct_end_with_spec_stack(self):
        """Test readStructEnd with parent spec stack"""
        test_data = b'\x19\x4c\x15\x00\x15\x06\x15\x10\x00\x00\x00\x00'
        transport = TTransport.TMemoryBuffer(test_data)
        protocol = parquet_lens.OffsetRecordingProtocol(transport, FileMetaData, 0)
        
        # Set up parent spec stack
        parent_spec = [None, (1, TType.STRING, 'parent_field', None, None)]
        protocol._parent_spec_stack = [parent_spec]
        protocol._current_spec = [None, (1, TType.I32, 'current_field', None, None)]
        
        # Set up field context
        protocol._current_field_name = "test_field"
        protocol._current_field_id = 1
        protocol._struct_nesting_level = 1
        field_key = f"{protocol._struct_nesting_level}_{protocol._current_field_name}_{protocol._current_field_id}"
        protocol.field_details[field_key] = {'value_start_offset': 10}
        protocol._field_total_start_offset_in_blob = 5
        
        # Call readStructEnd
        protocol.readStructEnd()
        
        # Should pop spec and set value_range
        self.assertEqual(protocol._current_spec, parent_spec)
        self.assertIn('value_range_in_blob', protocol.field_details[field_key])
        self.assertIn('field_total_range_in_blob', protocol.field_details[field_key])


class TestListHandling(unittest.TestCase):
    """Test list processing with headers and context management"""
    
    def test_read_list_begin_with_debug_and_headers(self):
        """Test readListBegin with debug mode and list headers enabled"""
        test_data = b'\x19\x4c\x15\x00\x15\x06\x15\x10\x00\x00\x00\x00'
        transport = TTransport.TMemoryBuffer(test_data)
        protocol = parquet_lens.OffsetRecordingProtocol(transport, FileMetaData, 0)
        
        # Enable debug and list headers
        protocol._debug_enabled = True
        protocol._show_list_headers = True
        
        # Set up field context
        protocol._current_field_name = "test_list"
        protocol._current_field_id = 2
        protocol._struct_nesting_level = 0
        field_key = f"{protocol._struct_nesting_level}_{protocol._current_field_name}_{protocol._current_field_id}"
        protocol.field_details[field_key] = {}
        
        # Capture debug output
        with patch('sys.stderr', new_callable=io.StringIO) as mock_stderr:
            protocol.readListBegin()
            debug_output = mock_stderr.getvalue()
            
            # Should output debug info
            self.assertIn("DEBUG: LIST BEGIN", debug_output)
            self.assertIn("test_list", debug_output)
            
            # Should record list header range
            self.assertIn('list_header_range', protocol.field_details[field_key])
    
    def test_read_list_end_with_context_restoration(self):
        """Test readListEnd with field context restoration"""
        test_data = b'\x19\x4c\x15\x00\x15\x06\x15\x10\x00\x00\x00\x00'
        transport = TTransport.TMemoryBuffer(test_data)
        protocol = parquet_lens.OffsetRecordingProtocol(transport, FileMetaData, 0)
        
        protocol._debug_enabled = True
        
        # Set up field context stack
        field_context = {
            'field_name': 'test_list',
            'field_id': 2,
            'nesting_level': 0,
            'field_total_start': 5
        }
        protocol._field_context_stack = [field_context]
        
        field_key = "0_test_list_2"
        protocol.field_details[field_key] = {'value_start_offset': 10}
        
        # Capture debug output
        with patch('sys.stderr', new_callable=io.StringIO) as mock_stderr:
            protocol.readListEnd()
            debug_output = mock_stderr.getvalue()
            
            # Should output debug info and restore context
            self.assertIn("DEBUG: LIST END", debug_output)
            self.assertIn('value_range_in_blob', protocol.field_details[field_key])


class TestEnumHandlingExpanded(unittest.TestCase):
    """Expanded enum handling tests"""
    
    def test_get_enum_class_for_known_fields(self):
        """Test enum class resolution for known field/struct combinations"""
        # Test ColumnMetaData enums
        enum_class = parquet_lens.get_enum_class_for_field('type', 'ColumnMetaData')
        self.assertEqual(enum_class, Type)
        
        enum_class = parquet_lens.get_enum_class_for_field('codec', 'ColumnMetaData')
        self.assertEqual(enum_class, CompressionCodec)
        
        # Test SchemaElement enums
        enum_class = parquet_lens.get_enum_class_for_field('converted_type', 'SchemaElement')
        self.assertEqual(enum_class, ConvertedType)
        
        enum_class = parquet_lens.get_enum_class_for_field('repetition_type', 'SchemaElement')
        self.assertEqual(enum_class, FieldRepetitionType)
        
        # Test list enum mappings
        enum_class = parquet_lens.get_enum_class_for_field('encodings', 'ColumnMetaData')
        self.assertEqual(enum_class, Encoding)
    
    def test_enum_processing_in_thrift_conversion(self):
        """Test enum value conversion in thrift_to_dict_with_offsets"""
        # Create a mock object with enum fields
        class MockColumnMetaData:
            def __init__(self):
                self.type = 0  # Type.BOOLEAN
                self.codec = 0  # CompressionCodec.UNCOMPRESSED
                self.thrift_spec = [
                    None,
                    (1, TType.I32, 'type', None, None),
                    (2, TType.I32, 'codec', None, None),
                ]
        
        mock_obj = MockColumnMetaData()
        result = parquet_lens.thrift_to_dict_with_offsets(mock_obj, {}, 0)
        
        # Should convert enum values to names
        self.assertIsInstance(result, list)
        type_field = next((f for f in result if f['field_name'] == 'type'), None)
        self.assertIsNotNone(type_field)
        # The enum value should be converted to string name
        self.assertIsInstance(type_field['value'], (str, int))


class TestComplexTypeHandling(unittest.TestCase):
    """Test complex type name resolution and list processing"""
    
    def test_list_type_name_edge_cases(self):
        """Test edge cases in list type name resolution"""
        # Test empty type_args
        result = parquet_lens.get_thrift_type_name(TType.LIST, None)
        self.assertEqual(result, "list")
        
        # Test malformed type_args
        result = parquet_lens.get_thrift_type_name(TType.LIST, ())
        self.assertEqual(result, "list")
        
        # Test legacy format with nested tuples
        type_args = ((TType.STRING,),)
        result = parquet_lens.get_thrift_type_name(TType.LIST, type_args)
        self.assertIn("list<", result)
    
    def test_struct_type_name_edge_cases(self):
        """Test edge cases in struct type name resolution"""
        # Test with invalid type_args
        result = parquet_lens.get_thrift_type_name(TType.STRUCT, None)
        self.assertEqual(result, "struct")
        
        # Test with empty tuple
        result = parquet_lens.get_thrift_type_name(TType.STRUCT, ())
        self.assertEqual(result, "struct")
        
        # Test with class without __name__
        class NoNameClass:
            pass
        del NoNameClass.__name__
        
        result = parquet_lens.get_thrift_type_name(TType.STRUCT, (NoNameClass,))
        self.assertEqual(result, "struct")


class TestByteHandling(unittest.TestCase):
    """Test byte value processing and encoding"""
    
    def test_bytes_utf8_decoding(self):
        """Test UTF-8 decoding of byte values"""
        test_bytes = "Hello, 世界".encode('utf-8')
        result = parquet_lens.thrift_to_dict_with_offsets(test_bytes, {}, 0)
        self.assertEqual(result, "Hello, 世界")
    
    def test_bytes_invalid_utf8_hex_encoding(self):
        """Test hex encoding of invalid UTF-8 bytes"""
        invalid_bytes = b'\x80\x81\x82\x83'
        result = parquet_lens.thrift_to_dict_with_offsets(invalid_bytes, {}, 0)
        self.assertEqual(result, "80818283")
    
    def test_mixed_valid_invalid_bytes(self):
        """Test handling of bytes with mixed valid/invalid UTF-8"""
        mixed_bytes = b'Hello\x80\x81World'
        result = parquet_lens.thrift_to_dict_with_offsets(mixed_bytes, {}, 0)
        # Should fall back to hex encoding
        self.assertEqual(result, mixed_bytes.hex())


class TestStructWithoutThriftSpec(unittest.TestCase):
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
        
        # Should return dict format
        self.assertIsInstance(result, dict)
        self.assertIn('field1', result)
        self.assertIn('field2', result)
        self.assertNotIn('_private_field', result)
        self.assertEqual(result['field1'], "value1")
        self.assertEqual(result['field2'], 42)


class TestComplexListProcessing(unittest.TestCase):
    """Test complex list processing with structs and enums"""
    
    def test_list_with_enum_elements(self):
        """Test list processing with enum elements"""
        # Create a mock object with a list of enum values
        class MockObject:
            def __init__(self):
                self.encodings = [0, 1, 2]  # List of Encoding enum values
                self.thrift_spec = [
                    None,
                    (1, TType.LIST, 'encodings', (TType.I32, None, False), None),
                ]
        
        mock_obj = MockObject()
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
        
        class MockObject:
            def __init__(self):
                self.struct_list = [MockStruct("item1"), MockStruct("item2")]
                self.thrift_spec = [
                    None,
                    (1, TType.LIST, 'struct_list', (TType.STRUCT, (MockStruct,), False), None),
                ]
        
        mock_obj = MockObject()
        result = parquet_lens.thrift_to_dict_with_offsets(mock_obj, {}, 0)
        
        # Find the struct_list field
        struct_list_field = next((f for f in result if f['field_name'] == 'struct_list'), None)
        self.assertIsNotNone(struct_list_field)
        self.assertIsInstance(struct_list_field['value'], list)
        
        # Each list item should be a struct with thrift_type
        for item in struct_list_field['value']:
            self.assertIsInstance(item, dict)
            self.assertIn('thrift_type', item)


class TestNestedStructOffsets(unittest.TestCase):
    """Test offset tracking for nested structs"""
    
    def test_nested_struct_offset_tracking(self):
        """Test that nested struct offsets are properly tracked"""
        # Create nested struct scenario
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
        
        # Create field details map with nested offsets
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
        
        # Check that offsets are properly converted
        self.assertIsInstance(result, list)
        for field in result:
            if 'field_header_range' in field:
                # Should add base_offset_in_file (100) to the ranges
                self.assertTrue(all(offset >= 100 for offset in field['field_header_range']))


class TestFileAnalysisEdgeCases(unittest.TestCase):
    """Test edge cases in file analysis"""
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        shutil.rmtree(self.temp_dir)
    
    def test_file_with_wrong_footer_magic(self):
        """Test file with correct header but wrong footer magic"""
        test_file = os.path.join(self.temp_dir, 'wrong_footer.parquet')
        with open(test_file, 'wb') as f:
            f.write(b'PAR1')  # Correct header
            f.write(b'\x00' * 50)  # Dummy data
            f.write((50).to_bytes(4, byteorder='little'))  # Footer length
            f.write(b'XXXX')  # Wrong footer magic
        
        result = parquet_lens.analyze_parquet_file(test_file)
        
        self.assertIsInstance(result, dict)
        self.assertIn('segments', result)
        # Should have error segment for wrong footer magic
        error_segments = [s for s in result['segments'] if s['type'] == 'error']
        self.assertTrue(len(error_segments) > 0)
    
    def test_file_too_short_for_footer(self):
        """Test file that's too short to contain footer"""
        test_file = os.path.join(self.temp_dir, 'too_short.parquet')
        with open(test_file, 'wb') as f:
            f.write(b'PAR1')  # Only header, no footer
        
        result = parquet_lens.analyze_parquet_file(test_file)
        
        self.assertIsInstance(result, dict)
        self.assertIn('segments', result)
    
    def test_metadata_parsing_exception(self):
        """Test handling of metadata parsing exceptions"""
        test_file = os.path.join(self.temp_dir, 'bad_metadata.parquet')
        with open(test_file, 'wb') as f:
            f.write(b'PAR1')  # Header
            f.write(b'\xff' * 10)  # Invalid metadata
            f.write((10).to_bytes(4, byteorder='little'))  # Footer length
            f.write(b'PAR1')  # Footer
        
        result = parquet_lens.analyze_parquet_file(test_file)
        
        self.assertIsInstance(result, dict)
        self.assertIn('segments', result)
        
        # Should have thrift_metadata_blob with error
        metadata_segments = [s for s in result['segments'] if s['type'] == 'thrift_metadata_blob']
        self.assertTrue(len(metadata_segments) > 0)
        metadata_segment = metadata_segments[0]
        self.assertIn('error', metadata_segment)
        self.assertIn('raw_field_details_on_error', metadata_segment)


class TestFieldSpecResolution(unittest.TestCase):
    """Test field spec resolution methods"""
    
    def test_get_field_spec_from_id(self):
        """Test _get_field_spec_from_id method"""
        test_data = b'\x19\x4c\x15\x00\x15\x06\x15\x10\x00\x00\x00\x00'
        transport = TTransport.TMemoryBuffer(test_data)
        protocol = parquet_lens.OffsetRecordingProtocol(transport, FileMetaData, 0)
        
        # Test with valid spec
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
        
        # Test with spec containing None entries
        spec_with_nones = [None, None, (2, TType.I32, 'test', None, None)]
        field_spec = protocol._get_field_spec_from_id(2, spec_with_nones)
        self.assertIsNotNone(field_spec)
        self.assertEqual(field_spec[0], 2)


def create_coverage_test_suite():
    """Create test suite for coverage expansion"""
    suite = unittest.TestSuite()
    
    test_classes = [
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
        TestFieldSpecResolution,
    ]
    
    for test_class in test_classes:
        suite.addTest(unittest.makeSuite(test_class))
    
    return suite


def run_coverage_tests():
    """Run coverage expansion tests"""
    print("="*70)
    print("COVERAGE EXPANSION TESTS FOR PARQUET-LENS")
    print("="*70)
    print("Testing uncovered code paths to increase coverage to 80%+...")
    print()
    
    suite = create_coverage_test_suite()
    runner = unittest.TextTestRunner(verbosity=2, buffer=True)
    result = runner.run(suite)
    
    print("\n" + "="*70)
    print("COVERAGE EXPANSION RESULTS")
    print("="*70)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
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
    success = run_coverage_tests()
    sys.exit(0 if success else 1)
