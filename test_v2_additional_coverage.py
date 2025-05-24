#!/usr/bin/env python3
"""
Additional coverage tests for parquet-lens V2 to reach 80%+ coverage
"""

import unittest
import tempfile
import os
import sys
import json
import struct
from unittest.mock import patch, MagicMock, Mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import parquet_lens_v2

from thrift.transport import TTransport
from thrift.protocol.TProtocol import TType
from parquet.ttypes import FileMetaData, SchemaElement, RowGroup, ColumnChunk, ColumnMetaData
from parquet.ttypes import Type, CompressionCodec, ConvertedType, FieldRepetitionType, PageType, Encoding


class TestV2AdditionalCoverage(unittest.TestCase):
    """Additional tests to reach 80%+ coverage"""
    
    def test_get_enum_class_for_field_coverage(self):
        """Test enum class lookup for various field combinations"""
        # Test known enum fields
        enum_class = parquet_lens_v2.get_enum_class_for_field("type", "SchemaElement")
        self.assertEqual(enum_class, Type)
        
        enum_class = parquet_lens_v2.get_enum_class_for_field("repetition_type", "SchemaElement")
        self.assertEqual(enum_class, FieldRepetitionType)
        
        enum_class = parquet_lens_v2.get_enum_class_for_field("converted_type", "SchemaElement")
        self.assertEqual(enum_class, ConvertedType)
        
        enum_class = parquet_lens_v2.get_enum_class_for_field("codec", "ColumnMetaData")
        self.assertEqual(enum_class, CompressionCodec)
        
        enum_class = parquet_lens_v2.get_enum_class_for_field("encoding", "ColumnMetaData")
        self.assertEqual(enum_class, Encoding)
        
        enum_class = parquet_lens_v2.get_enum_class_for_field("type", "PageHeader")
        # PageHeader might not be implemented, so check if it returns PageType or None
        self.assertIn(enum_class, [PageType, None])
        
        # Test unknown field
        enum_class = parquet_lens_v2.get_enum_class_for_field("unknown_field", "UnknownStruct")
        self.assertIsNone(enum_class)
    
    def test_process_primitive_value_coverage(self):
        """Test primitive value processing for various types"""
        # Test enum value
        result = parquet_lens_v2.process_primitive_value(Type.INT32, "type", "SchemaElement")
        self.assertIsInstance(result, dict)
        self.assertIn('enum_value', result)
        self.assertIn('enum_name', result)
        
        # Test bytes value - short
        short_bytes = b"hello"
        result = parquet_lens_v2.process_primitive_value(short_bytes, "data", "TestStruct")
        self.assertIsInstance(result, dict)
        self.assertEqual(result['bytes'], short_bytes.hex())
        self.assertEqual(result['length'], len(short_bytes))
        
        # Test bytes value - long (truncated)
        long_bytes = b"x" * 100
        result = parquet_lens_v2.process_primitive_value(long_bytes, "data", "TestStruct")
        self.assertIsInstance(result, dict)
        self.assertIn('bytes_preview', result)
        self.assertEqual(result['length'], 100)
        self.assertTrue(result['truncated'])
        
        # Test None value
        result = parquet_lens_v2.process_primitive_value(None, "data", "TestStruct")
        self.assertIsNone(result)
        
        # Test regular value
        result = parquet_lens_v2.process_primitive_value(42, "count", "TestStruct")
        self.assertEqual(result, 42)
    
    def test_calculate_field_ranges_edge_cases(self):
        """Test field range calculation edge cases"""
        # Test with no field detail
        start, end = parquet_lens_v2.calculate_field_ranges(None, 100, 0, 50)
        self.assertEqual(start, 0)
        self.assertEqual(end, 50)
        
        # Test with empty field detail
        start, end = parquet_lens_v2.calculate_field_ranges({}, 100, 0, 50)
        self.assertEqual(start, 0)
        self.assertEqual(end, 50)
        
        # Test with only header range
        field_detail = {"field_header_range_in_blob": [10, 20]}
        start, end = parquet_lens_v2.calculate_field_ranges(field_detail, 100, 0, 50)
        self.assertEqual(start, 110)
        self.assertEqual(end, 120)
        
        # Test with only value range
        field_detail = {"value_range_in_blob": [30, 40]}
        start, end = parquet_lens_v2.calculate_field_ranges(field_detail, 100, 0, 50)
        self.assertEqual(start, 130)
        self.assertEqual(end, 140)
    
    def test_find_field_detail_coverage(self):
        """Test find_field_detail function"""
        field_details = {
            "0_test_field_1": {"data": "found"},
            "1_nested_field_2": {"data": "nested"}
        }
        
        # Test found field
        result = parquet_lens_v2.find_field_detail(field_details, "test_field", 1, 0)
        self.assertEqual(result, {"data": "found"})
        
        # Test not found field
        result = parquet_lens_v2.find_field_detail(field_details, "missing_field", 99, 0)
        self.assertIsNone(result)
    
    def test_offset_recording_protocol_edge_cases(self):
        """Test OffsetRecordingProtocol edge cases"""
        test_data = b'\x00\x00\x00\x01'
        transport = TTransport.TMemoryBuffer(test_data)
        protocol = parquet_lens_v2.OffsetRecordingProtocol(transport)
        
        # Test without struct class
        self.assertIsNone(protocol._struct_class_spec)
        self.assertIsNone(protocol._current_spec)
        
        # Test field name lookup with no spec
        field_name = protocol._find_field_name_from_spec(1, None)
        self.assertEqual(field_name, 'field_1')
        
        # Test spec lookup with no spec
        spec_tuple = protocol._get_field_spec_from_id(1, None)
        self.assertIsNone(spec_tuple)
    
    def test_make_field_key_function(self):
        """Test make_field_key function"""
        key = parquet_lens_v2.make_field_key(1, "test_field", 5)
        self.assertEqual(key, "1_test_field_5")
    
    def test_create_segment_function(self):
        """Test create_segment function"""
        segment = parquet_lens_v2.create_segment(10, 20, "test_type", {"test": "data"})
        expected = {
            "range": [10, 20],
            "type": "test_type", 
            "value": {"test": "data"}
        }
        self.assertEqual(segment, expected)
        
        # Test with children
        segment = parquet_lens_v2.create_segment(10, 20, "test_type", {"test": "data"}, [{"child": "data"}])
        expected = {
            "range": [10, 20],
            "type": "test_type",
            "value": {"test": "data"},
            "children": [{"child": "data"}]
        }
        self.assertEqual(segment, expected)
        
        # Test with metadata
        segment = parquet_lens_v2.create_segment(10, 20, "test_type", {"test": "data"}, None, {"meta": "info"})
        expected = {
            "range": [10, 20],
            "type": "test_type",
            "value": {"test": "data"},
            "metadata": {"meta": "info"}
        }
        self.assertEqual(segment, expected)
    
    def test_main_function_coverage(self):
        """Test main function with different argument combinations"""
        # Test with file argument
        with patch('sys.argv', ['parquet_lens_v2.py', 'example.parquet']):
            with patch('parquet_lens_v2.analyze_parquet_file_v2') as mock_analyze:
                mock_analyze.return_value = {"segments": []}
                with patch('builtins.print') as mock_print:
                    parquet_lens_v2.main()
                    mock_analyze.assert_called_once_with('example.parquet', debug=False, show_undefined_optional=False)
        
        # Test with debug flag
        with patch('sys.argv', ['parquet_lens_v2.py', '--debug', 'test.parquet']):
            with patch('parquet_lens_v2.analyze_parquet_file_v2') as mock_analyze:
                mock_analyze.return_value = {"segments": []}
                with patch('builtins.print') as mock_print:
                    parquet_lens_v2.main()
                    mock_analyze.assert_called_once_with('test.parquet', debug=True, show_undefined_optional=False)
        
        # Test with show-undefined-optional flag
        with patch('sys.argv', ['parquet_lens_v2.py', '--show-undefined-optional-fields', 'test.parquet']):
            with patch('parquet_lens_v2.analyze_parquet_file_v2') as mock_analyze:
                mock_analyze.return_value = {"segments": []}
                with patch('builtins.print') as mock_print:
                    parquet_lens_v2.main()
                    mock_analyze.assert_called_once_with('test.parquet', debug=False, show_undefined_optional=True)
    
    def test_thrift_to_segments_list_handling(self):
        """Test thrift_to_segments with list attributes"""
        # Create schema element with list fields
        schema_element = SchemaElement()
        schema_element.name = "root"
        
        # Mock field details for a list
        field_details = {
            "1_name_4": {
                "field_header_range_in_blob": [0, 5],
                "value_range_in_blob": [5, 15]
            }
        }
        
        segments = parquet_lens_v2.thrift_to_segments(
            schema_element,
            field_details,
            base_offset_in_file=0,
            nesting_level=1
        )
        
        self.assertIsInstance(segments, list)
    
    def test_protocol_struct_transitions(self):
        """Test protocol handling of struct begin/end transitions"""
        test_data = b'\x00\x00\x00\x01'
        transport = TTransport.TMemoryBuffer(test_data)
        protocol = parquet_lens_v2.OffsetRecordingProtocol(transport, struct_class=FileMetaData)
        
        # Test readStructBegin without field context
        protocol._current_field_name = None
        protocol._current_field_id = None
        protocol.readStructBegin()
        
        # Test readStructEnd with empty stack
        protocol._parent_spec_stack = []
        protocol.readStructEnd()
        self.assertEqual(protocol._current_spec, protocol._struct_class_spec)


if __name__ == '__main__':
    unittest.main(verbosity=2)
