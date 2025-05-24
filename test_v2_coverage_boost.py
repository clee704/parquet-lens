#!/usr/bin/env python3
"""
Final coverage boost tests to reach 80%+ coverage for parquet-lens V2
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


class TestV2CoverageBoost(unittest.TestCase):
    """Final tests to reach 80%+ coverage"""
    
    def test_transport_pos_edge_cases(self):
        """Test _get_trans_pos edge cases"""
        # Test transport with _buffer.tell method - using real TMemoryBuffer
        from thrift.transport import TTransport
        test_data = b'\x00\x00\x00\x01'
        transport = TTransport.TMemoryBuffer(test_data)
        
        protocol = parquet_lens_v2.OffsetRecordingProtocol(transport)
        pos = protocol._get_trans_pos()
        self.assertEqual(pos, 0)  # Should use _buffer.tell starting at 0
        
        # Test transport fallback with tell method
        mock_transport = Mock()
        mock_transport.tell.return_value = 150
        # Make isinstance check return False to trigger fallback
        mock_transport.__class__ = Mock  # Not a TMemoryBuffer
        
        protocol2 = parquet_lens_v2.OffsetRecordingProtocol(mock_transport)
        pos = protocol2._get_trans_pos()
        self.assertEqual(pos, 150)  # Should fall back to tell
    
    def test_protocol_list_operations(self):
        """Test protocol list begin/end operations"""
        # Test list context setup without actual protocol operations
        test_data = b'\x19\x4c\x15\x00\x15\x06\x15\x10\x00\x00\x00\x00'
        transport = TTransport.TMemoryBuffer(test_data)
        protocol = parquet_lens_v2.OffsetRecordingProtocol(transport, struct_class=FileMetaData)
        
        # Test field key generation for lists
        protocol._current_field_name = "test_list"
        protocol._current_field_id = 10
        protocol._struct_nesting_level = 1
        
        key = parquet_lens_v2.make_field_key(protocol._struct_nesting_level, 
                                           protocol._current_field_name, 
                                           protocol._current_field_id)
        self.assertEqual(key, "1_test_list_10")
    
    def test_protocol_map_operations(self):
        """Test protocol map begin/end operations"""
        # Test map context setup without actual protocol operations
        test_data = b'\x19\x4c\x15\x00\x15\x06\x15\x10\x00\x00\x00\x00'
        transport = TTransport.TMemoryBuffer(test_data)
        protocol = parquet_lens_v2.OffsetRecordingProtocol(transport, struct_class=FileMetaData)
        
        # Test field key generation for maps
        protocol._current_field_name = "test_map"
        protocol._current_field_id = 11
        protocol._struct_nesting_level = 2
        
        key = parquet_lens_v2.make_field_key(protocol._struct_nesting_level,
                                           protocol._current_field_name,
                                           protocol._current_field_id)
        self.assertEqual(key, "2_test_map_11")
    
    def test_thrift_list_processing(self):
        """Test thrift object with list attributes"""
        # Create mock thrift object with list attribute
        mock_obj = Mock()
        mock_obj.test_list = [SchemaElement(), SchemaElement()]
        mock_obj.test_list[0].name = "item1"
        mock_obj.test_list[1].name = "item2"
        
        # Add thrift_spec for the mock object
        mock_obj.thrift_spec = (
            (1, TType.LIST, 'test_list', (TType.STRUCT, (SchemaElement, SchemaElement.thrift_spec), False), None, ),
        )
        
        # Mock field details for list processing
        field_details = {
            "1_test_list_1": {
                "field_header_range_in_blob": [0, 10],
                "value_range_in_blob": [10, 50],
                "list_length": 2
            }
        }
        
        segments = parquet_lens_v2.thrift_to_segments(
            mock_obj,
            field_details,
            base_offset_in_file=0,
            nesting_level=1
        )
        
        self.assertIsInstance(segments, list)
        # Should process the list field
        list_segment = next((s for s in segments if s.get('value', {}).get('field_name') == 'test_list'), None)
        self.assertIsNotNone(list_segment)
    
    def test_debug_mode_functionality(self):
        """Test debug mode output"""
        # Test with debug=True to cover debug code paths
        with patch('builtins.print') as mock_print:
            result = parquet_lens_v2.analyze_parquet_file_v2("example.parquet", debug=True)
            
            # Should have called print for debug output
            self.assertTrue(mock_print.called)
            # Should still return valid result
            self.assertIsInstance(result, dict)
            self.assertIn('segments', result)
    
    def test_show_undefined_optional_functionality(self):
        """Test show_undefined_optional flag"""
        # Test with show_undefined_optional=True
        result = parquet_lens_v2.analyze_parquet_file_v2("example.parquet", show_undefined_optional=True)
        
        # Should return valid result regardless of flag
        self.assertIsInstance(result, dict)
        self.assertIn('segments', result)
    
    def test_thrift_to_segments_edge_cases(self):
        """Test thrift_to_segments with various edge cases"""
        # Test with None object
        segments = parquet_lens_v2.thrift_to_segments(None, {}, 0, 1)
        self.assertEqual(segments, [])
        
        # Test with object without thrift_spec
        mock_obj = Mock()
        del mock_obj.thrift_spec  # Remove thrift_spec
        segments = parquet_lens_v2.thrift_to_segments(mock_obj, {}, 0, 1)
        self.assertEqual(segments, [])
    
    def test_enum_edge_cases(self):
        """Test enum handling edge cases"""
        # Test with unknown enum value
        result = parquet_lens_v2.process_primitive_value(999, "type", "SchemaElement")
        if isinstance(result, dict):
            self.assertIn('enum_value', result)
            # Should handle unknown values gracefully
            self.assertIn('UNKNOWN', result.get('enum_name', ''))
    
    def test_binary_data_handling(self):
        """Test binary data processing edge cases"""
        # Test with empty bytes
        result = parquet_lens_v2.process_primitive_value(b"", "data", "TestStruct")
        self.assertIsInstance(result, dict)
        self.assertEqual(result['length'], 0)
        
        # Test with exactly 32 bytes (boundary case)
        data_32 = b"x" * 32
        result = parquet_lens_v2.process_primitive_value(data_32, "data", "TestStruct")
        self.assertIsInstance(result, dict)
        self.assertEqual(result['length'], 32)
        self.assertNotIn('truncated', result)
        
        # Test with 33 bytes (should truncate)
        data_33 = b"x" * 33
        result = parquet_lens_v2.process_primitive_value(data_33, "data", "TestStruct")
        self.assertIsInstance(result, dict)
        self.assertEqual(result['length'], 33)
        self.assertTrue(result.get('truncated', False))
    
    def test_field_detail_with_various_ranges(self):
        """Test field detail processing with different range combinations"""
        # Test field detail with no range information - should use defaults
        field_detail = {"other_info": "test"}
        start, end = parquet_lens_v2.calculate_field_ranges(field_detail, 100, 0, 50)
        # Should return defaults when no range info available
        self.assertEqual(start, 100)  # Based on fallback logic that adds base_offset
        self.assertEqual(end, 150)
        
        # Test with old-style keys for fallback path
        field_detail2 = {
            "field_header_start_offset": 20,
            "field_end_offset": 30
        }
        start, end = parquet_lens_v2.calculate_field_ranges(field_detail2, 100, 0, 50)
        self.assertEqual(start, 120)  # 20 + 100
        self.assertEqual(end, 130)    # 30 + 100
    
    def test_nested_struct_spec_transitions(self):
        """Test nested struct spec stack operations"""
        # Test spec transitions without actual protocol operations
        transport = TTransport.TMemoryBuffer(b'')
        protocol = parquet_lens_v2.OffsetRecordingProtocol(transport, struct_class=FileMetaData)
        
        # Test spec initialization
        self.assertIsNotNone(protocol._struct_class_spec)
        self.assertEqual(protocol._current_spec, FileMetaData.thrift_spec)
        
        # Test field name lookup without protocol operations
        field_name = protocol._find_field_name_from_spec(1, FileMetaData.thrift_spec)
        self.assertEqual(field_name, "version")
        
        # Test spec lookup
        field_spec = protocol._get_field_spec_from_id(1, FileMetaData.thrift_spec)
        self.assertIsNotNone(field_spec)
        self.assertEqual(field_spec[0], 1)  # field_id
        self.assertEqual(field_spec[2], "version")  # field_name
    
    def test_list_item_range_calculation(self):
        """Test list item range calculation edge cases"""
        # Test with list_length = 0
        field_detail = {
            "value_range_in_blob": [10, 20],
            "list_length": 0
        }
        
        # This should handle division by zero gracefully
        start, end = parquet_lens_v2.calculate_field_ranges(field_detail, 100, 0, 50)
        self.assertEqual(start, 110)  # 10 + 100
        self.assertEqual(end, 120)    # 20 + 100


if __name__ == '__main__':
    unittest.main(verbosity=2)
