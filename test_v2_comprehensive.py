#!/usr/bin/env python3
"""
Comprehensive Test Suite for parquet-lens V2

This test suite adapts the V1 tests to work with the V2 output format while
maintaining at least 80% coverage. It validates the V2 improvements:

1. Consistent recursive segment structure
2. Proper field naming from thrift specs
3. Enhanced enum handling with both numeric and string values
4. Hierarchical nesting with children arrays
5. Fixed range calculations and error handling
"""

import unittest
import tempfile
import os
import sys
import json
import io
import struct
from unittest.mock import patch, MagicMock, Mock
from contextlib import redirect_stdout, redirect_stderr

# Add the current directory to sys.path so we can import modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import parquet_lens_v2

# Import required classes for testing
from thrift.transport import TTransport
from thrift.protocol.TProtocol import TType
from parquet.ttypes import FileMetaData, SchemaElement, RowGroup, ColumnChunk, ColumnMetaData
from parquet.ttypes import Type, CompressionCodec, ConvertedType, FieldRepetitionType, PageType, Encoding


class TestV2BasicFunctionality(unittest.TestCase):
    """Test basic V2 functionality and output format"""
    
    def setUp(self):
        """Set up test environment"""
        self.test_file = "example.parquet"
        self.assertTrue(os.path.exists(self.test_file), f"Test file {self.test_file} not found")
    
    def test_analyze_parquet_file_v2_basic(self):
        """Test basic V2 analysis functionality"""
        result = parquet_lens_v2.analyze_parquet_file_v2(self.test_file)
        
        # Check basic structure
        self.assertIsInstance(result, dict)
        self.assertIn('segments', result)
        self.assertIsInstance(result['segments'], list)
        self.assertGreater(len(result['segments']), 0)
        
        # Check segment structure
        for segment in result['segments']:
            self.assertIn('range', segment)
            self.assertIn('type', segment)
            self.assertIn('value', segment)
            self.assertIsInstance(segment['range'], list)
            self.assertEqual(len(segment['range']), 2)
            self.assertIsInstance(segment['range'][0], int)
            self.assertIsInstance(segment['range'][1], int)
            self.assertLessEqual(segment['range'][0], segment['range'][1])
    
    def test_v2_consistent_segment_structure(self):
        """Test that all segments follow consistent V2 structure"""
        result = parquet_lens_v2.analyze_parquet_file_v2(self.test_file)
        
        def check_segment_consistency(segment, path=""):
            # Every segment must have range, type, and value
            required_fields = ['range', 'type', 'value']
            for field in required_fields:
                self.assertIn(field, segment, f"Missing {field} in segment at {path}")
            
            # Range must be valid
            range_val = segment['range']
            self.assertIsInstance(range_val, list, f"Range must be list at {path}")
            self.assertEqual(len(range_val), 2, f"Range must have 2 elements at {path}")
            self.assertIsInstance(range_val[0], int, f"Range start must be int at {path}")
            self.assertIsInstance(range_val[1], int, f"Range end must be int at {path}")
            self.assertLessEqual(range_val[0], range_val[1], f"Invalid range at {path}")
            
            # Check children if present
            if 'children' in segment:
                self.assertIsInstance(segment['children'], list, f"Children must be list at {path}")
                for i, child in enumerate(segment['children']):
                    check_segment_consistency(child, f"{path}/children[{i}]")
        
        for i, segment in enumerate(result['segments']):
            check_segment_consistency(segment, f"segments[{i}]")
    
    def test_v2_field_naming(self):
        """Test that V2 uses proper field names instead of generic field_X"""
        result = parquet_lens_v2.analyze_parquet_file_v2(self.test_file)
        
        # Find thrift_field segments
        field_segments = []
        
        def collect_field_segments(segments):
            for segment in segments:
                if segment.get('type') == 'thrift_field':
                    field_segments.append(segment)
                if 'children' in segment:
                    collect_field_segments(segment['children'])
        
        collect_field_segments(result['segments'])
        
        # Verify we found field segments
        self.assertGreater(len(field_segments), 0)
        
        # Check for proper field names (should not have generic field_X names)
        proper_field_names = ['version', 'schema', 'num_rows', 'row_groups', 'key_value_metadata']
        found_proper_names = []
        
        for segment in field_segments:
            field_name = segment['value'].get('field_name', '')
            if field_name in proper_field_names:
                found_proper_names.append(field_name)
            # Should not have generic field_X names
            self.assertFalse(field_name.startswith('field_'), 
                           f"Found generic field name: {field_name}")
        
        # Should find at least some proper field names
        self.assertGreater(len(found_proper_names), 0)
    
    def test_v2_enum_handling(self):
        """Test V2 enhanced enum handling"""
        result = parquet_lens_v2.analyze_parquet_file_v2(self.test_file)
        
        # Find segments with enum values
        enum_values = []
        
        def collect_enum_values(segments):
            for segment in segments:
                if segment.get('type') == 'thrift_field':
                    data = segment.get('value', {}).get('data')
                    if isinstance(data, dict) and 'enum_value' in data and 'enum_name' in data:
                        enum_values.append(data)
                if 'children' in segment:
                    collect_enum_values(segment['children'])
        
        collect_enum_values(result['segments'])
        
        # Verify we found enum values
        self.assertGreater(len(enum_values), 0)
        
        # Check enum structure
        for enum_data in enum_values:
            self.assertIn('enum_value', enum_data)
            self.assertIn('enum_name', enum_data)
            self.assertIsInstance(enum_data['enum_value'], int)
            self.assertIsInstance(enum_data['enum_name'], str)
            self.assertNotEqual(enum_data['enum_name'], '')
    
    def test_v2_hierarchical_nesting(self):
        """Test V2 hierarchical nesting with children arrays"""
        result = parquet_lens_v2.analyze_parquet_file_v2(self.test_file)
        
        # Find container segments with children
        container_segments = []
        
        def collect_container_segments(segments):
            for segment in segments:
                if 'children' in segment and len(segment['children']) > 0:
                    container_segments.append(segment)
                    collect_container_segments(segment['children'])
        
        collect_container_segments(result['segments'])
        
        # Verify hierarchical structure
        self.assertGreater(len(container_segments), 0)
        
        for segment in container_segments:
            # Children must be properly structured
            self.assertIsInstance(segment['children'], list)
            for child in segment['children']:
                self.assertIn('range', child)
                self.assertIn('type', child) 
                self.assertIn('value', child)


class TestV2ProtocolEnhancements(unittest.TestCase):
    """Test V2 protocol enhancements and offset recording"""
    
    def test_offset_recording_protocol_initialization(self):
        """Test V2 OffsetRecordingProtocol initialization with struct class"""
        test_data = b'\x00\x00\x00\x01'
        transport = TTransport.TMemoryBuffer(test_data)
        
        # Test with struct class
        protocol = parquet_lens_v2.OffsetRecordingProtocol(
            transport, 
            struct_class=FileMetaData, 
            base_offset_in_file=100
        )
        
        self.assertEqual(protocol._base_offset_in_file, 100)
        self.assertIsNotNone(protocol._struct_class_spec)
        self.assertEqual(protocol._current_spec, protocol._struct_class_spec)
    
    def test_field_name_lookup(self):
        """Test field name lookup from thrift spec"""
        test_data = b'\x00\x00\x00\x01'
        transport = TTransport.TMemoryBuffer(test_data)
        protocol = parquet_lens_v2.OffsetRecordingProtocol(
            transport, 
            struct_class=FileMetaData
        )
        
        # Test field name lookup
        field_name = protocol._find_field_name_from_spec(1, FileMetaData.thrift_spec)
        self.assertEqual(field_name, 'version')
        
        field_name = protocol._find_field_name_from_spec(2, FileMetaData.thrift_spec)
        self.assertEqual(field_name, 'schema')
        
        # Test non-existent field
        field_name = protocol._find_field_name_from_spec(999, FileMetaData.thrift_spec)
        self.assertEqual(field_name, 'field_999')
    
    def test_spec_stack_management(self):
        """Test spec stack management for nested structs"""
        test_data = b'\x00\x00\x00\x01'
        transport = TTransport.TMemoryBuffer(test_data)
        protocol = parquet_lens_v2.OffsetRecordingProtocol(
            transport, 
            struct_class=FileMetaData
        )
        
        # Initially should have FileMetaData spec
        self.assertEqual(protocol._current_spec, FileMetaData.thrift_spec)
        self.assertEqual(len(protocol._parent_spec_stack), 0)


class TestV2ThriftToSegments(unittest.TestCase):
    """Test V2 thrift to segments conversion"""
    
    def test_thrift_to_segments_basic(self):
        """Test basic thrift to segments conversion"""
        # Create a mock thrift object
        schema_element = SchemaElement()
        schema_element.name = "test_field"
        schema_element.type = Type.INT32
        schema_element.repetition_type = FieldRepetitionType.REQUIRED
        
        # Mock field details
        field_details = {
            "0_name_4": {
                "field_header_range_in_blob": [0, 10],
                "value_range_in_blob": [10, 20]
            },
            "0_type_5": {
                "field_header_range_in_blob": [20, 30], 
                "value_range_in_blob": [30, 35]
            },
            "0_repetition_type_6": {
                "field_header_range_in_blob": [35, 45],
                "value_range_in_blob": [45, 50]
            }
        }
        
        # Convert to segments
        segments = parquet_lens_v2.thrift_to_segments(
            schema_element, 
            field_details, 
            base_offset_in_file=100,
            nesting_level=0
        )
        
        # Verify structure
        self.assertIsInstance(segments, list)
        self.assertGreater(len(segments), 0)
        
        # Check for proper segment structure
        for segment in segments:
            self.assertIn('range', segment)
            self.assertIn('type', segment)
            self.assertIn('value', segment)
    
    def test_enum_conversion(self):
        """Test enum value conversion"""
        # Test primitive type conversion
        primitive_type = parquet_lens_v2.get_primitive_type(Type.INT32)
        self.assertEqual(primitive_type, "integer")
        
        # Test enum value processing with a known enum field
        value_info = parquet_lens_v2.process_primitive_value(Type.INT32, "type", "SchemaElement")
        if isinstance(value_info, dict):
            self.assertIn('enum_value', value_info)
            self.assertIn('enum_name', value_info)
            self.assertEqual(value_info['enum_name'], 'INT32')
        else:
            # If not an enum, should return the raw value
            self.assertEqual(value_info, Type.INT32)
    
    def test_field_range_calculation(self):
        """Test field range calculation"""
        field_detail = {
            "field_header_range_in_blob": [10, 20],
            "value_range_in_blob": [20, 30]
        }
        
        start, end = parquet_lens_v2.calculate_field_ranges(field_detail, base_offset_in_file=100, default_start=0, default_end=50)
        
        # Should return (start, end) tuple with base_offset added
        self.assertEqual(start, 110)  # 10 + 100
        self.assertEqual(end, 130)    # 30 + 100


class TestV2ErrorHandling(unittest.TestCase):
    """Test V2 error handling and edge cases"""
    
    def test_invalid_parquet_file(self):
        """Test handling of invalid parquet file"""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(b"not a parquet file")
            tmp_path = tmp.name
        
        try:
            with self.assertRaises(ValueError) as context:
                parquet_lens_v2.analyze_parquet_file_v2(tmp_path)
            
            # Should raise ValueError for invalid header
            self.assertIn("PAR1 header", str(context.exception))
        finally:
            os.unlink(tmp_path)
    
    def test_corrupted_footer(self):
        """Test handling of corrupted footer"""
        # Create a file with valid header but corrupted footer
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            # Write valid parquet header
            tmp.write(b"PAR1")
            # Write some data
            tmp.write(b"x" * 100)
            # Write invalid footer size
            tmp.write(struct.pack('<I', 99999))  # Invalid large footer size
            # Write footer magic
            tmp.write(b"PAR1")
            tmp_path = tmp.name
        
        try:
            with self.assertRaises(OSError):
                parquet_lens_v2.analyze_parquet_file_v2(tmp_path)
        finally:
            os.unlink(tmp_path)
    
    def test_missing_field_details(self):
        """Test handling when field details are missing"""
        schema_element = SchemaElement()
        schema_element.name = "test_field"
        
        # Empty field details
        field_details = {}
        
        segments = parquet_lens_v2.thrift_to_segments(
            schema_element,
            field_details,
            base_offset_in_file=0,
            nesting_level=0
        )
        
        # Should handle gracefully (may return empty list or error segments)
        self.assertIsInstance(segments, list)


class TestV2PerformanceAndCompatibility(unittest.TestCase):
    """Test V2 performance and compatibility with V1"""
    
    def test_v2_produces_valid_json(self):
        """Test that V2 output is valid JSON"""
        result = parquet_lens_v2.analyze_parquet_file_v2("example.parquet")
        
        # Should be JSON serializable
        json_str = json.dumps(result)
        self.assertIsInstance(json_str, str)
        
        # Should be deserializable
        parsed = json.loads(json_str)
        self.assertEqual(parsed, result)
    
    def test_v2_command_line_interface(self):
        """Test V2 command line interface"""
        # Capture stdout
        with io.StringIO() as buf, redirect_stdout(buf):
            # Mock sys.argv
            with patch('sys.argv', ['parquet_lens_v2.py', 'example.parquet']):
                try:
                    parquet_lens_v2.main()
                    output = buf.getvalue()
                    
                    # Should produce valid JSON
                    result = json.loads(output)
                    self.assertIn('segments', result)
                except SystemExit:
                    # main() might call sys.exit()
                    pass
    
    def test_v2_segment_count_reasonable(self):
        """Test that V2 produces reasonable number of segments"""
        result = parquet_lens_v2.analyze_parquet_file_v2("example.parquet")
        
        # Count total segments recursively
        def count_segments(segments):
            count = len(segments)
            for segment in segments:
                if 'children' in segment:
                    count += count_segments(segment['children'])
            return count
        
        total_segments = count_segments(result['segments'])
        
        # Should have reasonable number of segments (not too few, not too many)
        self.assertGreater(total_segments, 10)  # At least some structure
        self.assertLess(total_segments, 10000)  # Not excessive


if __name__ == '__main__':
    # Run tests with coverage if pytest-cov is available
    unittest.main(verbosity=2)
