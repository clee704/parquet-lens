#!/usr/bin/env python3
"""
Final comprehensive test coverage to reach 80% coverage target.
Focuses on testing high-level functions that contain uncovered lines.
"""

import unittest
import os
import tempfile
from unittest.mock import patch, MagicMock
import parquet_lens
from parquet.ttypes import FileMetaData, SchemaElement, PageType, Encoding
from thrift.transport.TTransport import TMemoryBuffer
from thrift.protocol.TProtocol import TType


class MockTransportWithoutTell:
    """Mock transport that doesn't have tell() method to trigger line 79"""
    def __init__(self):
        self._buffer = object()  # Object without tell() method


class TestCoverageTargeted(unittest.TestCase):
    """Targeted tests for specific uncovered lines"""

    def test_transport_fallback_no_tell_method(self):
        """Test line 79 - transport fallback when tell() method doesn't exist"""
        mock_transport = MockTransportWithoutTell()
        
        # Create protocol with mock transport
        protocol = parquet_lens.OffsetRecordingProtocol(mock_transport, FileMetaData, 0)
        
        # This should trigger the AttributeError on line 79
        with self.assertRaises(AttributeError) as cm:
            protocol._get_trans_pos()
        
        self.assertIn("does not have a usable '_buffer' attribute with a 'tell' method", str(cm.exception))

    def test_get_thrift_type_name_list_element_class_name(self):
        """Test lines 360-362 - list type with element class having __name__"""
        # Test with a mock class that has __name__ attribute
        class MockClass:
            __name__ = "MockElement"
        
        # Test TType.LIST with element class having __name__
        result = parquet_lens.get_thrift_type_name(TType.LIST, (MockClass, TType.STRUCT))
        self.assertEqual(result, "list<MockElement>")

    def test_get_thrift_type_name_list_legacy_format(self):
        """Test lines 365-377 - list type with legacy format handling"""
        # Test legacy format with struct tuple
        class MockStruct:
            __name__ = "MockStruct"
        
        # Legacy format: tuple containing tuple with struct class
        legacy_args = ((MockStruct,),)
        result = parquet_lens.get_thrift_type_name(TType.LIST, legacy_args)
        self.assertEqual(result, "list<MockStruct>")
        
        # Test primitive type lists (line 375-377)
        primitive_args = (TType.STRING,)
        result = parquet_lens.get_thrift_type_name(TType.LIST, primitive_args)
        self.assertEqual(result, "list<string>")

    def test_process_field_list_with_enum_elements(self):
        """Test lines 435-475 - LIST processing with enum elements"""
        # Create a mock enum class
        class MockEnum:
            _VALUES_TO_NAMES = {1: "VALUE_ONE", 2: "VALUE_TWO"}
        
        # Create a mock thrift object with a list field
        class MockThriftObj:
            def __init__(self):
                self.enum_list = [1, 2]
        
        obj = MockThriftObj()
        
        # Mock spec tuple for list field with enum elements
        spec_tuple = (1, TType.LIST, 'enum_list', (MockEnum,), None)
        
        # Test the _process_field_value function
        result = parquet_lens._process_field_value(obj, spec_tuple, {}, 0, 0, False)
        
        self.assertIsInstance(result, dict)
        self.assertIn('processed_value', result)
        self.assertEqual(result['processed_value'], ["VALUE_ONE", "VALUE_TWO"])

    def test_process_field_list_with_struct_elements(self):
        """Test lines 450-470 - LIST processing with struct elements"""
        # Create a mock struct for list elements
        class MockStructElement:
            def __init__(self, value):
                self.field = value
            
            def __dict__(self):
                return {'field': self.field}
        
        # Create a mock thrift object with a list of structs
        class MockThriftObj:
            def __init__(self):
                self.struct_list = [MockStructElement("test1"), MockStructElement("test2")]
        
        obj = MockThriftObj()
        
        # Mock spec tuple for list field with struct elements
        spec_tuple = (1, TType.LIST, 'struct_list', ((MockStructElement,),), None)
        
        # Mock the thrift_to_dict_with_offsets function to return test data
        with patch('parquet_lens.thrift_to_dict_with_offsets') as mock_func:
            mock_func.return_value = {"field": "test_value"}
            
            result = parquet_lens._process_field_value(obj, spec_tuple, {}, 0, 0, False)
        
        self.assertIsInstance(result, dict)
        self.assertIn('processed_value', result)
        self.assertIsInstance(result['processed_value'], list)

    def test_process_field_struct_with_range(self):
        """Test lines 481-508 - STRUCT processing with range information"""
        # Create a mock struct
        class MockStruct:
            def __init__(self):
                self.inner_field = "test"
        
        # Create a mock thrift object with a struct field
        class MockThriftObj:
            def __init__(self):
                self.struct_field = MockStruct()
        
        obj = MockThriftObj()
        
        # Mock spec tuple for struct field
        spec_tuple = (1, TType.STRUCT, 'struct_field', (MockStruct,), None)
        
        # Create field details with range information
        field_details_map = {
            "0_struct_field_1": {
                'value_range_in_blob': [10, 20]
            }
        }
        
        # Mock the thrift_to_dict_with_offsets function
        with patch('parquet_lens.thrift_to_dict_with_offsets') as mock_func:
            mock_func.return_value = [{"field": "inner_field", "value": "test"}]
            
            result = parquet_lens._process_field_value(obj, spec_tuple, field_details_map, 100, 0, False)
        
        self.assertIsInstance(result, dict)
        self.assertIn('processed_value', result)
        struct_info = result['processed_value']
        self.assertIn('value_range', struct_info)
        self.assertEqual(struct_info['value_range'], [110, 120])  # base_offset_in_file + blob_range

    def test_process_field_bytes_decode(self):
        """Test lines 512-515 - bytes field processing"""
        # Create a mock thrift object with bytes field
        class MockThriftObj:
            def __init__(self):
                self.bytes_field = b"test_string"
                self.invalid_bytes_field = b"\xff\xfe\xfd"
        
        obj = MockThriftObj()
        
        # Test successful UTF-8 decode
        spec_tuple = (1, TType.STRING, 'bytes_field', None, None)
        result = parquet_lens._process_field_value(obj, spec_tuple, {}, 0, 0, False)
        self.assertEqual(result['processed_value'], "test_string")
        
        # Test failed UTF-8 decode (should return hex)
        spec_tuple = (2, TType.STRING, 'invalid_bytes_field', None, None)
        result = parquet_lens._process_field_value(obj, spec_tuple, {}, 0, 0, False)
        self.assertEqual(result['processed_value'], "fffefd")

    def test_process_field_enum_from_type_args(self):
        """Test lines 524-525 - enum processing from type_args"""
        # Create a mock enum class
        class MockEnum:
            _VALUES_TO_NAMES = {1: "ENUM_VALUE_ONE", 2: "ENUM_VALUE_TWO"}
        
        # Create a mock thrift object with enum field
        class MockThriftObj:
            def __init__(self):
                self.enum_field = 1
        
        obj = MockThriftObj()
        
        # Mock spec tuple with enum in type_args
        spec_tuple = (1, TType.I32, 'enum_field', (MockEnum,), None)
        
        result = parquet_lens._process_field_value(obj, spec_tuple, {}, 0, 0, False)
        
        self.assertIsInstance(result, dict)
        self.assertIn('processed_value', result)
        self.assertEqual(result['processed_value'], "ENUM_VALUE_ONE")
        self.assertTrue(result['is_enum'])

    def test_process_field_enum_from_field_name(self):
        """Test lines 531, 534 - enum processing from field name lookup"""
        # Create a mock enum class
        class MockEnum:
            _VALUES_TO_NAMES = {1: "FOUND_ENUM_VALUE"}
        
        # Create a mock thrift object with enum field
        class MockThriftObj:
            def __init__(self):
                self.special_enum_field = 1
        
        obj = MockThriftObj()
        
        # Mock spec tuple without enum in type_args
        spec_tuple = (1, TType.I32, 'special_enum_field', None, None)
        
        # Mock get_enum_class_for_field to return our enum class
        with patch('parquet_lens.get_enum_class_for_field', return_value=MockEnum):
            result = parquet_lens._process_field_value(obj, spec_tuple, {}, 0, 0, False)
        
        self.assertIsInstance(result, dict)
        self.assertIn('processed_value', result)
        self.assertEqual(result['processed_value'], "FOUND_ENUM_VALUE")
        self.assertTrue(result['is_enum'])

    def test_apply_spec_with_undefined_optional_fields(self):
        """Test lines 580-596 - apply_spec with show_undefined_optional_fields"""
        # Create a mock thrift object
        class MockThriftObj:
            thrift_spec = [
                None,
                (1, TType.STRING, 'defined_field', None, None),
                (2, TType.I32, 'undefined_optional_field', None, None, 2),  # field 5 indicates optional
            ]
            
            def __init__(self):
                self.defined_field = "test"
                # undefined_optional_field is not set
        
        obj = MockThriftObj()
        
        # Test with show_undefined_optional_fields=True
        result = parquet_lens.apply_spec_with_offsets(obj, {}, 0, 0, True)
        
        self.assertIsInstance(result, list)
        # Should include both defined and undefined optional fields
        self.assertEqual(len(result), 2)

    def test_debug_mode_field_details(self):
        """Test lines 607-621 - debug mode field details inclusion"""
        # Create a mock thrift object
        class MockThriftObj:
            thrift_spec = [
                None,
                (1, TType.STRING, 'test_field', None, None),
            ]
            
            def __init__(self):
                self.test_field = "test_value"
        
        obj = MockThriftObj()
        
        # Create field details with debug information
        field_details_map = {
            "0_test_field_1": {
                'value_range_in_blob': [10, 20],
                'field_total_range_in_blob': [5, 25],
                'value_start_offset': 15,
                'debug_info': 'test_debug'
            }
        }
        
        # Mock is_debug_mode to return True
        with patch('parquet_lens.is_debug_mode', return_value=True):
            result = parquet_lens.apply_spec_with_offsets(obj, field_details_map, 100, 0, False)
        
        self.assertIsInstance(result, list)
        self.assertTrue(len(result) > 0)
        
        # Find the field result
        field_result = result[0]
        self.assertIn('debug_details', field_result)

    def test_create_field_info_with_ranges(self):
        """Test lines 627-635 - create_field_info with various range types"""
        field_name = "test_field"
        field_id = 1
        nesting_level = 0
        
        # Create field details with all range types
        field_details_map = {
            "0_test_field_1": {
                'value_range_in_blob': [10, 20],
                'field_total_range_in_blob': [5, 25],
            }
        }
        
        base_offset_in_file = 100
        
        field_info = parquet_lens.create_field_info(
            field_name, field_id, "string", "test_value", False,
            field_details_map, base_offset_in_file, nesting_level
        )
        
        self.assertIn('value_range', field_info)
        self.assertIn('field_total_range', field_info)
        self.assertEqual(field_info['value_range'], [110, 120])
        self.assertEqual(field_info['field_total_range'], [105, 125])

    def test_create_field_info_without_ranges(self):
        """Test lines 648, 650 - create_field_info without range information"""
        field_name = "test_field"
        field_id = 1
        nesting_level = 0
        
        # Empty field details map
        field_details_map = {}
        base_offset_in_file = 100
        
        field_info = parquet_lens.create_field_info(
            field_name, field_id, "string", "test_value", False,
            field_details_map, base_offset_in_file, nesting_level
        )
        
        # Should not have range information
        self.assertNotIn('value_range', field_info)
        self.assertNotIn('field_total_range', field_info)

    def test_is_debug_mode_functions(self):
        """Test lines 668-671 - debug mode functions"""
        # Test with DEBUG environment variable set
        with patch.dict(os.environ, {'DEBUG': '1'}):
            self.assertTrue(parquet_lens.is_debug_mode())
        
        # Test with DEBUG environment variable not set
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(parquet_lens.is_debug_mode())


class TestInternalFunctionCoverage(unittest.TestCase):
    """Test internal functions to increase coverage"""

    def test_get_enum_class_for_field_coverage(self):
        """Test get_enum_class_for_field with various field names"""
        # Test known enum field
        result = parquet_lens.get_enum_class_for_field("type", "SchemaElement")
        self.assertIsNotNone(result)
        
        # Test unknown field
        result = parquet_lens.get_enum_class_for_field("unknown_field", "UnknownStruct")
        self.assertIsNone(result)

    def test_write_formatted_output_file_coverage(self):
        """Test write_formatted_output with file writing (line 801)"""
        test_data = {"test": "data"}
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            temp_file = f.name
        
        try:
            # This should exercise line 801
            parquet_lens.write_formatted_output(test_data, temp_file)
            
            # Verify file was written
            self.assertTrue(os.path.exists(temp_file))
            
            # Read and verify content
            with open(temp_file, 'r') as f:
                content = f.read()
                self.assertIn('"test"', content)
                self.assertIn('"data"', content)
                
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)

    def test_main_with_output_file_arg(self):
        """Test main function with output file argument (line 848)"""
        # Create a simple test parquet file path (doesn't need to exist for this test)
        test_args = ['script_name', 'nonexistent.parquet', '--output', 'test_output.json']
        
        # Mock sys.argv and the analyze_parquet function
        with patch('sys.argv', test_args):
            with patch('parquet_lens.analyze_parquet') as mock_analyze:
                with patch('parquet_lens.write_formatted_output') as mock_write:
                    mock_analyze.return_value = {"test": "result"}
                    
                    # This should trigger the output file writing path (line 848)
                    try:
                        parquet_lens.main()
                    except SystemExit:
                        pass  # main() calls sys.exit(), which is expected
                    
                    mock_write.assert_called_once()


if __name__ == '__main__':
    unittest.main()
