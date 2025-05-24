import unittest
import os
import tempfile
from unittest.mock import patch, MagicMock
import parquet_lens
from parquet.ttypes import FileMetaData, SchemaElement, PageType, Encoding
from thrift.transport.TTransport import TMemoryBuffer
from thrift.protocol.TProtocol import TType


class TestCoverageSimple(unittest.TestCase):
    """Simple tests for specific coverage areas"""

    def test_get_thrift_type_name_list_variations(self):
        """Test get_thrift_type_name with various list configurations"""
        # Test with element class having __name__
        class MockClass:
            __name__ = "MockElement"
        
        result = parquet_lens.get_thrift_type_name(TType.LIST, (MockClass, TType.STRUCT))
        self.assertEqual(result, "list<MockElement>")

        # Test legacy format
        legacy_args = ((MockClass,),)
        result = parquet_lens.get_thrift_type_name(TType.LIST, legacy_args)
        self.assertEqual(result, "list<MockElement>")

        # Test primitive list
        result = parquet_lens.get_thrift_type_name(TType.LIST, (TType.STRING,))
        self.assertEqual(result, "list<string>")

    def test_get_enum_class_for_field(self):
        """Test get_enum_class_for_field function"""
        # Test known enum field
        result = parquet_lens.get_enum_class_for_field("type", "SchemaElement")
        self.assertIsNotNone(result)
        
        # Test unknown field
        result = parquet_lens.get_enum_class_for_field("unknown_field", "UnknownStruct")
        self.assertIsNone(result)

    def test_thrift_to_dict_with_complex_structures(self):
        """Test thrift_to_dict_with_offsets with various structures"""
        # Create a mock schema element with complex data
        schema_element = SchemaElement()
        schema_element.name = "test_field"
        schema_element.type = Type.BYTE_ARRAY
        schema_element.converted_type = ConvertedType.UTF8
        
        result = parquet_lens.thrift_to_dict_with_offsets(schema_element, {}, 0, 0, False)
        self.assertIsInstance(result, list)
        self.assertTrue(len(result) > 0)

    def test_thrift_to_dict_with_undefined_optional_fields(self):
        """Test thrift_to_dict_with_offsets with show_undefined_optional=True"""
        schema_element = SchemaElement()
        schema_element.name = "test_field"
        
        result = parquet_lens.thrift_to_dict_with_offsets(schema_element, {}, 0, 0, True)
        self.assertIsInstance(result, list)

    def test_write_output_to_file(self):
        """Test writing output to file"""
        test_data = {"test": "data", "number": 42}
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            temp_file = f.name
        
        try:
            # Test the main function with file output
            with patch('sys.argv', ['script', 'test.parquet', '--output', temp_file]):
                with patch('parquet_lens.analyze_parquet_file') as mock_analyze:
                    mock_analyze.return_value = test_data
                    
                    try:
                        parquet_lens.main()
                    except SystemExit:
                        pass  # Expected
            
            # Verify file exists and has content
            self.assertTrue(os.path.exists(temp_file))
            
            with open(temp_file, 'r') as f:
                content = f.read()
                self.assertIn('test', content)
                
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)

    def test_protocol_with_mock_transport(self):
        """Test OffsetRecordingProtocol with edge cases"""
        # Test with transport that has buffer but no tell method
        class MockTransportNoTell:
            def __init__(self):
                self._buffer = object()  # No tell() method
        
        mock_transport = MockTransportNoTell()
        protocol = parquet_lens.OffsetRecordingProtocol(mock_transport, FileMetaData, 0)
        
        with self.assertRaises(AttributeError):
            protocol._get_trans_pos()


if __name__ == '__main__':
    unittest.main()
