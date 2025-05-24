"""
Parquet Lens: Detailed Parquet File Analyzer

This tool provides byte-level analysis of Parquet files, focusing on the FileMetaData 
Thrift structure and its nested components. It displays the exact byte locations (offset 
ranges) in the Parquet file from which each field originates.

Key Features:
1. **Field Ordering by File Appearance**: Fields are ordered by their actual appearance 
   in the file, not by their Thrift spec order.
2. **Comprehensive Field Metadata**: Each field includes:
   - Field ID (Thrift field identifier)
   - Field name
   - Type information (readable type names like "i32", "list<SchemaElement>")
   - Required/Optional flag
   - Actual value
   - Byte ranges (field_header_range and value_range)
3. **Nested Structure Support**: Nested structures are represented as JSON objects 
   with their own field metadata and ranges.
4. **Offset Tracking**: Records exact byte offsets for field headers and values 
   within the Thrift metadata blob.
5. **Debug Mode**: Optional debug output showing field processing details.

Usage:
    python parquet_lens.py <parquet_file> [--debug]

The output includes:
- File structure overview (magic numbers, data blocks, metadata blob)
- Detailed FileMetaData field analysis with byte-level precision
- Nested field ranges for complex structures like RowGroup, ColumnChunk, etc.
"""

import argparse
import json
import pyarrow.parquet as pq
import sys # Add sys import
import os # Add os import

# Imports for Thrift
from thrift.protocol import TCompactProtocol
from thrift.transport import TTransport
from thrift.protocol.TProtocol import TType # Import TType from TProtocol
from parquet.ttypes import FileMetaData # Only import FileMetaData
# Import enum classes for better enum value conversion
from parquet.ttypes import Type, Encoding, CompressionCodec, ConvertedType, FieldRepetitionType, PageType

# New OffsetRecordingProtocol
class OffsetRecordingProtocol(TCompactProtocol.TCompactProtocol):
    def __init__(self, trans, struct_class, base_offset_in_file): # Changed 'spec' to 'struct_class'
        super().__init__(trans)
        self._struct_class_spec = struct_class.thrift_spec if struct_class else None # Use thrift_spec
        self._base_offset_in_file = base_offset_in_file
        
        self.field_details = {}
        self._current_field_name = None
        self._current_field_id = None
        # self._field_header_start_offset_in_blob = None # Removed
        self._field_total_start_offset_in_blob = None
        # For nested structs
        self._parent_spec_stack = [] 
        self._current_spec = self._struct_class_spec
        # Track nesting level to only record top-level fields
        self._struct_nesting_level = 0

    def _get_trans_pos(self):
        # For TMemoryBuffer, the actual io.BytesIO object is typically in _buffer
        if isinstance(self.trans, TTransport.TMemoryBuffer):
            if hasattr(self.trans, '_buffer') and hasattr(self.trans._buffer, 'tell'):
                return self.trans._buffer.tell()
            else:
                # This case implies a non-standard TMemoryBuffer or an unexpected state.
                raise AttributeError(
                    f"TMemoryBuffer instance at {id(self.trans)} does not have a usable '_buffer' attribute with a 'tell' method."
                )
        # Fallback for other transport types that might have a direct tell() method
        elif hasattr(self.trans, 'tell'):
            return self.trans.tell()
        # If neither, this transport is not supported for position tracking.
        raise TypeError(
            f"Unsupported transport for position tracking: {type(self.trans)}. "
            f"It's not a TMemoryBuffer with a standard '_buffer', nor does it have a 'tell' method."
        )

    def readStructBegin(self):
        # For nested structs, we need to push the current spec and update to the new one.
        # This requires knowing the spec of the field we are about to read.
        # This part is tricky as readStructBegin itself doesn't know which field it belongs to.
        # We will handle spec update in readFieldBegin when a TType.STRUCT is encountered.
        self._struct_nesting_level += 1
        super().readStructBegin()

    def readStructEnd(self):
        super().readStructEnd()
        self._struct_nesting_level -= 1
        # Pop spec when exiting a struct
        if self._parent_spec_stack:
            self._current_spec = self._parent_spec_stack.pop()

    def _get_field_name_from_spec(self, field_id, spec):
        if not spec: return None
        for spec_tuple in spec: # Iterate through spec tuples
            if not spec_tuple: continue # Skip if a tuple is None
            s_fid, s_ftype, s_fname, s_fargs, s_fdefault = spec_tuple # Unpack
            if s_fid == field_id:
                return s_fname
        return None

    def _get_field_spec_from_id(self, field_id, spec):
        if not spec: return None
        for field_spec_tuple in spec:
            if not field_spec_tuple: continue # Skip if a tuple is None
            if field_spec_tuple[0] == field_id: # field_spec_tuple[0] is fid
                return field_spec_tuple
        return None

    def readFieldBegin(self):
        start_of_field_processing_offset = self._get_trans_pos() 
        self._field_total_start_offset_in_blob = start_of_field_processing_offset
        
        fname_ignored, ftype, fid = super().readFieldBegin() # This reads the header
        
        end_of_header_offset = self._get_trans_pos()
        
        self._current_field_id = fid
        self._current_field_name = self._get_field_name_from_spec(fid, self._current_spec)
        if not self._current_field_name:
            self._current_field_name = f"_unknown_field_id_{fid}_"

        # Record offsets for all fields (not just top-level)
        if ftype != TType.STOP:
            field_key = f"{self._struct_nesting_level}_{self._current_field_name}_{fid}"
            if field_key not in self.field_details:
                self.field_details[field_key] = {}
            
            self.field_details[field_key]['field_header_range_in_blob'] = [
                start_of_field_processing_offset, 
                end_of_header_offset
            ]
            self.field_details[field_key]['nesting_level'] = self._struct_nesting_level
            self.field_details[field_key]['field_name'] = self._current_field_name
            self.field_details[field_key]['field_id'] = fid
            
            # Debug output (optional - can be enabled for debugging)
            if hasattr(self, '_debug_enabled') and self._debug_enabled:
                print(f"DEBUG: Level {self._struct_nesting_level} - Field {self._current_field_name} (ID {fid}) - Header range: [{start_of_field_processing_offset}, {end_of_header_offset}] - Key: {field_key}", file=sys.stderr)

        # Handle spec changes for nested structs
        if ftype == TType.STRUCT:
            field_spec_tuple = self._get_field_spec_from_id(fid, self._current_spec)
            if field_spec_tuple and len(field_spec_tuple) > 3 and isinstance(field_spec_tuple[3], tuple) and len(field_spec_tuple[3]) > 0:
                nested_struct_class = field_spec_tuple[3][0]
                if hasattr(nested_struct_class, 'thrift_spec'):
                    self._parent_spec_stack.append(self._current_spec)
                    self._current_spec = nested_struct_class.thrift_spec
        return fname_ignored, ftype, fid

    def readFieldEnd(self):
        if self._current_field_name and self._current_field_id is not None:
            field_key = f"{self._struct_nesting_level}_{self._current_field_name}_{self._current_field_id}"
            if field_key in self.field_details:
                if 'value_range_in_blob' in self.field_details[field_key]:
                    # Field total range is from header start to value end
                    self.field_details[field_key]['field_total_range_in_blob'] = [
                        self._field_total_start_offset_in_blob,
                        self.field_details[field_key]['value_range_in_blob'][1]
                    ]
                elif self._field_total_start_offset_in_blob is not None:
                    # No value recorded, use current position
                    self.field_details[field_key]['field_total_range_in_blob'] = [
                        self._field_total_start_offset_in_blob,
                        self._get_trans_pos()
                    ]

        super().readFieldEnd()
        self._current_field_name = None
        self._current_field_id = None
        self._field_total_start_offset_in_blob = None

    def _read_primitive(self, read_method_name):
        value_start_offset = self._get_trans_pos()
        actual_read_method = getattr(super(), read_method_name)
        value = actual_read_method()
        value_end_offset = self._get_trans_pos()

        if self._current_field_name and self._current_field_id is not None:
            field_key = f"{self._struct_nesting_level}_{self._current_field_name}_{self._current_field_id}"
            if field_key in self.field_details:
                self.field_details[field_key]['value_range_in_blob'] = [
                    value_start_offset, value_end_offset
                ]
                if self._field_total_start_offset_in_blob is not None:
                     self.field_details[field_key]['field_total_range_in_blob'] = [
                        self._field_total_start_offset_in_blob, value_end_offset
                    ]
        return value

    def readString(self): return self._read_primitive('readString')
    def readI32(self): return self._read_primitive('readI32')
    def readI64(self): return self._read_primitive('readI64')
    def readByte(self): return self._read_primitive('readByte')
    def readBool(self): return self._read_primitive('readBool')
    def readDouble(self): return self._read_primitive('readDouble')
    def readBinary(self): return self._read_primitive('readBinary') # Added for TType.STRING when it's binary
    def readI16(self): return self._read_primitive('readI16')


def get_enum_class_for_field(field_name, struct_name):
    """
    Get the appropriate enum class for a field based on field name and struct name.
    This handles cases where the thrift_spec doesn't explicitly specify enum classes.
    """
    enum_mappings = {
        'ColumnMetaData': {
            'type': Type,
            'codec': CompressionCodec,
        },
        'SchemaElement': {
            'type': Type,
            'converted_type': ConvertedType,
            'repetition_type': FieldRepetitionType,
        },
        'PageHeader': {
            'type': PageType,
        },
        'PageEncodingStats': {
            'page_type': PageType,
            'encoding': Encoding,
        }
    }
    
    # For list elements of enums
    list_enum_mappings = {
        'ColumnMetaData': {
            'encodings': Encoding,  # list<Encoding>
        }
    }
    
    if struct_name in enum_mappings and field_name in enum_mappings[struct_name]:
        return enum_mappings[struct_name][field_name]
    
    # For list fields, return the element enum class
    if struct_name in list_enum_mappings and field_name in list_enum_mappings[struct_name]:
        return list_enum_mappings[struct_name][field_name]
        
    return None


def get_thrift_type_name(field_type, type_args):
    """Convert Thrift TType to readable type name"""
    type_map = {
        TType.BOOL: 'bool',
        TType.BYTE: 'i8',
        TType.I16: 'i16', 
        TType.I32: 'i32',
        TType.I64: 'i64',
        TType.DOUBLE: 'double',
        TType.STRING: 'string',
    }
    
    if field_type in type_map:
        return type_map[field_type]
    elif field_type == TType.LIST:
        if type_args and isinstance(type_args, tuple) and len(type_args) > 0:
            element_type = type_args[0]
            if isinstance(element_type, tuple) and len(element_type) > 0:
                # For struct lists like list<SchemaElement>
                if hasattr(element_type[0], '__name__'):
                    return f"list<{element_type[0].__name__}>"
                else:
                    return f"list<{get_thrift_type_name(element_type, None)}>"
            else:
                return f"list<{get_thrift_type_name(element_type, None)}>"
        return "list"
    elif field_type == TType.STRUCT:
        if type_args and isinstance(type_args, tuple) and len(type_args) > 0:
            if hasattr(type_args[0], '__name__'):
                return type_args[0].__name__
        return "struct"
    else:
        return f"unknown_type_{field_type}"


def thrift_to_dict_with_offsets(thrift_obj, field_details_map, base_offset_in_file, nesting_level=1, show_undefined_optional=False):
    """
    Recursively converts a Thrift object to a list or dict of field data,
    with detailed offset information and type annotations.
    """
    if thrift_obj is None:
        return None

    if isinstance(thrift_obj, list):
        return [thrift_to_dict_with_offsets(i, field_details_map, base_offset_in_file, nesting_level + 1, show_undefined_optional) for i in thrift_obj]

    if not hasattr(thrift_obj, '__dict__'): # Base types, enums
        if isinstance(thrift_obj, bytes):
            try:
                return thrift_obj.decode('utf-8')
            except UnicodeDecodeError:
                return thrift_obj.hex()
        return thrift_obj

    temp_fields_data = []
    obj_thrift_spec = getattr(thrift_obj, 'thrift_spec', None)

    if not obj_thrift_spec:
        # Fallback for objects without thrift_spec
        result = {}
        for attr_name, attr_value in thrift_obj.__dict__.items():
            if attr_name.startswith('_'): continue
            result[attr_name] = thrift_to_dict_with_offsets(attr_value, field_details_map, base_offset_in_file, nesting_level + 1, show_undefined_optional)
        return result

    for index, spec_tuple in enumerate(obj_thrift_spec):
        if not spec_tuple: continue
        
        field_id = spec_tuple[0]
        field_type = spec_tuple[1]
        field_name = spec_tuple[2]
        type_args = spec_tuple[3] if len(spec_tuple) > 3 else None
        default_value = spec_tuple[4] if len(spec_tuple) > 4 else None
        
        field_value = getattr(thrift_obj, field_name, None)
        processed_value = None
        is_enum = False

        # Process the field value based on its type
        if field_type == TType.LIST:
            processed_value = []
            if field_value:
                element_type_meta = type_args[0] if type_args and isinstance(type_args, tuple) else None
                element_enum_class = None
                element_is_enum = False
                
                # Check if list elements are enums from type_args
                if element_type_meta and isinstance(element_type_meta, tuple) and len(element_type_meta) == 1 and hasattr(element_type_meta[0], '_VALUES_TO_NAMES'):
                    element_enum_class = element_type_meta[0]
                    element_is_enum = True
                else:
                    # Try to find enum class for list elements based on field name and struct type
                    struct_name = type(thrift_obj).__name__ if hasattr(thrift_obj, '__class__') else 'Unknown'
                    element_enum_class = get_enum_class_for_field(field_name, struct_name)
                    if element_enum_class and hasattr(element_enum_class, '_VALUES_TO_NAMES'):
                        element_is_enum = True
                
                for item in field_value:
                    if element_is_enum and element_enum_class:
                        processed_value.append(element_enum_class._VALUES_TO_NAMES.get(item, item))
                    else:
                        processed_value.append(thrift_to_dict_with_offsets(item, field_details_map, base_offset_in_file, nesting_level + 1, show_undefined_optional))
            elif field_value is None:
                processed_value = None
        elif field_type == TType.STRUCT:
            processed_value = thrift_to_dict_with_offsets(field_value, field_details_map, base_offset_in_file, nesting_level + 1, show_undefined_optional)
        elif isinstance(field_value, bytes):
            try:
                processed_value = field_value.decode('utf-8')
            except UnicodeDecodeError:
                processed_value = field_value.hex()
        else:
            # Handle enums and primitive types
            processed_value = None
            is_enum = False
            
            # Handle enums - first check if type_args specifies an enum class
            enum_class = None
            if type_args and isinstance(type_args, tuple) and len(type_args) == 1 and hasattr(type_args[0], '_VALUES_TO_NAMES'):
                enum_class = type_args[0]
                is_enum = True
            else:
                # Try to find enum class based on field name and struct type
                struct_name = type(thrift_obj).__name__ if hasattr(thrift_obj, '__class__') else 'Unknown'
                enum_class = get_enum_class_for_field(field_name, struct_name)
                if enum_class and hasattr(enum_class, '_VALUES_TO_NAMES'):
                    is_enum = True
            
            if is_enum and enum_class:
                processed_value = enum_class._VALUES_TO_NAMES.get(field_value, field_value) if field_value is not None else None
            else:
                processed_value = field_value

        # Determine if field is required or optional from the thrift file
        # Look for "required" or "optional" in the thrift spec
        # The default_value being None doesn't always indicate required
        required = False
        # Check the thrift file structure - if we have the spec, we should parse it properly
        # For now, let's use a simple heuristic: if there's no default and it's not explicitly optional
        # We can improve this by parsing the actual thrift IDL
        
        # Common required fields we know from the thrift file
        known_required_fields = {
            'FileMetaData': {'version', 'schema', 'num_rows', 'row_groups'},
            'SchemaElement': {'name'},
            'RowGroup': {'columns', 'total_byte_size', 'num_rows'},
            'ColumnChunk': {'file_path', 'file_offset', 'meta_data'},
            'ColumnMetaData': {'type', 'encodings', 'path_in_schema', 'codec', 'num_values', 'total_uncompressed_size', 'total_compressed_size', 'data_page_offset'}
        }
        
        struct_name = type(thrift_obj).__name__ if hasattr(thrift_obj, '__class__') else 'Unknown'
        if struct_name in known_required_fields:
            required = field_name in known_required_fields[struct_name]
        else:
            # Default heuristic: assume optional unless we know it's required
            required = False

        # Skip undefined optional fields if show_undefined_optional is False
        if not show_undefined_optional and not required and processed_value is None:
            continue

        field_data = {
            "field_id": field_id,
            "field_name": field_name,
            "type": get_thrift_type_name(field_type, type_args),
            "value": processed_value,
            "original_index": index  # For sorting
        }

        # Only show "required": true when fields are actually required
        if required:
            field_data["required"] = True

        # Add offset information for this field if available
        field_key = f"{nesting_level}_{field_name}_{field_id}"
        if field_key in field_details_map:
            details = field_details_map[field_key]
            if 'field_header_range_in_blob' in details and details['field_header_range_in_blob']:
                blob_range = details['field_header_range_in_blob']
                field_data['field_header_range'] = [
                    blob_range[0] + base_offset_in_file,
                    blob_range[1] + base_offset_in_file
                ]
            if 'value_range_in_blob' in details and details['value_range_in_blob']:
                blob_range = details['value_range_in_blob']
                field_data['value_range'] = [
                    blob_range[0] + base_offset_in_file,
                    blob_range[1] + base_offset_in_file
                ]
        
        # For nested structures, recursively add offset information
        if field_type == TType.STRUCT and processed_value and isinstance(processed_value, dict):
            for nested_field_name, nested_field_data in processed_value.items():
                if isinstance(nested_field_data, dict) and 'field_id' in nested_field_data:
                    nested_field_id = nested_field_data['field_id']
                    nested_field_key = f"{nesting_level + 1}_{nested_field_name}_{nested_field_id}"
                    if nested_field_key in field_details_map:
                        nested_details = field_details_map[nested_field_key]
                        if 'field_header_range_in_blob' in nested_details and nested_details['field_header_range_in_blob']:
                            blob_range = nested_details['field_header_range_in_blob']
                            nested_field_data['field_header_range'] = [
                                blob_range[0] + base_offset_in_file,
                                blob_range[1] + base_offset_in_file
                            ]
                        if 'value_range_in_details' in nested_details and nested_details['value_range_in_blob']:
                            blob_range = nested_details['value_range_in_blob']
                            nested_field_data['value_range'] = [
                                blob_range[0] + base_offset_in_file,
                                blob_range[1] + base_offset_in_file
                            ]
        elif field_type == TType.LIST and processed_value and isinstance(processed_value, list):
            # For lists of structs, add offset information to each struct element
            for i, list_item in enumerate(processed_value):
                if isinstance(list_item, dict):
                    for nested_field_name, nested_field_data in list_item.items():
                        if isinstance(nested_field_data, dict) and 'field_id' in nested_field_data:
                            nested_field_id = nested_field_data['field_id']
                            # For list elements, we need to find the right offset key
                            # This is more complex as we'd need to track list indices in our offset recording
                            # For now, let's just handle the simpler struct case above
                            pass
        
        temp_fields_data.append(field_data)

    # Sort fields by their file appearance (offset-based) for top-level, otherwise by thrift_spec order
    if nesting_level == 1:  # Top-level fields
        def sort_key(field_item):
            # Primary: presence of offset (0 if present, 1 if not)
            has_offset = 'field_header_range' in field_item

            # Secondary: starting offset of field header
            start_offset = float('inf')
            if 'field_header_range' in field_item and field_item['field_header_range']:
                start_offset = field_item['field_header_range'][0]
            elif 'value_range' in field_item and field_item['value_range']:
                start_offset = field_item['value_range'][0]

            # Tertiary: original thrift_spec index
            original_idx = field_item['original_index']
            
            return (0 if has_offset else 1, start_offset, original_idx)

        temp_fields_data.sort(key=sort_key)

    # Remove the temporary sorting key and return as list for top-level, dict for nested
    result_fields = []
    for item in temp_fields_data:
        item_copy = item.copy()
        del item_copy['original_index']
        result_fields.append(item_copy)
        
    # For nested structs (not lists), return as dict instead of list
    if nesting_level > 1 and not isinstance(thrift_obj, list):
        result_dict = {}
        for field in result_fields:
            result_dict[field['field_name']] = field
        return result_dict
        
    return result_fields


def analyze_parquet_file(file_path, debug=False, show_undefined_optional=False):
    """
    Analyzes a Parquet file and returns a detailed byte-by-byte mapping.
    """
    segments = []
    current_offset = 0

    try:
        with open(file_path, 'rb') as f:
            # 1. Read Magic Number (PAR1) - 4 bytes
            magic = f.read(4)
            if magic == b'PAR1':
                segments.append({
                    "range": [current_offset, current_offset + 4],
                    "type": "magic_number",
                    "value": "PAR1",
                    "description": "Parquet magic number"
                })
                current_offset += 4
            else:
                segments.append({
                    "range": [current_offset, current_offset + len(magic)],
                    "type": "error",
                    "description": f"Invalid Parquet magic number. Expected PAR1, got {magic!r}",
                    "value": magic.hex()
                })
                # Potentially stop early or try to find footer
                # For now, we'll continue to see if we can find the footer
                current_offset += len(magic)


            # The core challenge is to map everything between the initial PAR1
            # and the final PAR1 + footer_length + FileMetaData.
            # This involves understanding row groups, column chunks, page headers,
            # and the data itself. PyArrow can help parse the metadata,
            # but we need to correlate its findings with byte offsets.

            # For now, let's try to locate and parse the FileMetaData
            # The Parquet file structure is:
            # PAR1
            # <Data (Row Groups -> Column Chunks -> Pages)>
            # <FileMetaData (Thrift)>
            # <Footer Length (4 bytes, int32)>
            # PAR1

            # To find FileMetaData, we read the last 8 bytes:
            # 4 bytes for footer length, 4 bytes for magic number
            f.seek(-8, 2) # Seek from the end of the file
            footer_len_bytes = f.read(4)
            footer_magic = f.read(4)

            if footer_magic == b'PAR1':
                footer_length = int.from_bytes(footer_len_bytes, byteorder='little', signed=False)
                segments.append({
                    "range": [f.tell() - 4, f.tell()], # range of PAR1
                    "type": "magic_number",
                    "value": "PAR1",
                    "description": "Footer magic number"
                })
                segments.append({
                    "range": [f.tell() - 8, f.tell() - 4], # range of footer_length
                    "type": "footer_length",
                    "value": footer_length,
                    "description": "Length of the FileMetaData (Thrift) structure"
                })

                # Now read the FileMetaData
                metadata_offset = f.tell() - 8 - footer_length
                f.seek(metadata_offset)
                metadata_bytes = f.read(footer_length)

                # Parse metadata_bytes using Thrift with offset recording
                parsed_metadata_dict = None
                raw_field_details = {} # To store details from OffsetRecordingProtocol

                try:
                    transport = TTransport.TMemoryBuffer(metadata_bytes)
                    protocol = OffsetRecordingProtocol(transport, FileMetaData, metadata_offset)
                    
                    # Enable debug output in the protocol if requested
                    protocol._debug_enabled = debug
                    
                    file_metadata_obj = FileMetaData()
                    file_metadata_obj.read(protocol)

                    raw_field_details = protocol.field_details
                    parsed_metadata_fields = thrift_to_dict_with_offsets(
                        file_metadata_obj, 
                        raw_field_details,
                        metadata_offset,
                        show_undefined_optional=show_undefined_optional
                    )
                    
                    # Directly use fields or error info for the segment
                    segment_data_for_thrift_blob = {
                        "range": [metadata_offset, metadata_offset + footer_length],
                        "type": "thrift_metadata_blob",
                        "thrift_type": "FileMetaData",
                        "description": "FileMetaData Thrift structure with field offsets",
                        "size": footer_length,
                        "fields": parsed_metadata_fields 
                    }

                except Exception as e:
                    error_details_for_json = {k: str(v) for k, v in raw_field_details.items()}
                    segment_data_for_thrift_blob = {
                        "range": [metadata_offset, metadata_offset + footer_length],
                        "type": "thrift_metadata_blob",
                        "thrift_type": "FileMetaData",
                        "description": "FileMetaData Thrift structure with field offsets",
                        "size": footer_length,
                        "error": f"Thrift deserialization or offset processing failed: {str(e)}",
                        "raw_field_details_on_error": error_details_for_json
                    }
                
                segments.append(segment_data_for_thrift_blob)

                # Placeholder for the data part
                # The data part is between the first PAR1 and the FileMetaData
                if segments[0]["value"] == "PAR1": # Check if initial magic was OK
                    data_start_offset = segments[0]["range"][1]
                    data_end_offset = metadata_offset
                    if data_end_offset > data_start_offset:
                        segments.insert(1, { # Insert after the first magic number
                            "range": [data_start_offset, data_end_offset],
                            "type": "data_block_unparsed",
                            "description": "Represents all row groups, column chunks, and pages. Needs detailed parsing.",
                            "size": data_end_offset - data_start_offset
                        })


            else:
                segments.append({
                    "range": [f.tell() - len(footer_magic), f.tell()],
                    "type": "error",
                    "description": f"Invalid Parquet magic number at footer. Expected PAR1, got {footer_magic!r}",
                    "value": footer_magic.hex()
                })


            # Sort segments by start offset to ensure correct order if insertions happened out of place
            segments.sort(key=lambda s: s["range"][0])

            # TODO: Fill gaps and identify unknown segments
            # TODO: Detailed Thrift parsing for FileMetaData and PageHeaders
            # TODO: Correlate with pyarrow.ParquetFile metadata for higher-level structure

    except FileNotFoundError:
        return {"error": f"File not found: {file_path}"}
    except Exception as e:
        return {"error": f"An error occurred: {str(e)}", "segments": segments}

    return {"segments": segments}

def main():
    parser = argparse.ArgumentParser(description="Parquet Lens: Detailed Parquet file analyzer.")
    parser.add_argument("parquet_file", help="Path to the Parquet file to analyze.")
    parser.add_argument("--debug", action="store_true", help="Enable debug output showing field processing details.")
    parser.add_argument("--show-undefined-optional-fields", action="store_true", 
                        help="Show optional fields even when they are undefined/null (default: hide them)")
    args = parser.parse_args()

    analysis_result = analyze_parquet_file(args.parquet_file, debug=args.debug, 
                                         show_undefined_optional=args.show_undefined_optional_fields)
    print(json.dumps(analysis_result, indent=4))

if __name__ == "__main__":
    main()
