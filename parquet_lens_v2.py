"""
Parquet Lens V2: Improved Consistent Output Format

This version redesigns the output format to use a consistent recursive segment structure
where all segments have `range`, `type`, and `value` properties, with proper handling of
nested Thrift structures as child segments.

Key Improvements:
1. **Consistent Segment Structure**: All segments follow the pattern:
   - `range`: [start_offset, end_offset] 
   - `type`: Clear type taxonomy (container, primitive, semantic)
   - `value`: Appropriately structured value
   - `children`: Optional array of child segments for containers

2. **Clear Type Taxonomy**:
   - File structure types: "file_magic", "metadata_blob", "data_block", "footer_length"
   - Thrift container types: "thrift_struct", "thrift_list", "thrift_field"
   - Primitive types: "i32", "i64", "string", "binary", "bool"
   - Enum types: "enum" with enum_class information

3. **Recursive Child Structure**: Complex types contain child segments rather than flat field arrays

4. **Consistent Metadata**: Technical details like endianness, encoding in separate metadata field
"""

import argparse
import json
import sys
import os
import struct

# Imports for Thrift
from thrift.protocol import TBinaryProtocol, TCompactProtocol
from thrift.transport import TTransport
from thrift.transport.TTransport import TMemoryBuffer
from thrift.protocol.TProtocol import TType
import parquet.ttypes as parquet
from parquet.ttypes import FileMetaData
# Import enum classes for better enum value conversion
from parquet.ttypes import Type, Encoding, CompressionCodec, ConvertedType, FieldRepetitionType, PageType

def make_field_key(nesting_level, field_name, field_id):
    """Create a standardized key for field lookup"""
    return f"{nesting_level}_{field_name}_{field_id}"

# Enhanced OffsetRecordingProtocol (reusing existing implementation)
class OffsetRecordingProtocol(TCompactProtocol.TCompactProtocol):
    def __init__(self, trans, struct_class=None, base_offset_in_file=0):
        super().__init__(trans)
        
        # Add spec tracking from V1
        self._struct_class_spec = struct_class.thrift_spec if struct_class else None
        self._current_spec = self._struct_class_spec
        self._base_offset_in_file = base_offset_in_file
        
        print(f"DEBUG: Initializing protocol with struct_class={struct_class.__name__ if struct_class else None}, spec_length={len(self._current_spec) if self._current_spec else 0}", file=sys.stderr)
        if self._current_spec and len(self._current_spec) > 0:
            # Show first few field IDs for debugging
            field_specs = [(i, spec[0] if spec and len(spec) > 0 else None, spec[2] if spec and len(spec) > 2 else None) for i, spec in enumerate(self._current_spec) if spec]
            print(f"DEBUG: Field specs (index, field_id, field_name): {field_specs}", file=sys.stderr)
        
        self.field_details = {}
        self._current_field_name = None
        self._current_field_id = None
        self._field_total_start_offset_in_blob = None
        # For nested structs
        self._parent_spec_stack = [] 
        # Track nesting level to only record top-level fields
        self._struct_nesting_level = 0
        
        # For list elements containing structs
        self._list_element_struct_spec = None
        self._list_element_struct_class = None
        
        # Stack to track field context during complex type reading
        self._field_context_stack = []
        
        # Field path for more reliable identification
        self._current_field_path = []

    def _get_trans_pos(self):
        # For TMemoryBuffer, the actual io.BytesIO object is typically in _buffer
        if isinstance(self.trans, TTransport.TMemoryBuffer):
            if hasattr(self.trans, '_buffer') and hasattr(self.trans._buffer, 'tell'):
                return self.trans._buffer.tell()
            else:
                raise AttributeError(
                    f"TMemoryBuffer instance at {id(self.trans)} does not have a usable '_buffer' attribute with a 'tell' method."
                )
        elif hasattr(self.trans, 'tell'):
            return self.trans.tell()
        raise TypeError(
            f"Unsupported transport for position tracking: {type(self.trans)}. "
            f"It's not a TMemoryBuffer with a standard '_buffer', nor does it have a 'tell' method."
        )

    def readStructBegin(self):
        if self._current_field_name and self._current_field_id is not None:
            field_key = make_field_key(self._struct_nesting_level, self._current_field_name, self._current_field_id)
            if field_key in self.field_details:
                value_start_offset = self._get_trans_pos()
                self.field_details[field_key]['value_start_offset'] = value_start_offset
            
            # Update field path for tracking
            self._current_field_path.append(self._current_field_name)

        # Push the current field context onto stack when entering a new struct
        if self._current_field_name is not None or self._current_field_id is not None:
            self._field_context_stack.append((self._current_field_name, self._current_field_id, self._struct_nesting_level))
            
        self._struct_nesting_level += 1
        result = super().readStructBegin()
        return result

    def readStructEnd(self):
        result = super().readStructEnd()
        self._struct_nesting_level -= 1
        
        # Restore parent spec when exiting a struct
        if self._parent_spec_stack:
            old_spec = self._current_spec
            self._current_spec = self._parent_spec_stack.pop()
            print(f"DEBUG: Restored parent spec at level {self._struct_nesting_level}", file=sys.stderr)
            
        # Restore the parent field context when exiting a struct
        if self._field_context_stack:
            self._current_field_name, self._current_field_id, _ = self._field_context_stack.pop()
            # Pop from field path
            if self._current_field_path:
                self._current_field_path.pop()
        else:
            self._current_field_name = None
            self._current_field_id = None
            self._current_field_path = []
            
        return result
        
    def _get_struct_type_for_field(self, field_name, spec):
        """Try to determine the struct type for a field"""
        if not spec:
            return None
            
        for field_spec in spec:
            if not field_spec or len(field_spec) < 4:
                continue
                
            field_id, field_type, spec_field_name, type_args = field_spec[:4]
            
            if spec_field_name == field_name and field_type == TType.STRUCT and type_args:
                # The type_args for a struct is typically the struct class
                return type_args
                
        return None

    def readFieldBegin(self):
        fname_ignored, field_type, field_id = super().readFieldBegin()
        
        print(f"DEBUG: readFieldBegin got fname={fname_ignored}, field_type={field_type}, field_id={field_id}", file=sys.stderr)
        
        # Record the start of the field (header part)
        if field_type != TType.STOP:
            current_position = self._get_trans_pos() - 1  # Subtract 1 because we already read the field type byte
            
            # Look up field name from thrift spec (like V1)
            field_name = self._find_field_name_from_spec(field_id, self._current_spec)
            if not field_name:
                field_name = f"field_{field_id}"
            
            # Debug output for field name resolution  
            print(f"DEBUG: Field ID {field_id} (type={field_type}, TType.STRUCT={TType.STRUCT}) resolved to '{field_name}' at level {self._struct_nesting_level} (spec: {type(self._current_spec).__name__ if hasattr(self._current_spec, '__name__') else 'list' if self._current_spec else 'None'})", file=sys.stderr)
            
            # Create a key for storing details
            field_key = make_field_key(self._struct_nesting_level, field_name, field_id)
            
            # Create a path-based key for more reliable lookup
            path_key = "_".join(self._current_field_path + [field_name]) if self._current_field_path else field_name
            full_path_key = f"{self._struct_nesting_level}_{path_key}_{field_id}"
            
            # Initialize field details
            field_detail_entry = {
                'field_header_start_offset': current_position,
                'field_name': field_name,
                'field_id': field_id,
                'field_type': field_type,
                'nesting_level': self._struct_nesting_level,
                'field_path': list(self._current_field_path + [field_name]),
                'path_key': full_path_key
            }
            
            # Store both with level-name-id key and path-based key
            self.field_details[field_key] = field_detail_entry
            self.field_details[full_path_key] = field_detail_entry
            
            # Store current field context for value reading
            self._current_field_name = field_name
            self._current_field_id = field_id
            
            # Handle spec changes for nested structs
            print(f"DEBUG: Checking field_type {field_type} - TType.STRUCT={TType.STRUCT}, TType.LIST={TType.LIST}", file=sys.stderr)
            if field_type == TType.STRUCT:
                print(f"DEBUG: Found STRUCT field {field_name} (ID {field_id}) at level {self._struct_nesting_level}", file=sys.stderr)
                
                # Check if we're reading a struct element within a list
                if self._list_element_struct_spec and self._list_element_struct_class:
                    print(f"DEBUG: Using list element spec for struct: {self._list_element_struct_class.__name__}", file=sys.stderr)
                    # Push current spec onto stack before switching to list element spec
                    self._parent_spec_stack.append(self._current_spec)
                    self._current_spec = self._list_element_struct_spec
                    print(f"DEBUG: Switching to list element spec for {self._list_element_struct_class.__name__} at level {self._struct_nesting_level}", file=sys.stderr)
                else:
                    # Handle direct struct fields: type_args = [struct_class, struct_spec]
                    field_spec_tuple = self._get_field_spec_from_id(field_id, self._current_spec)
                    print(f"DEBUG: Field spec tuple: {field_spec_tuple}", file=sys.stderr)
                    
                    if field_spec_tuple and len(field_spec_tuple) > 3 and isinstance(field_spec_tuple[3], list) and len(field_spec_tuple[3]) > 0:
                        nested_struct_class = field_spec_tuple[3][0]
                        print(f"DEBUG: Found direct struct class: {nested_struct_class}", file=sys.stderr)
                        if hasattr(nested_struct_class, 'thrift_spec'):
                            # Push current spec onto stack before switching to nested spec
                            self._parent_spec_stack.append(self._current_spec)
                            self._current_spec = nested_struct_class.thrift_spec
                            print(f"DEBUG: Switching to spec for {nested_struct_class.__name__} at level {self._struct_nesting_level}", file=sys.stderr)
                        else:
                            print(f"DEBUG: Nested struct class {nested_struct_class} has no thrift_spec", file=sys.stderr)
                    else:
                        print(f"DEBUG: Could not extract nested struct class from field spec", file=sys.stderr)
                    
            elif field_type == TType.LIST:
                print(f"DEBUG: Found LIST field {field_name} (ID {field_id}) at level {self._struct_nesting_level}", file=sys.stderr)
                field_spec_tuple = self._get_field_spec_from_id(field_id, self._current_spec)
                print(f"DEBUG: LIST field spec tuple: {field_spec_tuple}", file=sys.stderr)
                
                # Handle list fields containing structs: type_args = (element_type, [struct_class, struct_spec], is_binary)
                if (field_spec_tuple and len(field_spec_tuple) > 3 and isinstance(field_spec_tuple[3], tuple) and 
                    len(field_spec_tuple[3]) >= 2 and field_spec_tuple[3][0] == TType.STRUCT and 
                    isinstance(field_spec_tuple[3][1], list) and len(field_spec_tuple[3][1]) > 0):
                    nested_struct_class = field_spec_tuple[3][1][0]
                    print(f"DEBUG: Found list element struct class: {nested_struct_class}", file=sys.stderr)
                    if hasattr(nested_struct_class, 'thrift_spec'):
                        # Store the list element struct spec for when we read list elements
                        self._list_element_struct_spec = nested_struct_class.thrift_spec
                        self._list_element_struct_class = nested_struct_class
                        print(f"DEBUG: Set list element spec for {nested_struct_class.__name__}", file=sys.stderr)
                    else:
                        print(f"DEBUG: List element struct class {nested_struct_class} has no thrift_spec", file=sys.stderr)
                else:
                    print(f"DEBUG: List field does not contain structs", file=sys.stderr)
        
        return fname_ignored, field_type, field_id

    def _find_field_name_from_spec(self, field_id, spec):
        """Find field name from thrift spec"""
        if spec:
            for field_spec in spec:
                if field_spec and len(field_spec) >= 3 and field_spec[0] == field_id:
                    return field_spec[2]  # field name is typically at index 2
        # Return generic field name for unknown field IDs
        return f'field_{field_id}'

    def _get_field_spec_from_id(self, field_id, spec):
        """Find field spec from thrift spec"""
        if spec:
            for field_spec in spec:
                if field_spec and len(field_spec) >= 1 and field_spec[0] == field_id:
                    return field_spec
        return None

    def readFieldEnd(self):
        # Record end position after reading field value
        if self._current_field_name and self._current_field_id is not None:
            field_key = make_field_key(self._struct_nesting_level, self._current_field_name, self._current_field_id)
            
            # Also check path-based key
            path_key = "_".join(self._current_field_path + [self._current_field_name]) if self._current_field_path else self._current_field_name
            full_path_key = f"{self._struct_nesting_level}_{path_key}_{self._current_field_id}"
            
            # Get the correct entry (either by field_key or full_path_key)
            detail_entry = self.field_details.get(field_key) or self.field_details.get(full_path_key)
            
            if detail_entry:
                current_position = self._get_trans_pos()
                detail_entry['field_end_offset'] = current_position
                
                # Calculate ranges
                start_offset = detail_entry['field_header_start_offset']
                value_start = detail_entry.get('value_start_offset', start_offset + 1)
                
                # Store ranges relative to blob start
                detail_entry['field_header_range_in_blob'] = [start_offset, value_start]
                detail_entry['value_range_in_blob'] = [value_start, current_position]
                
                # Update both entries with same information
                if field_key in self.field_details and full_path_key in self.field_details and field_key != full_path_key:
                    self.field_details[field_key] = detail_entry
                    self.field_details[full_path_key] = detail_entry
        
        result = super().readFieldEnd()
        self._current_field_name = None
        self._current_field_id = None
        return result

    def readListBegin(self):
        elem_type, size = super().readListBegin()
        
        # If we're currently in a field context, record the list start position
        if self._current_field_name and self._current_field_id is not None:
            field_key = make_field_key(self._struct_nesting_level, self._current_field_name, self._current_field_id)
            
            # Also check path-based key
            path_key = "_".join(self._current_field_path + [self._current_field_name]) if self._current_field_path else self._current_field_name
            full_path_key = f"{self._struct_nesting_level}_{path_key}_{self._current_field_id}"
            
            # Get the correct entry
            detail_entry = self.field_details.get(field_key) or self.field_details.get(full_path_key)
            
            if detail_entry:
                list_start = self._get_trans_pos()
                if 'list_details' not in detail_entry:
                    detail_entry['list_details'] = {
                        'start': list_start,
                        'elem_type': elem_type,
                        'size': size,
                        'elements': []
                    }
                
                # Push the current field context to stack for nested tracking
                self._field_context_stack.append((self._current_field_name, self._current_field_id, self._struct_nesting_level))
                
                # Update both entries with same information
                if field_key in self.field_details and full_path_key in self.field_details and field_key != full_path_key:
                    self.field_details[field_key] = detail_entry
                    self.field_details[full_path_key] = detail_entry
        
        return elem_type, size
    
    def readListEnd(self):
        result = super().readListEnd()
        
        # Clear list element struct spec when ending a list
        if self._list_element_struct_spec:
            print(f"DEBUG: Clearing list element struct spec for {self._list_element_struct_class.__name__ if self._list_element_struct_class else 'None'}", file=sys.stderr)
            self._list_element_struct_spec = None
            self._list_element_struct_class = None
        
        # When ending a list, record the end position in field details
        if self._field_context_stack:
            prev_field_name, prev_field_id, prev_level = self._field_context_stack.pop()
            field_key = make_field_key(prev_level, prev_field_name, prev_field_id)
            
            # Also try path key
            path_elements = self._current_field_path + [prev_field_name] if self._current_field_path else [prev_field_name]
            path_key = "_".join(path_elements)
            full_path_key = f"{prev_level}_{path_key}_{prev_field_id}"
            
            # Get the correct entry
            detail_entry = self.field_details.get(field_key) or self.field_details.get(full_path_key)
            
            if detail_entry and 'list_details' in detail_entry:
                list_end = self._get_trans_pos()
                detail_entry['list_details']['end'] = list_end
                
                # Update value range to include the list
                if 'value_range_in_blob' in detail_entry:
                    current_range = detail_entry['value_range_in_blob']
                    detail_entry['value_range_in_blob'] = [
                        current_range[0],
                        max(current_range[1], list_end)
                    ]
                    
                # Update both entries with same information
                if field_key in self.field_details and full_path_key in self.field_details and field_key != full_path_key:
                    self.field_details[field_key] = detail_entry
                    self.field_details[full_path_key] = detail_entry
        
        return result

    def readMapBegin(self):
        key_type, val_type, size = super().readMapBegin()
        
        # Track map position similar to list
        if self._current_field_name and self._current_field_id is not None:
            field_key = make_field_key(self._struct_nesting_level, self._current_field_name, self._current_field_id)
            
            # Path key
            path_key = "_".join(self._current_field_path + [self._current_field_name]) if self._current_field_path else self._current_field_name
            full_path_key = f"{self._struct_nesting_level}_{path_key}_{self._current_field_id}"
            
            # Get entry
            detail_entry = self.field_details.get(field_key) or self.field_details.get(full_path_key)
            
            if detail_entry:
                map_start = self._get_trans_pos()
                if 'map_details' not in detail_entry:
                    detail_entry['map_details'] = {
                        'start': map_start,
                        'key_type': key_type,
                        'val_type': val_type,
                        'size': size
                    }
                
                # Push the current field context to stack for nested tracking
                self._field_context_stack.append((self._current_field_name, self._current_field_id, self._struct_nesting_level))
                
                # Update both entries
                if field_key in self.field_details and full_path_key in self.field_details and field_key != full_path_key:
                    self.field_details[field_key] = detail_entry
                    self.field_details[full_path_key] = detail_entry
                
        return key_type, val_type, size
        
    def readMapEnd(self):
        result = super().readMapEnd()
        
        # When ending a map, record the end position in field details
        if self._field_context_stack:
            prev_field_name, prev_field_id, prev_level = self._field_context_stack.pop()
            field_key = make_field_key(prev_level, prev_field_name, prev_field_id)
            
            # Path key
            path_elements = self._current_field_path + [prev_field_name] if self._current_field_path else [prev_field_name]
            path_key = "_".join(path_elements)
            full_path_key = f"{prev_level}_{path_key}_{prev_field_id}"
            
            # Get entry
            detail_entry = self.field_details.get(field_key) or self.field_details.get(full_path_key)
            
            if detail_entry and 'map_details' in detail_entry:
                map_end = self._get_trans_pos()
                detail_entry['map_details']['end'] = map_end
                
                # Update value range to include the map
                if 'value_range_in_blob' in detail_entry:
                    current_range = detail_entry['value_range_in_blob']
                    detail_entry['value_range_in_blob'] = [
                        current_range[0],
                        max(current_range[1], map_end)
                    ]
                    
                # Update both entries
                if field_key in self.field_details and full_path_key in self.field_details and field_key != full_path_key:
                    self.field_details[field_key] = detail_entry
                    self.field_details[full_path_key] = detail_entry
        
        return result
def get_enum_class_for_field(field_name, struct_name):
    """Map field names to their corresponding enum classes"""
    enum_mapping = {
        ('type', 'ColumnMetaData'): Type,
        ('type', 'SchemaElement'): Type,
        ('repetition_type', 'SchemaElement'): FieldRepetitionType,
        ('converted_type', 'SchemaElement'): ConvertedType,
        ('codec', 'ColumnMetaData'): CompressionCodec,
        ('page_type', 'PageEncodingStats'): PageType,
        ('encoding', 'PageEncodingStats'): Encoding,
    }
    
    # Handle list types
    for key in ['encodings', 'encoding']:
        if field_name == key:
            return Encoding
    
    return enum_mapping.get((field_name, struct_name))

def get_thrift_type_name(field_type, type_args):
    """Convert Thrift TType to readable type name"""
    type_map = {
        TType.BOOL: "bool",
        TType.BYTE: "i8", 
        TType.I16: "i16",
        TType.I32: "i32",
        TType.I64: "i64",
        TType.DOUBLE: "double",
        TType.STRING: "string",
        TType.STRUCT: "struct",
        TType.MAP: "map",
        TType.SET: "set",
        TType.LIST: "list"
    }
    
    base_type = type_map.get(field_type, f"unknown_{field_type}")
    
    if field_type == TType.LIST and type_args:
        element_type = get_thrift_type_name(type_args[0], type_args[1:] if len(type_args) > 1 else None)
        return f"list<{element_type}>"
    
    return base_type

def create_segment(range_start, range_end, segment_type, value=None, children=None, metadata=None):
    """Create a standardized segment with consistent structure"""
    segment = {
        "range": [range_start, range_end],
        "type": segment_type,
        "value": value
    }
    
    if children:
        segment["children"] = children
    
    if metadata:
        segment["metadata"] = metadata
        
    return segment

def thrift_to_segments(thrift_obj, field_details_map, base_offset_in_file, nesting_level=1, show_undefined_optional=False, debug=False, field_path=None):
    """
    Convert Thrift object to hierarchical segment structure
    """
    if field_path is None:
        field_path = []
    if not hasattr(thrift_obj, 'thrift_spec') or not thrift_obj.thrift_spec:
        return []
    
    segments = []
    
    for index, field_spec in enumerate(thrift_obj.thrift_spec):
        if not field_spec:
            continue
            
        # field_id, field_type, field_name, type_args, default_value, required = field_spec[:6]
        if len(field_spec) == 6:
            field_id, field_type, field_name, type_args, default_value, required = field_spec
        elif len(field_spec) == 5:
            field_id, field_type, field_name, type_args, default_value = field_spec
            required = False # Default for optional fields when not specified
        else:
            # Handle unexpected field_spec length, perhaps log an error or skip
            if debug:
                print(f"Skipping field_spec with unexpected length: {field_spec}", file=sys.stderr)
            continue
        
        # Get field value
        field_value = getattr(thrift_obj, field_name, None)
        
        # Skip undefined optional fields if requested
        if not show_undefined_optional and not required and field_value is None:
            continue
        
        # Find the field detail using multiple lookup strategies
        field_detail = find_field_detail(field_details_map, field_name, field_id, nesting_level, debug, field_path)
        
        # Calculate absolute ranges
        if field_detail:
            header_range = field_detail.get('field_header_range_in_blob', [0, 0])
            value_range = field_detail.get('value_range_in_blob', [0, 0])
            
            field_start = header_range[0] + base_offset_in_file
            field_end = value_range[1] + base_offset_in_file
            value_start = value_range[0] + base_offset_in_file
            value_end = value_range[1] + base_offset_in_file
        else:
            # Default ranges when no field detail available
            # Use a unique default range for each field based on its position
            field_start = base_offset_in_file
            field_end = base_offset_in_file + 1
            value_start = base_offset_in_file  
            value_end = base_offset_in_file + 1
        
        # Debug output
        if debug:
            print(f"Field {field_name} (id={field_id}, level={nesting_level}) - {type(thrift_obj).__name__}:", file=sys.stderr)
            print(f"  Field detail found: {bool(field_detail)}", file=sys.stderr)
            if field_detail:
                print(f"  Field path: {field_detail.get('field_path', [])}", file=sys.stderr)
                print(f"  Field detail keys: {list(field_detail.keys())}", file=sys.stderr)
                print(f"  Header range: {field_detail.get('field_header_range_in_blob')}", file=sys.stderr)
                print(f"  Value range: {field_detail.get('value_range_in_blob')}", file=sys.stderr)
            print(f"  Calculated ranges: field=[{field_start}, {field_end}], value=[{value_start}, {value_end}]", file=sys.stderr)
        
        # Create field segment based on type
        if field_type == TType.STRUCT and field_value:
            # Recursive struct handling
            child_segments = thrift_to_segments(
                field_value, field_details_map, base_offset_in_file, 
                nesting_level + 1, show_undefined_optional, debug, field_path + [f"field_{field_id}"]
            )
            
            struct_segment = create_segment(
                field_start, field_end,
                "thrift_field",
                value={
                    "field_name": field_name,
                    "field_id": field_id,
                    "struct_type": type(field_value).__name__
                },
                children=child_segments,
                metadata={
                    "thrift_type": get_thrift_type_name(field_type, type_args),
                    "required": required
                }
            )
            segments.append(struct_segment)
            
        elif field_type == TType.LIST and field_value:
            # List handling
            list_children = []
            
            # Get list range info from field details if available
            list_details = field_detail.get('list_details', {}) if field_detail else {}
            list_start = list_details.get('start', 0) + base_offset_in_file if list_details else value_start
            list_end = list_details.get('end', 0) + base_offset_in_file if list_details else value_end
            
            # Calculate item ranges if available
            item_ranges = []
            if list_details and 'elements' in list_details and list_details['elements']:
                item_ranges = list_details['elements']
            else:
                # Fallback: divide list space equally if no specific ranges
                if len(field_value) > 0 and list_start < list_end:
                    total_size = list_end - list_start
                    items_count = len(field_value)
                    
                    for i in range(items_count):
                        # Use integer division to avoid fractional byte positions
                        start_pos = list_start + (i * total_size // items_count)
                        end_pos = list_start + ((i + 1) * total_size // items_count)
                        # Ensure we don't go beyond the list end
                        if end_pos > list_end:
                            end_pos = list_end
                        item_ranges.append([start_pos, end_pos])
            
            # When no usable ranges are available
            if not item_ranges or len(item_ranges) != len(field_value):
                # Create dummy ranges
                item_ranges = [[value_start, value_end] for _ in range(len(field_value))]
                
            # For debug output
            if debug:
                print(f"List {field_name} with {len(field_value)} items:", file=sys.stderr)
                print(f"  List range: [{list_start}, {list_end}]", file=sys.stderr)
                print(f"  Item ranges available: {len(item_ranges)}", file=sys.stderr)
            
            for i, list_item in enumerate(field_value):
                # Get item range, defaulting to list range if not available
                item_start = item_ranges[i][0] if i < len(item_ranges) else list_start
                item_end = item_ranges[i][1] if i < len(item_ranges) else list_end
                
                if hasattr(list_item, 'thrift_spec'):  # List of structs
                    item_segments = thrift_to_segments(
                        list_item, field_details_map, base_offset_in_file,
                        nesting_level + 1, show_undefined_optional, debug
                    )
                    
                    list_item_segment = create_segment(
                        item_start, item_end,
                        "thrift_struct",
                        value={"struct_type": type(list_item).__name__, "index": i},
                        children=item_segments
                    )
                    list_children.append(list_item_segment)
                else:
                    # List of primitives
                    processed_value = process_primitive_value(list_item, field_name, type(thrift_obj).__name__)
                    
                    primitive_segment = create_segment(
                        item_start, item_end,
                        get_primitive_type(list_item),
                        value=processed_value
                    )
                    list_children.append(primitive_segment)
            
            list_segment = create_segment(
                field_start, field_end,
                "thrift_field", 
                value={
                    "field_name": field_name,
                    "field_id": field_id,
                    "list_length": len(field_value)
                },
                children=list_children,
                metadata={
                    "thrift_type": get_thrift_type_name(field_type, type_args),
                    "required": required
                }
            )
            segments.append(list_segment)
        
        else:
            # Primitive field
            processed_value = process_primitive_value(field_value, field_name, type(thrift_obj).__name__)
            
            primitive_segment = create_segment(
                field_start, field_end,
                "thrift_field",
                value={
                    "field_name": field_name,
                    "field_id": field_id,
                    "data": processed_value
                },
                metadata={
                    "thrift_type": get_thrift_type_name(field_type, type_args),
                    "required": required,
                    "primitive_type": get_primitive_type(field_value)
                }
            )
            segments.append(primitive_segment)
    
    return segments

def find_field_detail(field_details_map, field_name, field_id, nesting_level, debug=False, field_path=None):
    """Find field details using multiple lookup strategies"""
    if field_path is None:
        field_path = []
    
    # First try the hierarchical path-based lookup
    if field_path:
        # Construct hierarchical key: level_field_path[0]_field_path[1]_..._field_name_id
        path_parts = []
        for part in field_path:
            path_parts.extend(['field', part])
        path_parts.extend(['field', field_name, str(field_id)])
        hierarchical_key = f"{nesting_level}_" + "_".join(path_parts)
        
        field_detail = field_details_map.get(hierarchical_key)
        if field_detail:
            if debug:
                print(f"  Found field detail for {field_name} (ID {field_id}) using hierarchical key: {hierarchical_key}", file=sys.stderr)
            return field_detail
    
    # Try basic lookup with level_name_id key
    field_key = make_field_key(nesting_level, field_name, field_id)
    field_detail = field_details_map.get(field_key)
    
    # If not found, try with generic field_N format
    if not field_detail and field_name != f"field_{field_id}":
        alt_key = make_field_key(nesting_level, f"field_{field_id}", field_id)
        field_detail = field_details_map.get(alt_key)
    
    # If still not found, try searching for path-based keys that end with field_name_field_id
    if not field_detail:
        suffix = f"{field_name}_{field_id}"
        # Try to find keys with path information that end with our field
        path_candidates = [
            k for k in field_details_map.keys() 
            if k.startswith(f"{nesting_level}_") and k.endswith(suffix)
        ]
        if path_candidates:
            field_detail = field_details_map.get(path_candidates[0])
            
    # If still not found, try just looking for the field_id at this level 
    if not field_detail:
        id_candidates = [
            k for k in field_details_map.keys()
            if k.startswith(f"{nesting_level}_") and k.endswith(f"_{field_id}")
        ]
        if id_candidates:
            field_detail = field_details_map.get(id_candidates[0])
    
    if debug and field_detail:
        print(f"  Found field detail for {field_name} (ID {field_id}) using key: {field_key if field_detail == field_details_map.get(field_key) else 'path-based'}", file=sys.stderr)
    elif debug:
        # Debug why we couldn't find it
        available_keys_for_level = [k for k in field_details_map.keys() if k.startswith(f"{nesting_level}_")]
        print(f"  Could not find field detail for {field_name} (ID {field_id}) at level {nesting_level}", file=sys.stderr)
        print(f"  Tried keys: {field_key}, {alt_key if field_name != f'field_{field_id}' else 'N/A'}", file=sys.stderr)
        if available_keys_for_level:
            print(f"  Available keys at level {nesting_level}: {available_keys_for_level[:5]}{'...' if len(available_keys_for_level) > 5 else ''}", file=sys.stderr)
    
    return field_detail

def get_primitive_type(value):
    """Get the primitive type name for a value"""
    if isinstance(value, bool):
        return "boolean"
    elif isinstance(value, int):
        return "integer"
    elif isinstance(value, float):
        return "double"
    elif isinstance(value, str):
        return "string"
    elif isinstance(value, bytes):
        return "binary"
    else:
        return "unknown"

def process_primitive_value(value, field_name, struct_name):
    """Process primitive values with special handling for enums and known fields"""
    if value is None:
        return None
        
    # Handle enum values
    enum_class = get_enum_class_for_field(field_name, struct_name)
    if enum_class and isinstance(value, int) and hasattr(enum_class, '_VALUES_TO_NAMES'):
        enum_name = enum_class._VALUES_TO_NAMES.get(value, f"UNKNOWN({value})")
        return {"enum_value": value, "enum_name": enum_name}
    
    # Handle binary data - show first few bytes for readability
    if isinstance(value, bytes):
        if len(value) <= 32:
            return {"bytes": value.hex(), "length": len(value)}
        else:
            return {
                "bytes_preview": value[:16].hex() + "...", 
                "length": len(value),
                "truncated": True
            }
    
    return value

def analyze_parquet_file_v2(file_path, debug=False, show_undefined_optional=False):
    """
    Analyze a Parquet file and return segment structure (V2)
    """
    segments = []
    
    with open(file_path, 'rb') as f:
        # Read file header
        f.seek(0)
        header = f.read(4)
        if header != b'PAR1':
            raise ValueError("Not a valid Parquet file - missing PAR1 header")
        
        segments.append(create_segment(0, 4, "parquet_header", {"header": "PAR1"}))
        
        # Read footer length (last 8 bytes)
        f.seek(-8, 2)
        footer_size_bytes = f.read(4)
        footer_magic = f.read(4)
        
        if footer_magic != b'PAR1':
            raise ValueError("Not a valid Parquet file - missing PAR1 footer")
        
        footer_size = struct.unpack('<I', footer_size_bytes)[0]
        file_size = f.tell()
        
        # Add footer magic segment
        segments.append(create_segment(file_size - 4, file_size, "parquet_footer_magic", {"footer": "PAR1"}))
        
        # Add footer size segment
        segments.append(create_segment(file_size - 8, file_size - 4, "footer_size", {"size": footer_size}))
        
        # Read footer metadata
        footer_start = file_size - 8 - footer_size
        f.seek(footer_start)
        footer_data = f.read(footer_size)
        
        # Parse footer with offset recording
        protocol = OffsetRecordingProtocol(TMemoryBuffer(footer_data), struct_class=FileMetaData, base_offset_in_file=footer_start)
        
        try:
            footer_metadata = parquet.FileMetaData()
            footer_metadata.read(protocol)
            
            # Convert footer to segments
            footer_segments = thrift_to_segments(
                footer_metadata, 
                protocol.field_details, 
                footer_start,
                nesting_level=1,
                show_undefined_optional=show_undefined_optional,
                debug=debug
            )
            
            footer_segment = create_segment(
                footer_start, file_size - 8,
                "parquet_footer",
                {"footer_size": footer_size},
                children=footer_segments
            )
            segments.append(footer_segment)
            
        except Exception as e:
            # Create error segment for unparseable footer
            error_segment = create_segment(
                footer_start, file_size - 8,
                "parquet_footer_error", 
                {"error": str(e), "footer_size": footer_size}
            )
            segments.append(error_segment)
        
        # Process row groups if footer was parsed successfully
        if 'footer_metadata' in locals() and footer_metadata.row_groups:
            for rg_idx, row_group in enumerate(footer_metadata.row_groups):
                rg_start = footer_start  # Will be updated based on column chunks
                rg_end = footer_start
                
                rg_children = []
                
                for col_idx, column_chunk in enumerate(row_group.columns):
                    if column_chunk.file_offset is not None:
                        col_start = column_chunk.file_offset
                        col_size = column_chunk.meta_data.total_compressed_size if column_chunk.meta_data else 0
                        col_end = col_start + col_size
                        
                        # Update row group bounds
                        if col_idx == 0:
                            rg_start = col_start
                            rg_end = col_end
                        else:
                            rg_start = min(rg_start, col_start)
                            rg_end = max(rg_end, col_end)
                        
                        # Read and parse column chunk
                        f.seek(col_start)
                        col_data = f.read(col_size)
                        
                        col_segment = create_segment(
                            col_start, col_end,
                            "column_chunk",
                            {
                                "row_group": rg_idx,
                                "column": col_idx,
                                "size": col_size,
                                "path": ".".join(column_chunk.meta_data.path_in_schema) if column_chunk.meta_data else "unknown"
                            }
                        )
                        rg_children.append(col_segment)
                
                if rg_children:
                    rg_segment = create_segment(
                        rg_start, rg_end,
                        "row_group",
                        {"index": rg_idx, "num_columns": len(row_group.columns)},
                        children=rg_children
                    )
                    segments.append(rg_segment)
    
    # Sort segments by their byte position in the file for proper sequential reading
    segments.sort(key=lambda segment: segment["range"][0])
    
    return {"segments": segments}

def main():
    parser = argparse.ArgumentParser(description="Parquet Lens V2: Detailed Parquet file analyzer with improved structure.")
    parser.add_argument("parquet_file", help="Path to the Parquet file to analyze.")
    parser.add_argument("--debug", action="store_true", help="Enable debug output showing field processing details.")
    parser.add_argument("--show-undefined-optional-fields", action="store_true", 
                        help="Show optional fields even when they are undefined/null (default: hide them)")
    args = parser.parse_args()

    analysis_result = analyze_parquet_file_v2(args.parquet_file, debug=args.debug, 
                                         show_undefined_optional=args.show_undefined_optional_fields)
    print(json.dumps(analysis_result, indent=4))

if __name__ == "__main__":
    main()
