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

# New OffsetRecordingProtocol
class OffsetRecordingProtocol(TCompactProtocol.TCompactProtocol):
    def __init__(self, trans, struct_class, base_offset_in_file): # Changed 'spec' to 'struct_class'
        super().__init__(trans)
        self._struct_class_spec = struct_class.thrift_spec if struct_class else None # Use thrift_spec
        self._base_offset_in_file = base_offset_in_file
        
        self.field_details = {}
        self._current_field_name = None
        self._current_field_id = None
        self._field_header_start_offset_in_blob = None
        self._field_total_start_offset_in_blob = None
        # For nested structs
        self._parent_spec_stack = [] 
        self._current_spec = self._struct_class_spec

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
        super().readStructBegin()

    def readStructEnd(self):
        super().readStructEnd()
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
        self._field_total_start_offset_in_blob = self._get_trans_pos()
        self._field_header_start_offset_in_blob = self._field_total_start_offset_in_blob
        
        fname_ignored, ftype, fid = super().readFieldBegin()
        
        self._current_field_id = fid
        # Use _current_spec which is updated for nested structs
        self._current_field_name = self._get_field_name_from_spec(fid, self._current_spec)
        if not self._current_field_name:
            self._current_field_name = f"_unknown_field_id_{fid}_"

        if self._current_field_name not in self.field_details and ftype != TType.STOP:
            self.field_details[self._current_field_name] = {}
        
        if ftype != TType.STOP:
            header_end_offset = self._get_trans_pos()
            self.field_details[self._current_field_name]['field_header_range_in_blob'] = [
                self._field_header_start_offset_in_blob, header_end_offset
            ]

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
        if self._current_field_name and self._current_field_name in self.field_details:
            if 'value_range_in_blob' in self.field_details[self._current_field_name]:
                 self.field_details[self._current_field_name]['field_total_range_in_blob'] = [
                    self._field_total_start_offset_in_blob,
                    self.field_details[self._current_field_name]['value_range_in_blob'][1]
                ]
            elif self._field_total_start_offset_in_blob is not None:
                 self.field_details[self._current_field_name]['field_total_range_in_blob'] = [
                    self._field_total_start_offset_in_blob,
                    self._get_trans_pos()
                ]

        super().readFieldEnd()
        self._current_field_name = None
        self._current_field_id = None
        self._field_total_start_offset_in_blob = None
        self._field_header_start_offset_in_blob = None

    def _read_primitive(self, read_method_name):
        value_start_offset = self._get_trans_pos()
        actual_read_method = getattr(super(), read_method_name)
        value = actual_read_method()
        value_end_offset = self._get_trans_pos()

        if self._current_field_name and self._current_field_name in self.field_details:
            self.field_details[self._current_field_name]['value_range_in_blob'] = [
                value_start_offset, value_end_offset
            ]
            if self._field_total_start_offset_in_blob is not None:
                 self.field_details[self._current_field_name]['field_total_range_in_blob'] = [
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


def thrift_to_dict_with_offsets(thrift_obj, field_details_map, base_offset_in_file):
    """
    Recursively converts a Thrift object to a list of field data,
    ordered by thrift_spec, merging field_details.
    Handles enums, bytes, and adds offset information.
    """
    if thrift_obj is None:
        return None

    if isinstance(thrift_obj, list):
        # For lists, each element is processed. The list itself doesn't have 'fields'.
        # This path is for when a list is passed directly, not as a field of a struct.
        return [thrift_to_dict_with_offsets(i, {}, base_offset_in_file) for i in thrift_obj]

    if not hasattr(thrift_obj, '__dict__'): # Base types, enums
        if isinstance(thrift_obj, bytes):
            try:
                return thrift_obj.decode('utf-8')
            except UnicodeDecodeError:
                return thrift_obj.hex()
        return thrift_obj

    result_fields_list = []
    obj_thrift_spec = getattr(thrift_obj, 'thrift_spec', None)

    if not obj_thrift_spec:
        # Fallback for objects without thrift_spec (should not happen for Thrift-generated classes)
        # This will return a list of {"field_name": ..., "value": ...} for raw attributes.
        for attr_name, attr_value in thrift_obj.__dict__.items():
            if attr_name.startswith('_'): continue
            result_fields_list.append({
                "field_name": attr_name,
                "value": thrift_to_dict_with_offsets(attr_value, {}, base_offset_in_file)
            })
        return result_fields_list

    # Iterate through the thrift_spec to ensure all defined fields are considered in order
    for spec_tuple in obj_thrift_spec:
        if not spec_tuple: continue
        # field_id = spec_tuple[0] # Not strictly needed for output, but part of spec
        field_type = spec_tuple[1]
        field_name = spec_tuple[2]
        type_args = spec_tuple[3]
        # default_value = spec_tuple[4] # Not used here

        field_value = getattr(thrift_obj, field_name, None)
        
        # Skip fields that are None and not explicitly set (Thrift optional fields)
        # However, we want to represent all fields defined in the spec.
        # If field_value is None, it means it wasn't present in the serialized data or was explicitly null.

        processed_value = None
        is_enum = False
        if type_args and isinstance(type_args, tuple) and len(type_args) == 1 and hasattr(type_args[0], '_VALUES_TO_NAMES'):
            enum_class = type_args[0]
            if field_value is not None:
                processed_value = enum_class._VALUES_TO_NAMES.get(field_value, field_value)
            else:
                processed_value = None # Explicitly None if field_value is None
            is_enum = True
        
        if is_enum:
            pass 
        elif field_type == TType.LIST:
            processed_value = []
            if field_value: # If the list field exists and is not None
                element_type_meta = type_args[0] if type_args and isinstance(type_args, tuple) else None
                element_is_enum = False
                element_enum_class = None
                if element_type_meta and isinstance(element_type_meta, tuple) and len(element_type_meta) == 1 and hasattr(element_type_meta[0], '_VALUES_TO_NAMES'):
                    element_enum_class = element_type_meta[0]
                    element_is_enum = True

                for item in field_value:
                    if element_is_enum and element_enum_class:
                        processed_value.append(element_enum_class._VALUES_TO_NAMES.get(item, item))
                    else:
                        # Recursive call for list elements (e.g., list of structs)
                        # The result of this will be the direct value or a list of fields if it's a struct
                        processed_value.append(thrift_to_dict_with_offsets(item, {}, base_offset_in_file))
            # If field_value is None (optional list not set), processed_value remains None or empty list as initialized
            elif field_value is None:
                 processed_value = None

        elif field_type == TType.STRUCT:
            # Recursive call for nested structs. The result will be a list of fields.
            processed_value = thrift_to_dict_with_offsets(field_value, {}, base_offset_in_file)
        elif isinstance(field_value, bytes):
            try:
                processed_value = field_value.decode('utf-8')
            except UnicodeDecodeError:
                processed_value = field_value.hex()
        else:
            processed_value = field_value

        field_data_to_store = {"value": processed_value}

        if field_name in field_details_map:
            details = field_details_map[field_name]
            for range_type, blob_range in details.items():
                if blob_range and len(blob_range) == 2 and blob_range[0] is not None and blob_range[1] is not None:
                    file_range_key = range_type.replace('_in_blob', '_in_file')
                    field_data_to_store[file_range_key] = [
                        blob_range[0] + base_offset_in_file,
                        blob_range[1] + base_offset_in_file
                    ]
        
        result_fields_list.append({
            "field_name": field_name,
            **field_data_to_store
        })
    return result_fields_list


def analyze_parquet_file(file_path):
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
                    # Pass the FileMetaData class itself to OffsetRecordingProtocol
                    protocol = OffsetRecordingProtocol(transport, FileMetaData, metadata_offset)
                    
                    file_metadata_obj = FileMetaData()
                    file_metadata_obj.read(protocol)

                    raw_field_details = protocol.field_details
                    # The function now returns a list of fields
                    parsed_metadata_fields = thrift_to_dict_with_offsets(
                        file_metadata_obj, 
                        raw_field_details,
                        metadata_offset
                    )
                    # Store as "fields" instead of "parsed_value"
                    metadata_representation = {"fields": parsed_metadata_fields}

                except Exception as e:
                    error_details_for_json = {k: str(v) for k, v in raw_field_details.items()}
                    # Store error under "fields" as well for consistency, or keep "error" at top level of parsed_value
                    metadata_representation = {
                        "error": f"Thrift deserialization or offset processing failed: {str(e)}",
                        "raw_field_details_on_error": error_details_for_json
                    }

                segments.append({
                    "range": [metadata_offset, metadata_offset + footer_length],
                    "type": "thrift_metadata_blob",
                    "thrift_type": "FileMetaData",
                    "description": "FileMetaData Thrift structure with field offsets",
                    "size": footer_length,
                    "parsed_value": metadata_representation # Use the new representation
                })

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
    args = parser.parse_args()

    analysis_result = analyze_parquet_file(args.parquet_file)
    print(json.dumps(analysis_result, indent=4))

if __name__ == "__main__":
    main()
