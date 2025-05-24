import argparse
import json
import pyarrow.parquet as pq
import sys # Add sys import
import os # Add os import

# Imports for Thrift
from thrift.protocol import TCompactProtocol
from thrift.transport import TTransport
from parquet.ttypes import FileMetaData # Updated import path

def thrift_to_dict(thrift_obj):
    """
    Recursively converts a Thrift object to a dictionary.
    Handles enums by converting them to their string names.
    Handles bytes by attempting UTF-8 decode or converting to hex.
    """
    if hasattr(thrift_obj, '__dict__'):
        result = {}
        for k, v in thrift_obj.__dict__.items():
            if hasattr(v, '_NAMES_TO_VALUES'): # Check if it's a Thrift enum
                result[k] = thrift_obj._VALUES_TO_NAMES[v] if v in thrift_obj._VALUES_TO_NAMES else v
            elif isinstance(v, list):
                result[k] = [thrift_to_dict(i) for i in v]
            elif hasattr(v, '__dict__'):
                result[k] = thrift_to_dict(v)
            elif isinstance(v, bytes): # Handle bytes
                try:
                    result[k] = v.decode('utf-8')
                except UnicodeDecodeError:
                    result[k] = v.hex() # Fallback to hex representation
            else:
                result[k] = v
        return result
    elif isinstance(thrift_obj, bytes): # Handle top-level bytes object, though less common for complex structs
        try:
            return thrift_obj.decode('utf-8')
        except UnicodeDecodeError:
            return thrift_obj.hex()
    return thrift_obj

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

                # Parse metadata_bytes using Thrift
                parsed_metadata = None
                try:
                    transport = TTransport.TMemoryBuffer(metadata_bytes)
                    protocol = TCompactProtocol.TCompactProtocol(transport)
                    file_metadata_obj = FileMetaData()
                    file_metadata_obj.read(protocol)
                    parsed_metadata = thrift_to_dict(file_metadata_obj)
                except Exception as e:
                    parsed_metadata = {"error": f"Thrift deserialization failed: {str(e)}"}


                segments.append({
                    "range": [metadata_offset, metadata_offset + footer_length],
                    "type": "thrift_metadata_blob",
                    "thrift_type": "FileMetaData",
                    "description": "FileMetaData Thrift structure",
                    "size": footer_length,
                    "parsed_value": parsed_metadata # Add the parsed structure
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
