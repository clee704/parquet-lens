# List Header Gap Analysis

## Investigation Summary

The gap between `field_header_range` and `value_range` for list types in Parquet files has been investigated and fully explained.

## Root Cause

The gap exists because of the **Thrift Binary Protocol specification**. When encoding list fields, Thrift uses a specific structure:

1. **Field Header** (1+ bytes): Field ID + field type (LIST)
2. **List Header** (1+ bytes): Element type + element count  
3. **List Elements** (variable): The actual list data

## Example

For the `schema` field in FileMetaData:

### Without `--show-list-headers`:
```json
{
    "field_id": 2,
    "field_name": "schema",
    "type": "list<struct>",
    "required": true,
    "field_header_range": [132, 133],  // Field header: 1 byte
    "value_range": [134, 175],         // List elements: 41 bytes
    // GAP: byte 133-134 (1 byte) - contains list metadata
}
```

### With `--show-list-headers`:
```json
{
    "field_id": 2,
    "field_name": "schema", 
    "type": "list<struct>",
    "required": true,
    "field_header_range": [132, 133],  // Field header: 1 byte
    "list_header_range": [133, 134],   // List metadata: 1 byte
    "value_range": [134, 175],         // List elements: 41 bytes
    // NO GAP: all bytes accounted for
}
```

## What's in the List Header

The list header at byte 133 contains:
- **Element Type** (4 bits): 0x0C = STRUCT type
- **Element Count** (remaining bits): Number of list elements

In hex: `0x4C` → Element type STRUCT (12 = 0xC), with element count information.

## Implementation

Added a new command-line option `--show-list-headers` that:

1. **Tracks List Headers**: Modified `OffsetRecordingProtocol.readListBegin()` to record the position before and after reading list metadata
2. **Shows the Gap**: Displays `list_header_range` showing exactly what bytes contain the Thrift list metadata
3. **Explains the Protocol**: Provides clear documentation that this is normal Thrift binary protocol behavior

## Usage

```bash
# Show the gap (default behavior)
python parquet_lens.py file.parquet

# Explain the gap by showing list headers
python parquet_lens.py file.parquet --show-list-headers
```

## Conclusion

The gap between header and value ranges for list types is **completely normal and expected**. It represents the Thrift binary protocol's list metadata that tells the deserializer:

1. What type of elements are in the list
2. How many elements to expect

This is not a bug or issue - it's the standard way Thrift encodes list structures in binary format.
