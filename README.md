# Parquet Lens

A developer tool for deep inspection of Parquet files. Provides a detailed byte-by-byte mapping of Parquet file structures, accounting for every byte and explaining Thrift structs and data segments.

## Features (Planned)

*   Sequential, non-overlapping segment analysis of a Parquet file.
*   Detailed information for each segment:
    *   Byte range (`[start, end]`)
    *   Type (e.g., `magic_number`, `thrift`, `data`, `padding`, `unknown`, `error`)
    *   For Thrift segments:
        *   Thrift type (e.g., `struct`, `enum`, `i32`)
        *   Name (e.g., `PageHeader`, `FileMetaData`)
        *   Field details (range, type, name, value, required/optional status)
    *   For data segments:
        *   Encoding
        *   Compression
        *   Statistics (if available)
*   Error tolerance: Identifies and reports erroneous segments.
*   Scalable output for large files (focus on metadata and ranges, not necessarily full data dumps).
