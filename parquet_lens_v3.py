import argparse
import json
import logging
import struct

from thrift.protocol import TCompactProtocol
from thrift.protocol.TProtocol import TType
from thrift.transport.TTransport import TMemoryBuffer
from parquet.ttypes import (
    BoundaryOrder,
    ColumnIndex,
    CompressionCodec,
    ColumnMetaData,
    ConvertedType,
    DataPageHeader,
    DataPageHeaderV2,
    DictionaryPageHeader,
    EdgeInterpolationAlgorithm,
    Encoding,
    FieldRepetitionType,
    FileMetaData,
    GeographyType,
    PageEncodingStats,
    PageHeader,
    PageType,
    SchemaElement,
    Type,
)


class OffsetRecordingProtocol(TCompactProtocol.TCompactProtocol):
    logger = logging.getLogger(__qualname__)

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
        TType.LIST: "list",
    }

    enum_map = {
        ColumnMetaData: {
            "codec": CompressionCodec,
            "type": Type,
            "encodings": Encoding,
        },
        SchemaElement: {
            "converted_type": ConvertedType,
            "type": Type,
            "repetition_type": FieldRepetitionType,
        },
        GeographyType: {
            "algorithm": EdgeInterpolationAlgorithm,
        },
        PageEncodingStats: {
            "encoding": Encoding,
            "page_type": PageType,
        },
        PageHeader: {
            "type": PageType,
        },
        DataPageHeader: {
            "encoding": Encoding,
            "definition_level_encoding": Encoding,
            "repetition_level_encoding": Encoding,
        },
        DataPageHeaderV2: {
            "encoding": Encoding,
        },
        DictionaryPageHeader: {
            "encoding": Encoding,
        },
        ColumnIndex: {
            "boundary_order": BoundaryOrder,
        }
    }

    def __init__(self, trans, name, struct_class):
        super().__init__(trans)
        self._parents = []
        self._current = {
            "name": name,
            "type": "struct",
            "type_class": struct_class,
            "spec": (struct_class, struct_class.thrift_spec),
            "range_from": None,
            "range_to": None,
            "value": [],
        }

    def get_info(self):
        return self._current

    def readStructBegin(self):
        ret = super().readStructBegin()
        self.logger.debug(f"readStructBegin: {ret}")
        if self._current["type"] == "list":
            type_id, (struct_class, spec), required = self._current["spec"]
            assert type_id == TType.STRUCT
            self._new_child(
                {
                    "name": "element",
                    "type": "struct",
                    "type_class": struct_class,
                    "spec": (struct_class, spec),
                    "range_from": None,
                    "range_to": None,
                    "value": [],
                }
            )
        self._current["range_from"] = self._get_pos()
        return ret

    def readStructEnd(self):
        ret = super().readStructEnd()
        self.logger.debug(f"readStructEnd: {ret}")
        self._current["range_to"] = self._get_pos()
        if self._has_parent(lambda p: p["type"] == "list"):
            self._finish_child()
        return ret

    def readFieldBegin(self):
        assert self._current["type"] == "struct"
        ret = super().readFieldBegin()
        self.logger.debug(f"readFieldBegin: {ret} (struct: {self._current['name']})")
        _, type_id, field_id = ret
        if field_id > 0:
            spec = self._current["spec"]
            field_info = spec[1][field_id]
            field_id, field_type_id, field_name, field_spec, _ = field_info
            if field_type_id == TType.STRUCT:
                type_class = field_spec[0]
            else:
                type_class = None
            self._new_child(
                {
                    "name": field_name,
                    "type": self.type_map[field_type_id],
                    "type_class": type_class,
                    "spec": field_spec,
                    "range_from": self._get_pos(),
                    "range_to": None,
                    "value": [] if self._is_complex_type(field_type_id) else None,
                }
            )
        return ret

    def readFieldEnd(self):
        ret = super().readFieldEnd()
        self.logger.debug(f"readFieldEnd: {ret}")
        self._current["range_to"] = self._get_pos()
        self._finish_child()
        return ret

    def readListBegin(self):
        ret = super().readListBegin()
        self.logger.debug(f"readListBegin: {ret}")
        return ret

    def readListEnd(self):
        ret = super().readListEnd()
        self.logger.debug(f"readListEnd: {ret}")
        return ret

    def readMapBegin(self):
        ret = super().readMapBegin()
        self.logger.debug(f"readMapBegin: {ret}")
        return ret

    def readMapEnd(self):
        ret = super().readMapEnd()
        self.logger.debug(f"readMapEnd: {ret}")
        return ret

    def readSetBegin(self):
        ret = super().readSetBegin()
        self.logger.debug(f"readSetBegin: {ret}")
        return ret

    def readSetEnd(self):
        ret = super().readSetEnd()
        self.logger.debug(f"readSetEnd: {ret}")
        return ret

    def readMessageBegin(self):
        ret = super().readMessageBegin()
        self.logger.debug(f"readMessageBegin: {ret}")
        return ret

    def readMessageEnd(self):
        ret = super().readMessageEnd()
        self.logger.debug(f"readMessageEnd: {ret}")
        return ret

    def readByte(self):
        ret = super().readByte()
        self.logger.debug(f"readByte: {ret}")
        self._append_value(ret)
        return ret

    def readI16(self):
        ret = super().readI16()
        self.logger.debug(f"readI16: {ret}")
        self._append_value(ret)
        return ret

    def readI32(self):
        ret = super().readI32()
        self.logger.debug(f"readI32: {ret}")
        self._append_value(ret)
        return ret

    def readI64(self):
        ret = super().readI64()
        self.logger.debug(f"readI64: {ret}")
        self._append_value(ret)
        return ret

    def readDouble(self):
        ret = super().readDouble()
        self.logger.debug(f"readDouble: {ret}")
        self._append_value(ret)
        return ret

    def readBool(self):
        ret = super().readBool()
        self.logger.debug(f"readBool: {ret}")
        self._append_value(ret)
        return ret

    def readString(self):
        ret = super().readString()
        self.logger.debug(f"readString: {ret}")
        self._append_value(ret)
        return ret

    def _get_pos(self):
        return self.trans._buffer.tell()

    def _is_complex_type(self, type_id):
        return type_id in {TType.STRUCT, TType.MAP, TType.SET, TType.LIST}

    def _has_parent(self, predicate):
        return self._parents and predicate(self._parents[-1])

    def _get_parent(self):
        return self._parents[-1]

    def _append_value(self, value):
        if isinstance(self._current["value"], list):
            self._current["value"].append(value)
        else:
            self._current["value"] = value
        self._annotate_enum()

    def _annotate_enum(self):
        if self._has_parent(
            lambda p: p["type"] in ("struct", "list")
            and self._is_enum(p["type_class"], self._current["name"])
        ):
            enum_class, name = self._get_enum(
                self._get_parent()["type_class"], self._current["name"]
            )
            self._current["enum_type"] = enum_class.__name__
            self._current["enum_name"] = name

    def _is_enum(self, parent_class, field_name):
        return self.enum_map.get(parent_class, {}).get(field_name) is not None

    def _get_enum(self, parent_class, field_name):
        enum_class = self.enum_map.get(parent_class, {}).get(field_name)
        value = self._current["value"]
        if isinstance(value, list):
            return (enum_class, [enum_class._VALUES_TO_NAMES.get(v) for v in value])
        else:
            return (enum_class, enum_class._VALUES_TO_NAMES.get(value))

    def _new_child(self, child):
        self.logger.debug(f"Starting child for {self._current['name']}")
        self.logger.debug(f"Push: {child}")
        self._parents.append(self._current)
        self._current = child

    def _finish_child(self):
        self.logger.debug(f"Pop: {self._current}")
        parent = self._parents.pop()
        parent["value"].append(self._current)
        self.logger.debug(f"Finished child for {parent['name']}")
        self._current = parent


def create_segments_from_offset_info(info, base_offset):
    if not isinstance(info, dict):
        return info
    if info["type"] in ("struct", "list"):
        value = []
        for value_info in info["value"]:
            value.append(create_segments_from_offset_info(value_info, base_offset))
    else:
        value = info["value"]
    metadata = {}
    metadata["type"] = info["type"]
    if info["type_class"]:
        metadata["type_class"] = info["type_class"].__name__
    if "enum_type" in info:
        metadata["enum_type"] = info["enum_type"]
        metadata["enum_name"] = info["enum_name"]
    return create_segment(
        base_offset + info["range_from"],
        base_offset + info["range_to"],
        info["name"],
        value,
        metadata,
    )


def create_segment(range_start, range_end, name, value=None, metadata=None):
    segment = {}
    segment["offset_range"] = [range_start, range_end]
    segment["name"] = name
    segment["value"] = value
    if metadata:
        segment["metadata"] = metadata
    return segment


def get_segments(file_path):
    segments = []

    with open(file_path, "rb") as f:
        # Read file header
        f.seek(0)
        header = f.read(4)
        if header != b"PAR1":
            raise ValueError("Not a valid Parquet file - missing PAR1 header")
        segments.append(create_segment(0, 4, "magic_number", "PAR1"))

        # Read footer length (last 8 bytes)
        f.seek(-8, 2)
        footer_size_bytes = f.read(4)
        footer_magic = f.read(4)
        file_size = f.tell()
        if footer_magic != b"PAR1":
            raise ValueError("Not a valid Parquet file - missing PAR1 footer")
        footer_size = struct.unpack("<I", footer_size_bytes)[0]
        segments.append(
            create_segment(file_size - 4, file_size, "magic_number", "PAR1")
        )
        segments.append(
            create_segment(file_size - 8, file_size - 4, "footer_length", footer_size)
        )

        # Read footer metadata
        footer_start = file_size - 8 - footer_size
        f.seek(footer_start)
        footer_data = f.read(footer_size)

        # Parse footer with offset recording
        protocol = OffsetRecordingProtocol(
            TMemoryBuffer(footer_data),
            "footer",
            struct_class=FileMetaData,
        )

        footer_metadata = FileMetaData()
        footer_metadata.read(protocol)

        segments.append(
            create_segments_from_offset_info(
                protocol.get_info(), base_offset=footer_start
            )
        )

    segments.sort(key=lambda s: s["offset_range"])
    return segments


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("parquet_file")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.getLevelNamesMapping()[args.log_level.upper()],
        format="%(asctime)s %(name)s [%(threadName)s] %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    segments = get_segments(args.parquet_file)
    print(json.dumps(segments, indent=2))


if __name__ == "__main__":
    main()
