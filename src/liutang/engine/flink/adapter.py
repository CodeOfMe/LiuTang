from __future__ import annotations

from typing import Any, Dict, List, Optional

from liutang.core.errors import EngineNotAvailableError, EngineVersionError
from liutang.core.schema import Schema, Field, FieldType


FLINK_MIN_VERSION = "1.18.0"


def _check_flink() -> Any:
    try:
        import pyflink
        return pyflink
    except ImportError:
        raise EngineNotAvailableError("flink")


def _check_version() -> str:
    try:
        from pyflink import __version__ as flink_ver
        return flink_ver
    except (ImportError, AttributeError):
        return "unknown"


class FlinkTypeAdapter:
    _TYPE_MAP = {
        FieldType.STRING: "STRING",
        FieldType.INTEGER: "INT",
        FieldType.LONG: "BIGINT",
        FieldType.FLOAT: "FLOAT",
        FieldType.DOUBLE: "DOUBLE",
        FieldType.BOOLEAN: "BOOLEAN",
        FieldType.TIMESTAMP: "TIMESTAMP(3)",
        FieldType.DATE: "DATE",
        FieldType.BYTES: "BYTES",
    }

    @staticmethod
    def field_to_flink(field: Field) -> Any:
        from pyflink.table import DataTypes
        type_map = {
            FieldType.STRING: DataTypes.STRING(),
            FieldType.INTEGER: DataTypes.INT(),
            FieldType.LONG: DataTypes.BIGINT(),
            FieldType.FLOAT: DataTypes.FLOAT(),
            FieldType.DOUBLE: DataTypes.DOUBLE(),
            FieldType.BOOLEAN: DataTypes.BOOLEAN(),
            FieldType.TIMESTAMP: DataTypes.TIMESTAMP(3),
            FieldType.DATE: DataTypes.DATE(),
            FieldType.BYTES: DataTypes.BYTES(),
        }
        flink_type = type_map.get(field.field_type)
        if flink_type is None:
            raise EngineNotAvailableError("flink", f"Unsupported field type: {field.field_type}")
        if field.nullable:
            return flink_type
        return flink_type

    @staticmethod
    def schema_to_flink(schema: Schema) -> Any:
        from pyflink.table import DataTypes
        fields = []
        for f in schema.fields:
            nullable_str = "" if f.nullable else " NOT NULL"
            type_str = FlinkTypeAdapter._TYPE_MAP.get(f.field_type, "STRING")
            fields.append(f"{f.name} {type_str}{nullable_str}")
        return DataTypes.ROW([DataTypes.FIELD(f.name, FlinkTypeAdapter.field_to_flink(f)) for f in schema.fields])

    @staticmethod
    def schema_to_ddl(schema: Schema, table_name: str = "table") -> str:
        cols = []
        for f in schema.fields:
            col = f"{f.name} {FlinkTypeAdapter._TYPE_MAP.get(f.field_type, 'STRING')}"
            if not f.nullable:
                col += " NOT NULL"
            cols.append(col)
        return f"({', '.join(cols)})"