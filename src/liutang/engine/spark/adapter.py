from __future__ import annotations

from typing import Any, Dict, List, Optional

from liutang.core.errors import EngineNotAvailableError
from liutang.core.schema import Schema, Field, FieldType


SPARK_MIN_VERSION = "3.4.0"


def _check_spark() -> None:
    try:
        import pyspark
        from packaging.version import Version
        ver = pyspark.__version__
        if Version(ver) < Version(SPARK_MIN_VERSION):
            raise EngineVersionError("pyspark", SPARK_MIN_VERSION, ver)
    except ImportError:
        raise EngineNotAvailableError("spark")


class SparkTypeAdapter:
    _TYPE_MAP = {
        FieldType.STRING: "string",
        FieldType.INTEGER: "int",
        FieldType.LONG: "bigint",
        FieldType.FLOAT: "float",
        FieldType.DOUBLE: "double",
        FieldType.BOOLEAN: "boolean",
        FieldType.TIMESTAMP: "timestamp",
        FieldType.DATE: "date",
        FieldType.BYTES: "binary",
    }

    @staticmethod
    def field_to_spark(field: Field) -> Any:
        from pyspark.sql.types import (
            StringType, IntegerType, LongType, FloatType, DoubleType,
            BooleanType, TimestampType, DateType, BinaryType,
            ArrayType, MapType, StructField,
        )
        type_map = {
            FieldType.STRING: StringType(),
            FieldType.INTEGER: IntegerType(),
            FieldType.LONG: LongType(),
            FieldType.FLOAT: FloatType(),
            FieldType.DOUBLE: DoubleType(),
            FieldType.BOOLEAN: BooleanType(),
            FieldType.TIMESTAMP: TimestampType(),
            FieldType.DATE: DateType(),
            FieldType.BYTES: BinaryType(),
        }
        spark_type = type_map.get(field.field_type)
        if spark_type is None:
            raise EngineNotAvailableError("spark", f"Unsupported field type: {field.field_type}")
        return StructField(field.name, spark_type, nullable=field.nullable)

    @staticmethod
    def schema_to_spark(schema: Schema) -> Any:
        from pyspark.sql.types import StructType
        fields = [SparkTypeAdapter.field_to_spark(f) for f in schema.fields]
        return StructType(fields)

    @staticmethod
    def schema_to_ddl(schema: Schema) -> str:
        cols = []
        for f in schema.fields:
            col = f"{f.name} {SparkTypeAdapter._TYPE_MAP.get(f.field_type, 'string')}"
            if not f.nullable:
                col += " NOT NULL"
            cols.append(col)
        return ", ".join(cols)