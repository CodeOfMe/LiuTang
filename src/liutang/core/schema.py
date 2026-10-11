from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


class FieldType(Enum):
    STRING = "string"
    INTEGER = "integer"
    LONG = "long"
    FLOAT = "float"
    DOUBLE = "double"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    DATE = "date"
    BYTES = "bytes"
    ARRAY = "array"
    MAP = "map"
    ROW = "row"


@dataclass
class Field:
    name: str
    field_type: FieldType
    nullable: bool = True
    description: str = ""
    element_type: Optional[FieldType] = None
    key_type: Optional[FieldType] = None
    value_type: Optional[FieldType] = None
    fields: Optional[List["Field"]] = None


@dataclass
class Schema:
    fields: List[Field] = field(default_factory=list)

    def add(self, name: str, field_type: FieldType, **kwargs: Any) -> "Schema":
        self.fields.append(Field(name=name, field_type=field_type, **kwargs))
        return self

    def field_names(self) -> List[str]:
        return [f.name for f in self.fields]

    def get_field(self, name: str) -> Optional[Field]:
        for f in self.fields:
            if f.name == name:
                return f
        return None

    def column_count(self) -> int:
        return len(self.fields)

    def to_dict_list(self) -> List[Dict[str, str]]:
        return [{"name": f.name, "type": f.field_type.value, "nullable": f.nullable} for f in self.fields]

    @classmethod
    def from_dict(cls, spec: Dict[str, FieldType]) -> "Schema":
        return cls(fields=[Field(name=n, field_type=t) for n, t in spec.items()])

    @classmethod
    def from_pairs(cls, pairs: List[Tuple[str, FieldType]]) -> "Schema":
        return cls(fields=[Field(name=n, field_type=t) for n, t in pairs])