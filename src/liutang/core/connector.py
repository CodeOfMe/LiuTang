from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

from liutang.core.schema import Schema


class SourceKind(Enum):
    COLLECTION = "collection"
    FILE = "file"
    KAFKA = "kafka"
    DATAGEN = "datagen"
    SOCKET = "socket"


class SinkKind(Enum):
    PRINT = "print"
    FILE = "file"
    KAFKA = "kafka"
    CONSOLE = "console"
    CALLBACK = "callback"


class SourceConnector(ABC):
    @abstractmethod
    def kind(self) -> SourceKind:
        ...


class SinkConnector(ABC):
    @abstractmethod
    def kind(self) -> SinkKind:
        ...


@dataclass
class CollectionSource(SourceConnector):
    data: List[Any]

    def kind(self) -> SourceKind:
        return SourceKind.COLLECTION


@dataclass
class FileSource(SourceConnector):
    path: str
    fmt: str = "text"
    schema: Optional[Schema] = None

    def kind(self) -> SourceKind:
        return SourceKind.FILE


@dataclass
class KafkaSource(SourceConnector):
    topic: str
    bootstrap_servers: str = "localhost:9092"
    group_id: str = "liutang-group"
    start_from: str = "earliest"
    schema: Optional[Schema] = None

    def kind(self) -> SourceKind:
        return SourceKind.KAFKA


@dataclass
class DatagenSource(SourceConnector):
    rows_per_second: int = 100
    fields: Optional[Dict[str, str]] = None

    def kind(self) -> SourceKind:
        return SourceKind.DATAGEN


@dataclass
class SocketSource(SourceConnector):
    host: str = "localhost"
    port: int = 9999
    delimiter: str = "\n"

    def kind(self) -> SourceKind:
        return SourceKind.SOCKET


class PrintSink(SinkConnector):
    def kind(self) -> SinkKind:
        return SinkKind.PRINT


@dataclass
class FileSink(SinkConnector):
    path: str
    fmt: str = "csv"

    def kind(self) -> SinkKind:
        return SinkKind.FILE


@dataclass
class KafkaSink(SinkConnector):
    topic: str
    bootstrap_servers: str = "localhost:9092"

    def kind(self) -> SinkKind:
        return SinkKind.KAFKA


@dataclass
class CallbackSink(SinkConnector):
    func: Any

    def kind(self) -> SinkKind:
        return SinkKind.CALLBACK