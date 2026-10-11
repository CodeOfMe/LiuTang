from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from liutang.core.schema import Schema


class SourceKind(Enum):
    COLLECTION = "collection"
    FILE = "file"
    KAFKA = "kafka"
    DATAGEN = "datagen"
    SOCKET = "socket"
    GENERATOR = "generator"


class SinkKind(Enum):
    PRINT = "print"
    FILE = "file"
    KAFKA = "kafka"
    CALLBACK = "callback"
    COLLECT = "collect"
    SOCKET = "socket"


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
class GeneratorSource(SourceConnector):
    generator: Callable[[], Any]
    max_items: Optional[int] = None

    def kind(self) -> SourceKind:
        return SourceKind.GENERATOR


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
    max_records: Optional[int] = None

    def kind(self) -> SourceKind:
        return SourceKind.DATAGEN


@dataclass
class SocketSource(SourceConnector):
    host: str = "localhost"
    port: int = 9999
    delimiter: str = "\n"
    encoding: str = "utf-8"

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
    func: Callable[[Any], None]

    def kind(self) -> SinkKind:
        return SinkKind.CALLBACK


@dataclass
class CollectSink(SinkConnector):
    results: List[Any] = None

    def __post_init__(self) -> None:
        if self.results is None:
            self.results = []

    def kind(self) -> SinkKind:
        return SinkKind.COLLECT


@dataclass
class SocketSink(SinkConnector):
    host: str = "localhost"
    port: int = 9999

    def kind(self) -> SinkKind:
        return SinkKind.SOCKET