import pytest
from liutang import (
    Flow,
    Stream,
    RuntimeMode,
    FieldType,
    Schema,
    Field,
    WindowType,
    WindowKind,
    CollectionSource,
    FileSource,
    PrintSink,
    CallbackSink,
    MemoryStateBackend,
    KeyedState,
    LiuTangError,
    EngineNotAvailableError,
    list_engines,
    is_engine_available,
    quick_flow,
)


class TestFlow:
    def test_create_flow_default(self):
        flow = Flow()
        assert flow.name == "liutang-flow"
        assert flow.engine == "flink"
        assert flow.mode == RuntimeMode.STREAMING

    def test_create_flow_custom(self):
        flow = Flow(name="test", engine="local", mode=RuntimeMode.BATCH)
        assert flow.name == "test"
        assert flow.engine == "local"
        assert flow.mode == RuntimeMode.BATCH

    def test_set_parallelism(self):
        flow = Flow().set_parallelism(4)
        assert flow.parallelism == 4

    def test_configure(self):
        flow = Flow().configure("key", "value")
        assert flow.config["key"] == "value"

    def test_add_jar(self):
        flow = Flow().add_jar("/path/to/connector.jar")
        assert "/path/to/connector.jar" in flow.jar_paths

    def test_set_checkpoint(self):
        flow = Flow().set_checkpoint("/tmp/checkpoints")
        assert flow.checkpoint_dir == "/tmp/checkpoints"

    def test_from_collection(self):
        flow = Flow(engine="local")
        stream = flow.from_collection([1, 2, 3])
        assert isinstance(stream, Stream)
        assert len(flow.sources) == 1

    def test_quick_flow(self):
        flow = quick_flow("test")
        assert flow.name == "test"


class TestSchema:
    def test_empty_schema(self):
        schema = Schema()
        assert len(schema.fields) == 0

    def test_add_field(self):
        schema = Schema().add("name", FieldType.STRING).add("age", FieldType.INTEGER)
        assert schema.field_names() == ["name", "age"]

    def test_from_dict(self):
        schema = Schema.from_dict({"name": FieldType.STRING, "value": FieldType.FLOAT})
        assert len(schema.fields) == 2

    def test_from_pairs(self):
        schema = Schema.from_pairs([("x", FieldType.DOUBLE), ("y", FieldType.DOUBLE)])
        assert schema.field_names() == ["x", "y"]

    def test_get_field(self):
        schema = Schema.from_dict({"name": FieldType.STRING})
        field = schema.get_field("name")
        assert field is not None
        assert field.field_type == FieldType.STRING

    def test_get_field_missing(self):
        schema = Schema()
        assert schema.get_field("missing") is None


class TestStream:
    def test_map(self):
        flow = Flow(engine="local")
        stream = flow.from_collection([1, 2, 3])
        result = stream.map(lambda x: x * 2)
        assert isinstance(result, Stream)
        assert len(result.operations()) == 1

    def test_filter(self):
        flow = Flow(engine="local")
        stream = flow.from_collection([1, 2, 3, 4])
        result = stream.filter(lambda x: x > 2)
        assert len(result.operations()) == 1

    def test_flat_map(self):
        flow = Flow(engine="local")
        stream = flow.from_collection(["hello world", "foo bar"])
        result = stream.flat_map(lambda s: s.split())
        assert len(result.operations()) == 1

    def test_key_by(self):
        flow = Flow(engine="local")
        stream = flow.from_collection([("a", 1), ("b", 2)])
        result = stream.key_by(lambda x: x[0])
        from liutang.core.stream import KeyedStream
        assert isinstance(result, KeyedStream)

    def test_chained_operations(self):
        flow = Flow(engine="local")
        stream = flow.from_collection(["hello world", "foo bar"])
        result = (
            stream
            .flat_map(lambda s: s.split())
            .filter(lambda w: len(w) > 3)
            .map(lambda w: (w, 1))
        )
        assert len(result.operations()) == 3


class TestWindow:
    def test_tumbling_window(self):
        w = WindowType.tumbling(size=10.0, time_field="ts")
        assert w.kind == WindowKind.TUMBLING
        assert w.size == 10.0

    def test_sliding_window(self):
        w = WindowType.sliding(size=10.0, slide=5.0, time_field="ts")
        assert w.kind == WindowKind.SLIDING
        assert w.slide == 5.0

    def test_session_window(self):
        w = WindowType.session(gap=30.0, time_field="ts")
        assert w.kind == WindowKind.SESSION
        assert w.gap == 30.0

    def test_over_window(self):
        w = WindowType.over(time_field="ts")
        assert w.kind == WindowKind.OVER


class TestConnector:
    def test_collection_source(self):
        src = CollectionSource(data=[1, 2, 3])
        assert src.kind().value == "collection"
        assert src.data == [1, 2, 3]

    def test_file_source(self):
        src = FileSource(path="/tmp/data.csv", fmt="csv")
        assert src.kind().value == "file"

    def test_print_sink(self):
        sink = PrintSink()
        assert sink.kind().value == "print"

    def test_callback_sink(self):
        results = []
        sink = CallbackSink(func=lambda x: results.append(x))
        assert sink.kind().value == "callback"


class TestState:
    def test_memory_backend(self):
        backend = MemoryStateBackend()
        backend.set_value("key", 42)
        assert backend.get_value("key") == 42

    def test_keyed_state(self):
        backend = MemoryStateBackend()
        state = KeyedState(backend, "user:1")
        state.value = 100
        assert state.value == 100

    def test_list_state(self):
        backend = MemoryStateBackend()
        backend.append_list("events", "event1")
        backend.append_list("events", "event2")
        assert backend.get_list("events") == ["event1", "event2"]

    def test_map_state(self):
        backend = MemoryStateBackend()
        backend.put_map("counts", "a", 1)
        backend.put_map("counts", "b", 2)
        assert backend.get_map("counts") == {"a": 1, "b": 2}


class TestLocalExecution:
    def test_batch_word_count(self):
        flow = Flow(name="wc", engine="local", mode=RuntimeMode.BATCH)
        stream = flow.from_collection(["hello world", "hello liutang"])
        result = (
            stream
            .flat_map(lambda line: line.split())
            .map(lambda w: (w, 1))
            .key_by(lambda pair: pair[0])
        )
        result.print()
        output = flow.execute()
        assert "source_0" in output
        data = output["source_0"]
        assert len(data) > 0

    def test_batch_filter(self):
        flow = Flow(name="filter", engine="local", mode=RuntimeMode.BATCH)
        stream = flow.from_collection([1, 2, 3, 4, 5, 6])
        result = stream.filter(lambda x: x > 3)
        result.print()
        output = flow.execute()
        data = output["source_0"]
        assert all(x > 3 for x in data)

    def test_batch_map(self):
        flow = Flow(name="map", engine="local", mode=RuntimeMode.BATCH)
        stream = flow.from_collection([1, 2, 3])
        result = stream.map(lambda x: x * 10)
        result.print()
        output = flow.execute()
        data = output["source_0"]
        assert sorted(data) == [10, 20, 30]

    def test_callback_sink(self):
        results = []
        flow = Flow(name="cb", engine="local", mode=RuntimeMode.BATCH)
        stream = flow.from_collection([1, 2, 3])
        result = stream.map(lambda x: x * 2)
        result.sink_to(CallbackSink(func=lambda x: results.append(x)))
        flow.execute()
        assert sorted(results) == [2, 4, 6]


class TestErrors:
    def test_unknown_engine(self):
        flow = Flow(engine="nonexistent")
        with pytest.raises(EngineNotAvailableError):
            flow.execute()

    def test_engine_not_installed(self):
        flow = Flow(engine="flink")
        with pytest.raises((EngineNotAvailableError, ImportError, ModuleNotFoundError)):
            flow.execute()

    def test_list_engines(self):
        engines = list_engines()
        assert "local" in engines
        assert "flink" in engines
        assert "spark" in engines

    def test_is_engine_available(self):
        assert is_engine_available("local") is True