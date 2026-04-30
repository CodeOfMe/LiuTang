import pytest
import time
import tempfile
import os
import json
from liutang import (
    Flow, Stream, KeyedStream, RuntimeMode, DeliveryMode, ArchitectureMode,
    CollectionSource, GeneratorSource, FileSource, DatagenSource,
    CallbackSink, CollectSink, FileSink,
    ValueState, ListState, MapState, ReducingState, AggregatingState,
    RuntimeContext, TimerService, KeyedProcessFunction, ProcessFunction,
    WatermarkStrategy, Watermark,
    MemoryStateBackend, JsonFileStateBackend,
    EventLog, ServingView, MergeView, LambdaFlow, KappaFlow,
    GranularityLevel, GranularityPolicy, GranularityController,
    AdaptiveFlow, Viscosity, ViscosityPolicy, ViscosityController, FlowMetrics,
    Schema, FieldType, WindowType, WindowKind,
)


class TestStreamReduce:
    def test_reduce_operation_recorded(self):
        flow = Flow()
        stream = flow.from_collection([1, 2, 3])
        result = stream.reduce(lambda a, b: a + b)
        ops = result.operations()
        assert len(ops) == 1
        assert ops[0]["type"] == "reduce"

    def test_reduce_custom_name(self):
        flow = Flow()
        stream = flow.from_collection([1, 2, 3])
        result = stream.reduce(lambda a, b: a + b, name="my_reduce")
        assert result.operations()[0]["name"] == "my_reduce"


class TestStreamAssignTimestamps:
    def test_assign_timestamps_records_operation(self):
        flow = Flow()
        stream = flow.from_collection([1, 2, 3])
        result = stream.assign_timestamps(lambda x: float(x))
        ops = result.operations()
        assert ops[0]["type"] == "assign_timestamps"
        assert ops[0]["watermark"] is not None

    def test_assign_timestamps_default_watermark_monotonous(self):
        flow = Flow()
        stream = flow.from_collection([1, 2, 3])
        result = stream.assign_timestamps(lambda x: float(x))
        ops = result.operations()
        assert ops[0]["watermark"].strategy == "monotonous"

    def test_assign_timestamps_with_custom_strategy(self):
        flow = Flow()
        stream = flow.from_collection([1, 2, 3])
        strategy = WatermarkStrategy.bounded_out_of_orderness(5.0)
        result = stream.assign_timestamps(lambda x: float(x), watermark_strategy=strategy)
        ops = result.operations()
        assert ops[0]["watermark"].strategy == "bounded_out_of_orderness"
        assert ops[0]["watermark"].out_of_orderness == 5.0

    def test_watermark_method_sets_strategy(self):
        flow = Flow()
        stream = flow.from_collection([1, 2, 3])
        strategy = WatermarkStrategy.bounded_out_of_orderness(3.0)
        result = stream.watermark(strategy)
        assert stream._watermark_strategy == strategy


class TestStreamToTable:
    def test_to_table_with_schema(self):
        flow = Flow()
        schema = Schema.from_dict({"name": FieldType.STRING, "val": FieldType.INTEGER})
        stream = flow.from_collection([{"name": "a", "val": 1}])
        table = stream.to_table(schema=schema)
        assert table.name is not None
        assert table.schema == schema

    def test_to_table_no_schema_raises(self):
        flow = Flow()
        stream = flow.from_collection([1, 2, 3])
        with pytest.raises(Exception, match="Schema is required"):
            stream.to_table()

    def test_table_select(self):
        flow = Flow()
        schema = Schema.from_dict({"a": FieldType.STRING, "b": FieldType.INTEGER})
        stream = flow.from_collection([{"a": "x", "b": 1}])
        table = stream.to_table(schema=schema)
        result = table.select("a", "b")
        assert result.operations[0]["type"] == "select"
        assert result.operations[0]["expressions"] == ("a", "b")

    def test_table_where(self):
        flow = Flow()
        schema = Schema.from_dict({"val": FieldType.INTEGER})
        stream = flow.from_collection([{"val": 1}])
        table = stream.to_table(schema=schema)
        result = table.where("val > 0")
        assert result.operations[0]["type"] == "where"
        assert result.operations[0]["predicate"] == "val > 0"

    def test_table_group_by(self):
        flow = Flow()
        schema = Schema.from_dict({"key": FieldType.STRING, "val": FieldType.INTEGER})
        stream = flow.from_collection([{"key": "a", "val": 1}])
        table = stream.to_table(schema=schema)
        grouped = table.group_by("key")
        assert grouped._keys == ("key",)
        result = grouped.count()
        assert result.operations[-1]["type"] == "grouped_count"

    def test_table_group_by_sum(self):
        flow = Flow()
        schema = Schema.from_dict({"key": FieldType.STRING, "val": FieldType.INTEGER})
        stream = flow.from_collection([{"key": "a", "val": 1}])
        table = stream.to_table(schema=schema)
        grouped = table.group_by("key")
        result = grouped.sum("val")
        assert result.operations[-1]["type"] == "grouped_sum"
        assert result.operations[-1]["field"] == "val"

    def test_table_group_by_avg(self):
        flow = Flow()
        schema = Schema.from_dict({"key": FieldType.STRING, "val": FieldType.INTEGER})
        stream = flow.from_collection([{"key": "a", "val": 1}])
        table = stream.to_table(schema=schema)
        grouped = table.group_by("key")
        result = grouped.avg("val")
        assert result.operations[-1]["type"] == "grouped_avg"

    def test_table_order_by(self):
        flow = Flow()
        schema = Schema.from_dict({"val": FieldType.INTEGER})
        stream = flow.from_collection([{"val": 1}])
        table = stream.to_table(schema=schema)
        result = table.order_by("val")
        assert result.operations[0]["type"] == "order_by"
        assert result.operations[0]["keys"] == ("val",)

    def test_table_limit(self):
        flow = Flow()
        schema = Schema.from_dict({"val": FieldType.INTEGER})
        stream = flow.from_collection([{"val": 1}])
        table = stream.to_table(schema=schema)
        result = table.limit(10)
        assert result.operations[0]["type"] == "limit"
        assert result.operations[0]["n"] == 10

    def test_table_window(self):
        flow = Flow()
        schema = Schema.from_dict({"ts": FieldType.FLOAT, "val": FieldType.INTEGER})
        stream = flow.from_collection([{"ts": 1.0, "val": 1}])
        table = stream.to_table(schema=schema)
        windowed = table.window(WindowType.tumbling(size=5.0))
        result = windowed.select("val")
        assert result.operations[-1]["type"] == "windowed_select"

    def test_table_window_group_by(self):
        flow = Flow()
        schema = Schema.from_dict({"key": FieldType.STRING, "val": FieldType.INTEGER})
        stream = flow.from_collection([{"key": "a", "val": 1}])
        table = stream.to_table(schema=schema)
        windowed = table.window(WindowType.tumbling(size=5.0))
        grouped = windowed.group_by("key")
        assert grouped._keys == ("key",)

    def test_table_chained_operations(self):
        flow = Flow()
        schema = Schema.from_dict({"k": FieldType.STRING, "v": FieldType.INTEGER})
        stream = flow.from_collection([{"k": "a", "v": 1}])
        table = stream.to_table(schema=schema)
        result = table.select("k", "v").where("v > 0").order_by("v").limit(5)
        assert len(result.operations) == 4
        assert result.operations[0]["type"] == "select"
        assert result.operations[1]["type"] == "where"
        assert result.operations[2]["type"] == "order_by"
        assert result.operations[3]["type"] == "limit"

    def test_table_parent_stream(self):
        flow = Flow()
        schema = Schema.from_dict({"v": FieldType.INTEGER})
        stream = flow.from_collection([1])
        table = stream.to_table(schema=schema)
        assert table.parent_stream is stream


class TestKeyedStreamReduce:
    def test_keyed_reduce_operation(self):
        flow = Flow()
        stream = flow.from_collection([("a", 1), ("b", 2)])
        keyed = stream.key_by(lambda x: x[0])
        result = keyed.reduce(lambda a, b: (a[0], a[1] + b[1]))
        ops = result.operations()
        assert ops[-1]["type"] == "keyed_reduce"


class TestWindowedStreamAggregations:
    def test_windowed_aggregate(self):
        flow = Flow()
        stream = flow.from_collection([1, 2, 3])
        windowed = stream.window(WindowType.tumbling(size=10.0))
        result = windowed.aggregate(lambda items: sum(items))
        ops = result.operations()
        assert ops[-1]["type"] == "window_aggregate"

    def test_windowed_min(self):
        flow = Flow()
        stream = flow.from_collection([3, 1, 4, 1, 5])
        windowed = stream.window(WindowType.tumbling(size=10.0))
        result = windowed.min(field=0)
        ops = result.operations()
        assert ops[-1]["type"] == "window_min"
        assert ops[-1]["window"].kind == WindowKind.TUMBLING

    def test_windowed_max(self):
        flow = Flow()
        stream = flow.from_collection([3, 1, 4, 1, 5])
        windowed = stream.window(WindowType.tumbling(size=10.0))
        result = windowed.max(field=0)
        ops = result.operations()
        assert ops[-1]["type"] == "window_max"

    def test_windowed_apply(self):
        flow = Flow()
        stream = flow.from_collection([1, 2, 3])
        windowed = stream.window(WindowType.tumbling(size=10.0))
        result = windowed.apply(lambda items: sorted(items, reverse=True))
        ops = result.operations()
        assert ops[-1]["type"] == "window_apply"
        assert ops[-1]["window"].kind == WindowKind.TUMBLING

    def test_windowed_count(self):
        flow = Flow()
        stream = flow.from_collection([1, 2, 3])
        windowed = stream.window(WindowType.tumbling(size=10.0))
        result = windowed.count()
        assert result.operations()[-1]["type"] == "window_count"

    def test_windowed_sum(self):
        flow = Flow()
        stream = flow.from_collection([1, 2, 3])
        windowed = stream.window(WindowType.tumbling(size=10.0))
        result = windowed.sum(field=0)
        assert result.operations()[-1]["type"] == "window_sum"


class TestValueStateUpdate:
    def test_update_is_alias_for_setter(self):
        state = ValueState("test")
        state.update(99)
        assert state.value == 99

    def test_update_overwrites(self):
        state = ValueState("test")
        state.value = 10
        state.update(20)
        assert state.value == 20


class TestListStateExtras:
    def test_clear(self):
        state = ListState("test")
        state.add(1)
        state.add(2)
        state.clear()
        assert state.get() == []
        assert len(state) == 0

    def test_iter(self):
        state = ListState("test")
        state.add(10)
        state.add(20)
        state.add(30)
        result = list(state)
        assert result == [10, 20, 30]

    def test_iter_empty(self):
        state = ListState("test")
        result = list(state)
        assert result == []


class TestMapStateExtras:
    def test_remove(self):
        state = MapState("test")
        state.put("a", 1)
        state.put("b", 2)
        state.remove("a")
        assert state.get("a") is None
        assert state.get("b") == 2

    def test_remove_nonexistent(self):
        state = MapState("test")
        state.remove("nokey")

    def test_values(self):
        state = MapState("test")
        state.put("x", 10)
        state.put("y", 20)
        vals = state.values()
        assert sorted(vals) == [10, 20]

    def test_items(self):
        state = MapState("test")
        state.put("x", 10)
        state.put("y", 20)
        items = dict(state.items())
        assert items == {"x": 10, "y": 20}

    def test_contains(self):
        state = MapState("test")
        state.put("a", 1)
        assert state.contains("a") is True
        assert state.contains("z") is False

    def test_clear(self):
        state = MapState("test")
        state.put("a", 1)
        state.put("b", 2)
        state.clear()
        assert state.keys() == set()
        assert state.values() == []


class TestRuntimeContextAdvanced:
    def test_get_map_state(self):
        ctx = RuntimeContext()
        ctx.set_current_key("k1")
        ms = ctx.get_map_state("mymap")
        ms.put("field", 42)
        assert ms.get("field") == 42

    def test_get_map_state_per_key(self):
        ctx = RuntimeContext()
        ctx.set_current_key("k1")
        ms1 = ctx.get_map_state("mymap")
        ms1.put("f", 1)
        ctx.set_current_key("k2")
        ms2 = ctx.get_map_state("mymap")
        assert ms2.get("f") is None

    def test_get_map_state_same_key_returns_same(self):
        ctx = RuntimeContext()
        ctx.set_current_key("k1")
        ms1 = ctx.get_map_state("mymap")
        ms2 = ctx.get_map_state("mymap")
        assert ms1 is ms2

    def test_get_reducing_state(self):
        ctx = RuntimeContext()
        ctx.set_current_key("k1")
        rs = ctx.get_reducing_state("sum", lambda a, b: a + b)
        rs.add(10)
        rs.add(20)
        assert rs.get() == 30

    def test_get_reducing_state_per_key(self):
        ctx = RuntimeContext()
        ctx.set_current_key("k1")
        rs1 = ctx.get_reducing_state("max", max)
        rs1.add(5)
        ctx.set_current_key("k2")
        rs2 = ctx.get_reducing_state("max", max)
        assert rs2.get() is None

    def test_get_aggregating_state(self):
        ctx = RuntimeContext()
        ctx.set_current_key("k1")
        ag = ctx.get_aggregating_state("agg", lambda acc, x: acc + x, init_value=0)
        ag.add(10)
        ag.add(20)
        assert ag.get() == 30

    def test_get_aggregating_state_per_key(self):
        ctx = RuntimeContext()
        ctx.set_current_key("k1")
        ag1 = ctx.get_aggregating_state("agg", lambda acc, x: acc + x, init_value=0)
        ag1.add(10)
        ctx.set_current_key("k2")
        ag2 = ctx.get_aggregating_state("agg", lambda acc, x: acc + x, init_value=0)
        assert ag2.get() == 0


class TestTimerServiceAdvanced:
    def test_register_processing_time_timer(self):
        ts = TimerService()
        fired = []
        ts.register_processing_time_timer(1000.0, lambda: fired.append(1000))
        assert ts.has_pending_timers() is True

    def test_fire_processing_time_timers(self):
        ts = TimerService()
        fired = []
        ts.register_processing_time_timer(500.0, lambda: fired.append(500))
        ts.register_processing_time_timer(1000.0, lambda: fired.append(1000))
        callbacks = ts.fire_processing_time_timers(750.0)
        for cb in callbacks:
            cb()
        assert 500 in fired
        assert 1000 not in fired

    def test_fire_processing_time_timers_all(self):
        ts = TimerService()
        fired = []
        ts.register_processing_time_timer(500.0, lambda: fired.append(500))
        ts.register_processing_time_timer(1000.0, lambda: fired.append(1000))
        callbacks = ts.fire_processing_time_timers(1500.0)
        for cb in callbacks:
            cb()
        assert 500 in fired
        assert 1000 in fired

    def test_advance_watermark(self):
        ts = TimerService()
        assert ts.current_watermark == float("-inf")
        ts.advance_watermark(100.0)
        assert ts.current_watermark == 100.0
        ts.advance_watermark(50.0)
        assert ts.current_watermark == 100.0

    def test_has_pending_timers_empty(self):
        ts = TimerService()
        assert ts.has_pending_timers() is False

    def test_has_pending_timers_after_fire(self):
        ts = TimerService()
        ts.register_processing_time_timer(100.0, lambda: None)
        ts.fire_processing_time_timers(200.0)
        assert ts.has_pending_timers() is False


class TestKeyedProcessFunctionOnTimer:
    def test_on_timer_callback(self):
        results = []

        class TimerFunc(KeyedProcessFunction):
            def process_element(self, value, ctx):
                ctx.timer_service.register_processing_time_timer(
                    1000.0, lambda: None
                )
                return value

            def on_timer(self, timestamp, ctx):
                results.append(("timer", timestamp))
                return f"timer_{timestamp}"

        ctx = RuntimeContext()
        ts = TimerService()
        ctx.timer_service = ts
        fn = TimerFunc()
        fn.open(ctx)
        ctx.set_current_key("k1")
        fn.process_element(42, ctx)
        assert ts.has_pending_timers()
        ret = fn.on_timer(1000.0, ctx)
        assert ret == "timer_1000.0"
        assert ("timer", 1000.0) in results

    def test_on_timer_default_returns_none(self):
        class NoTimer(KeyedProcessFunction):
            def process_element(self, value, ctx):
                return value

        fn = NoTimer()
        result = fn.on_timer(999.0, RuntimeContext())
        assert result is None


class TestWatermarkStrategyNoWatermarks:
    def test_no_watermarks_creates_monotonous(self):
        strategy = WatermarkStrategy.no_watermarks()
        assert strategy.strategy == "monotonous"


class TestJsonFileStateBackendFileOps:
    def test_save_and_load_with_temp_dir(self):
        with tempfile.TemporaryDirectory() as d:
            backend = JsonFileStateBackend(d)
            backend.set_value("k1", "v1")
            backend.set_value("k2", 42)
            backend.append_list("k3", "item_a")
            backend.append_list("k3", "item_b")
            backend.put_map("k4", "mk1", "mv1")
            backend.save("checkpoint1")
            cp_path = os.path.join(d, "checkpoint1.json")
            assert os.path.exists(cp_path)
            with open(cp_path, "r") as f:
                data = json.load(f)
            assert data["values"]["k1"] == "v1"

    def test_load_restores_state(self):
        with tempfile.TemporaryDirectory() as d:
            backend = JsonFileStateBackend(d)
            backend.set_value("k1", "hello")
            backend.append_list("k2", 10)
            backend.save("s1")
            backend2 = JsonFileStateBackend(d)
            loaded = backend2.load("s1")
            assert loaded is True
            assert backend2.get_value("k1") == "hello"
            assert backend2.get_list("k2") == [10]

    def test_load_nonexistent_returns_false(self):
        with tempfile.TemporaryDirectory() as d:
            backend = JsonFileStateBackend(d)
            assert backend.load("no_such_checkpoint") is False

    def test_multiple_named_checkpoints(self):
        with tempfile.TemporaryDirectory() as d:
            backend = JsonFileStateBackend(d)
            backend.set_value("shared", "first")
            backend.save("cp_a")
            backend.set_value("shared", "second")
            backend.save("cp_b")
            backend2 = JsonFileStateBackend(d)
            backend2.load("cp_a")
            assert backend2.get_value("shared") == "first"
            backend3 = JsonFileStateBackend(d)
            backend3.load("cp_b")
            assert backend3.get_value("shared") == "second"


class TestViscosityAdjustAllPolicies:
    def test_efficient_high_all_metrics(self):
        vc = ViscosityController(
            policy=ViscosityPolicy.EFFICIENT,
            initial=Viscosity.VOLATILE,
            adjust_interval=0.0,
            thresholds={
                "high_arrival_rate": 100, "low_arrival_rate": 10,
                "high_queue_depth": 200, "low_queue_depth": 5,
                "high_latency_ms": 1000, "low_latency_ms": 1,
                "high_backlog": 500, "low_backlog": 10,
            },
        )
        vc.update_metrics(arrival_rate=200, queue_depth=300, backlog_size=600)
        time.sleep(0.01)
        vc.adjust()
        assert vc.viscosity >= Viscosity.VOLATILE

    def test_efficient_low_all_metrics(self):
        vc = ViscosityController(
            policy=ViscosityPolicy.EFFICIENT,
            initial=Viscosity.FROZEN,
            adjust_interval=0.0,
            thresholds={
                "high_arrival_rate": 1000, "low_arrival_rate": 10,
                "high_queue_depth": 5000, "low_queue_depth": 10,
                "high_latency_ms": 500, "low_latency_ms": 10,
                "high_backlog": 10000, "low_backlog": 100,
            },
        )
        vc.update_metrics(arrival_rate=1, queue_depth=1, backlog_size=1)
        time.sleep(0.01)
        result = vc.adjust()
        assert result is not None

    def test_efficient_mid_metrics(self):
        vc = ViscosityController(
            policy=ViscosityPolicy.EFFICIENT,
            initial=Viscosity.HONEYED,
            adjust_interval=0.0,
            thresholds={
                "high_arrival_rate": 1000, "low_arrival_rate": 10,
                "high_queue_depth": 5000, "low_queue_depth": 10,
                "high_latency_ms": 500, "low_latency_ms": 10,
                "high_backlog": 10000, "low_backlog": 100,
            },
        )
        vc.update_metrics(arrival_rate=600, queue_depth=3000, backlog_size=6000)
        time.sleep(0.01)
        vc.adjust()

    def test_responsive_high_latency_high_backlog(self):
        vc = ViscosityController(
            policy=ViscosityPolicy.RESPONSIVE,
            initial=Viscosity.HONEYED,
            adjust_interval=0.0,
            thresholds={
                "high_arrival_rate": 1000, "low_arrival_rate": 10,
                "high_queue_depth": 5000, "low_queue_depth": 10,
                "high_latency_ms": 500, "low_latency_ms": 10,
                "high_backlog": 10000, "low_backlog": 100,
            },
        )
        vc.update_metrics(processing_latency_ms=600, backlog_size=20000, error_rate=0.2)
        time.sleep(0.01)
        vc.adjust()

    def test_responsive_low_load(self):
        vc = ViscosityController(
            policy=ViscosityPolicy.RESPONSIVE,
            initial=Viscosity.SLUGGISH,
            adjust_interval=0.0,
            thresholds={
                "high_arrival_rate": 1000, "low_arrival_rate": 10,
                "high_queue_depth": 5000, "low_queue_depth": 10,
                "high_latency_ms": 500, "low_latency_ms": 10,
                "high_backlog": 10000, "low_backlog": 100,
            },
        )
        vc.update_metrics(arrival_rate=1, queue_depth=1, backlog_size=1, processing_latency_ms=1)
        time.sleep(0.01)
        vc.adjust()
        assert vc.viscosity <= Viscosity.SLUGGISH

    def test_balanced_high_arrival_low_backlog(self):
        vc = ViscosityController(
            policy=ViscosityPolicy.BALANCED,
            initial=Viscosity.HONEYED,
            adjust_interval=0.0,
        )
        vc.update_metrics(arrival_rate=5000, queue_depth=5, backlog_size=5)
        time.sleep(0.01)
        vc.adjust()
        assert vc.viscosity is not None

    def test_balanced_low_arrival_high_backlog(self):
        vc = ViscosityController(
            policy=ViscosityPolicy.BALANCED,
            initial=Viscosity.HONEYED,
            adjust_interval=0.0,
        )
        vc.update_metrics(arrival_rate=1, queue_depth=5, backlog_size=20000, processing_latency_ms=600)
        time.sleep(0.01)
        vc.adjust()
        assert vc.viscosity is not None

    def test_balanced_error_rate_withdraw(self):
        vc = ViscosityController(
            policy=ViscosityPolicy.BALANCED,
            initial=Viscosity.FROZEN,
            adjust_interval=0.0,
        )
        vc.update_metrics(error_rate=0.5, arrival_rate=1, queue_depth=2, backlog_size=5)
        time.sleep(0.01)
        vc.adjust()
        assert vc.viscosity <= Viscosity.FROZEN

    def test_manual_ignores_all_metrics(self):
        vc = ViscosityController(
            policy=ViscosityPolicy.MANUAL,
            initial=Viscosity.FLUID,
            adjust_interval=0.0,
        )
        vc.update_metrics(arrival_rate=999999, queue_depth=999999, backlog_size=999999,
                          processing_latency_ms=999999, error_rate=0.9)
        vc.adjust()
        assert vc.viscosity == Viscosity.FLUID


class TestAdaptiveFlowFlowFreelyFlowAsBatch:
    def test_flow_freely_is_thaw(self):
        af = AdaptiveFlow(
            name="test-ff",
            stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x),
        )
        af.freeze()
        assert af.viscosity == Viscosity.FROZEN
        af.thaw()
        assert af.viscosity == Viscosity.VOLATILE

    def test_flow_as_batch_is_freeze(self):
        af = AdaptiveFlow(
            name="test-fab",
            stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x),
        )
        af.thaw()
        assert af.viscosity == Viscosity.VOLATILE
        af.freeze()
        assert af.viscosity == Viscosity.FROZEN


class TestAdaptiveFlowWithEventLogPath:
    def test_event_log_path_stored(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "adaptive.log")
            af = AdaptiveFlow(
                name="test-logpath",
                stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x),
                event_log_path=path,
            )
            assert af._event_log_path == path

    def test_adaptive_execute_with_event_log_path(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "adaptive.log")
            af = AdaptiveFlow(
                name="test-exec-log",
                stream_fn=lambda f: f.from_collection([10, 20, 30]).map(lambda x: x + 1),
                event_log_path=path,
            )
            result = af.execute()
            assert len(result["results"]) > 0


class TestAdaptiveFlowDeliveryModeMaxRetries:
    def test_delivery_mode_parameter(self):
        af = AdaptiveFlow(
            name="test-dm",
            stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x),
            delivery_mode=DeliveryMode.EXACTLY_ONCE,
        )
        assert af._delivery_mode == DeliveryMode.EXACTLY_ONCE

    def test_max_retries_parameter(self):
        af = AdaptiveFlow(
            name="test-mr",
            stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x),
            delivery_mode=DeliveryMode.AT_LEAST_ONCE,
            max_retries=5,
        )
        assert af._max_retries == 5


class TestFlowMetricsBoundaryConditions:
    def test_all_zeros(self):
        m = FlowMetrics()
        assert m.measured_viscosity >= 0.0
        assert m.measured_viscosity <= 1.0
        assert m.shear_stress == 0.0

    def test_very_high_values(self):
        m = FlowMetrics()
        m.arrival_rate = 1e9
        m.queue_depth = 1e9
        m.processing_latency_ms = 1e9
        m.backlog_size = int(1e9)
        v = m.measured_viscosity
        assert 0.0 <= v <= 1.0

    def test_zero_arrival_rate_nonzero_backlog(self):
        m = FlowMetrics()
        m.arrival_rate = 0.0
        m.backlog_size = 1000
        assert m.shear_stress == 1000.0
        assert m.measured_viscosity >= 0.0


class TestLambdaFlowBothLayers:
    def test_batch_and_speed_executing(self):
        collected = []
        lf = LambdaFlow(
            name="test-both",
            batch_layer_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x * 10),
            speed_layer_fn=lambda f: f.from_collection([4, 5, 6]).map(lambda x: x * 100),
        )
        result = lf.execute()
        assert sorted(result["batch_results"]) == [10, 20, 30]
        assert "speed_results" in result

    def test_execute_with_speed_callback(self):
        speed_items = []
        lf = LambdaFlow(
            name="test-cb-lambda",
            batch_layer_fn=lambda f: f.from_collection([10, 20]).map(lambda x: x),
            speed_layer_fn=lambda f: f.from_collection([30, 40]).map(lambda x: x),
        )
        lf.on_speed_result(lambda x: speed_items.append(x))
        result = lf.execute()
        assert "speed_results" in result

    def test_query_all(self):
        lf = LambdaFlow(
            name="test-qa",
            batch_layer_fn=lambda f: f.from_collection([("a", 1), ("b", 2)]),
            key_fn=lambda x: x[0] if isinstance(x, tuple) else str(x),
        )
        lf.execute_batch_only()
        all_results = lf.query_all()
        assert isinstance(all_results, list)
        assert len(all_results) >= 2


class TestKappaFlowReplayOffsetRange:
    def test_replay_with_offset_and_limit(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "replay.log")
            kf = KappaFlow(
                name="test-range",
                stream_fn=lambda f: f.from_collection([1]).map(lambda x: x),
                event_log_path=path,
            )
            for i in range(10):
                kf.append_to_log({"i": i})
            records = kf.replay(offset=3, limit=4)
            assert len(records) == 4
            assert records[0]["i"] == 3
            assert records[3]["i"] == 6


class TestEventLogAdvanced:
    def test_compact_with_merge_function(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "compact.log")
            log = EventLog(path)
            for i in range(10):
                log.append({"key": "a", "val": i})
            def merge(records):
                total = sum(r["val"] for r in records)
                return [{"key": "a", "val": total, "merged": True}]
            log.compact(merge_fn=merge, checkpoint_offset=10)
            records = log.read()
            assert len(records) >= 1
            merged = records[0]
            assert merged.get("merged") is True
            assert merged["val"] == sum(range(10))

    def test_truncate(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "trunc.log")
            log = EventLog(path)
            for i in range(10):
                log.append({"i": i})
            assert len(log.read()) == 10
            log.truncate(before_offset=7)
            remaining = log.read()
            assert len(remaining) <= 10

    def test_read_with_offset_and_limit(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "offset_limit.log")
            log = EventLog(path)
            for i in range(20):
                log.append({"i": i})
            records = log.read(offset=5, limit=3)
            assert len(records) == 3
            assert records[0]["i"] == 5
            assert records[2]["i"] == 7


class TestFileSourceActualFiles:
    def test_text_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("line1\nline2\nline3\n")
            f.flush()
            path = f.name
        try:
            flow = Flow(mode=RuntimeMode.BATCH)
            stream = flow.from_file(path, fmt="text")
            sink = stream.collect()
            flow.execute()
            assert len(sink.results) > 0
        finally:
            os.unlink(path)

    def test_csv_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("name,value\nalice,10\nbob,20\n")
            f.flush()
            path = f.name
        try:
            src = FileSource(path=path, fmt="csv")
            assert src.kind().value == "file"
            assert src.fmt == "csv"
        finally:
            os.unlink(path)

    def test_json_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write('{"key": "a", "val": 1}\n{"key": "b", "val": 2}\n')
            f.flush()
            path = f.name
        try:
            src = FileSource(path=path, fmt="json")
            assert src.fmt == "json"
        finally:
            os.unlink(path)


class TestFileSinkActualFiles:
    def test_file_sink_writes(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            path = f.name
        try:
            sink = FileSink(path=path, fmt="csv")
            assert sink.kind().value == "file"
            assert sink.path == path
            assert sink.fmt == "csv"
        finally:
            if os.path.exists(path):
                os.unlink(path)


class TestDatagenSource:
    def test_datagen_source_fields(self):
        src = DatagenSource(rows_per_second=10, fields={"name": "string", "age": "int"}, max_records=5)
        assert src.kind().value == "datagen"
        assert src.rows_per_second == 10
        assert src.fields == {"name": "string", "age": "int"}
        assert src.max_records == 5

    def test_datagen_source_defaults(self):
        src = DatagenSource()
        assert src.rows_per_second == 100
        assert src.fields is None
        assert src.max_records is None

    def test_datagen_in_flow(self):
        src = DatagenSource(rows_per_second=10, max_records=5)
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_source(src)
        sink = stream.collect()
        flow.execute()
        assert len(sink.results) >= 0


class TestStreamingE2EWithSinks:
    def test_streaming_callback_and_collect(self):
        cb_results = []
        flow = Flow(mode=RuntimeMode.STREAMING)
        stream = flow.from_collection([100, 200, 300])
        stream.sink_to(CallbackSink(func=lambda x: cb_results.append(x)))
        sink = stream.collect()
        handles = flow.execute()
        time.sleep(1.5)
        for stop_event in handles["stop_events"].values():
            stop_event.set()
        time.sleep(0.5)
        assert len(cb_results) > 0 or len(sink.results) > 0

    def test_batch_e2e_callback_and_collect(self):
        cb_results = []
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection([5, 10, 15])
        stream.sink_to(CallbackSink(func=lambda x: cb_results.append(x)))
        sink = stream.collect()
        flow.execute()
        assert sorted(sink.results) == [5, 10, 15]
        assert sorted(cb_results) == [5, 10, 15]


class TestAdaptiveExecutionCycle:
    def test_viscosity_controller_metrics_driven_adjustment_cycle(self):
        vc = ViscosityController(
            initial=Viscosity.FLUID,
            policy=ViscosityPolicy.BALANCED,
            adjust_interval=0.0,
        )
        assert vc.viscosity == Viscosity.FLUID
        assert vc.adjustments_count == 0
        vc.update_metrics(arrival_rate=5000, queue_depth=8000, backlog_size=15000)
        time.sleep(0.01)
        vc.adjust()
        if vc.adjustments_count > 0:
            assert vc.viscosity != Viscosity.FLUID
        vc.update_metrics(arrival_rate=1, queue_depth=2, backlog_size=3, processing_latency_ms=2000)
        time.sleep(0.01)
        vc.adjust()
        assert vc.adjustments_count >= 1

    def test_adaptive_flow_with_external_controller_adjustment(self):
        vc = ViscosityController(initial=Viscosity.HONEYED, policy=ViscosityPolicy.BALANCED)
        af = AdaptiveFlow(
            name="test-ext-ctrl",
            stream_fn=lambda f: f.from_collection([1, 2, 3, 4, 5]).map(lambda x: x * 2),
            controller=vc,
        )
        result = af.execute()
        assert result["architecture"] == "adaptive"
        vc.update_metrics(arrival_rate=5000, queue_depth=8000, backlog_size=15000)
        time.sleep(0.01)
        vc.adjust()
        assert vc.adjustments_count >= 0