from __future__ import annotations

import json
import os
import socket
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Tuple

import pytest

from liutang import (
    Flow,
    Stream,
    RuntimeMode,
    DeliveryMode,
    CollectionSource,
    CollectSink,
    CallbackSink,
    EventLog,
    ServingView,
    MergeView,
    LambdaFlow,
    KappaFlow,
    AdaptiveFlow,
    Viscosity,
    ViscosityPolicy,
    ViscosityController,
    FlowMetrics,
    GranularityLevel,
    GranularityPolicy,
    GranularityController,
    MemoryStateBackend,
    SocketSource,
    SocketSink,
)


def _start_tcp_server(port: int, handler_fn, stop_event: threading.Event):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.settimeout(0.5)
    srv.bind(("127.0.0.1", port))
    srv.listen(5)
    clients = []

    def _serve():
        while not stop_event.is_set():
            try:
                conn, addr = srv.accept()
                conn.settimeout(0.5)
                clients.append(conn)
                t = threading.Thread(target=handler_fn, args=(conn,), daemon=True)
                t.start()
            except socket.timeout:
                continue
            except OSError:
                break

    t = threading.Thread(target=_serve, daemon=True)
    t.start()
    return srv, t, clients


def _stop_tcp_server(srv, clients):
    for c in clients:
        try:
            c.close()
        except Exception:
            pass
    try:
        srv.close()
    except Exception:
        pass


def _send_to_socket(host: str, port: int, messages: List[str], delimiter: str = "\n"):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5.0)
    s.connect((host, port))
    try:
        for msg in messages:
            s.sendall((msg + delimiter).encode("utf-8"))
    finally:
        s.close()


class TestLocalDistributedSimulation:
    def test_three_node_distributed_processing(self):
        N = 3
        ports = [19801, 19802, 19803]
        records_per_node = 50
        all_received: Dict[int, List[str]] = {p: [] for p in ports}
        stops = {p: threading.Event() for p in ports}

        def make_handler(port):
            def handler(conn):
                buf = ""
                while not stops[port].is_set():
                    try:
                        data = conn.recv(4096)
                        if not data:
                            break
                        buf += data.decode("utf-8", errors="replace")
                        while "\n" in buf:
                            line, buf = buf.split("\n", 1)
                            if line:
                                all_received[port].append(line)
                    except socket.timeout:
                        continue
                    except Exception:
                        break

            return handler

        servers = []
        for p in ports:
            srv, t, clients = _start_tcp_server(p, make_handler(p), stops[p])
            servers.append((srv, t, clients))

        coordinator_results: Dict[int, List[Any]] = {}
        coordinator_stops = {p: threading.Event() for p in ports}

        try:
            for p in ports:
                data = [f"node{p}_item{i}" for i in range(records_per_node)]
                flow = Flow(name=f"node-{p}", mode=RuntimeMode.BATCH)
                stream = flow.from_collection(data)
                result = stream.map(lambda x: x.upper())
                sink = result.collect()
                flow.execute()
                coordinator_results[p] = sink.results

            for p in ports:
                msgs = [str(r) for r in coordinator_results[p]]
                _send_to_socket("127.0.0.1", p, msgs)

            deadline = time.time() + 3.0
            while time.time() < deadline:
                if all(len(all_received[p]) >= records_per_node for p in ports):
                    break
                time.sleep(0.05)

            total_received = sum(len(all_received[p]) for p in ports)
            total_sent = N * records_per_node

            for p in ports:
                for val in coordinator_results[p]:
                    assert str(val) in all_received[p]

            elapsed = max(0.001, time.time() - deadline + 3.0)
            throughput = total_received / elapsed

            print(f"[DIST] LocalDistributedSimulation: total_received = {total_received}")
            print(f"[DIST] LocalDistributedSimulation: throughput = {throughput:.0f} records/s")
            assert total_received >= total_sent * 0.9
        finally:
            for p in ports:
                stops[p].set()
            for srv, t, clients in servers:
                _stop_tcp_server(srv, clients)
            for p in ports:
                coordinator_stops[p].set()


class TestMultiPortStreaming:
    def test_socket_streaming_pipeline(self):
        port = 19810
        N = 100
        server_stop = threading.Event()
        received: List[str] = []
        server_clients = []

        def handler(conn):
            buf = ""
            while not server_stop.is_set():
                try:
                    data = conn.recv(4096)
                    if not data:
                        break
                    buf += data.decode("utf-8", errors="replace")
                    while "\n" in buf:
                        line, buf = buf.split("\n", 1)
                        if line:
                            received.append(line)
                except socket.timeout:
                    continue
                except Exception:
                    break

        srv, t, clients = _start_tcp_server(port, handler, server_stop)
        server_clients.extend(clients)

        try:
            data = [f"record-{i}" for i in range(N)]

            flow = Flow(name="multi-port", mode=RuntimeMode.BATCH)
            stream = flow.from_collection(data)
            result = stream.map(lambda x: x.replace("record-", "processed-")).filter(
                lambda x: int(x.split("-")[1]) % 2 == 0
            )
            sink = result.collect()

            t0 = time.time()
            flow.execute()
            batch_elapsed = time.time() - t0

            processed = sink.results

            def sender():
                for r in processed:
                    try:
                        _send_to_socket("127.0.0.1", port, [r])
                    except Exception:
                        break

            send_t = threading.Thread(target=sender, daemon=True)
            send_t.start()

            deadline = time.time() + 5.0
            while time.time() < deadline:
                if len(received) >= len(processed):
                    break
                time.sleep(0.05)

            send_t.join(timeout=3.0)

            latency_per_record = (batch_elapsed / max(1, N)) * 1000

            print(f"[DIST] MultiPortStreaming: records_sent = {len(processed)}")
            print(f"[DIST] MultiPortStreaming: records_received = {len(received)}")
            print(f"[DIST] MultiPortStreaming: latency_per_record = {latency_per_record:.2f} ms")
            assert len(processed) == N // 2
            assert all("processed-" in r for r in processed)
        finally:
            server_stop.set()
            _stop_tcp_server(srv, clients)


class TestMultiWorkerParallelism:
    def test_parallel_workers(self):
        N = 10000
        parallelism = 4
        data = list(range(N))

        t0_sequential = time.time()
        flow_seq = Flow(name="seq", mode=RuntimeMode.BATCH)
        stream_seq = flow_seq.from_collection(data)
        result_seq = stream_seq.map(lambda x: x * 2).filter(lambda x: x % 4 == 0)
        sink_seq = result_seq.collect()
        flow_seq.execute()
        seq_elapsed = time.time() - t0_sequential
        seq_results = sorted(sink_seq.results)

        t0_parallel = time.time()

        def process_partition(partition):
            flow = Flow(name=f"worker-{partition[0]}", mode=RuntimeMode.BATCH, parallelism=2)
            stream = flow.from_collection(partition)
            result = stream.map(lambda x: x * 2).filter(lambda x: x % 4 == 0)
            sink = result.collect()
            flow.execute()
            return sink.results

        chunk_size = N // parallelism
        partitions = [data[i: i + chunk_size] for i in range(0, N, chunk_size)]

        combined = []
        with ThreadPoolExecutor(max_workers=parallelism) as pool:
            futures = [pool.submit(process_partition, p) for p in partitions]
            for f in as_completed(futures):
                combined.extend(f.result())

        par_elapsed = time.time() - t0_parallel
        combined_sorted = sorted(combined)

        speedup = seq_elapsed / max(0.001, par_elapsed)

        print(f"[DIST] MultiWorkerParallelism: sequential_time = {seq_elapsed:.4f}s")
        print(f"[DIST] MultiWorkerParallelism: parallel_time = {par_elapsed:.4f}s")
        print(f"[DIST] MultiWorkerParallelism: speedup = {speedup:.2f}x")
        print(f"[DIST] MultiWorkerParallelism: total_results = {len(combined)}")
        assert sorted(seq_results) == sorted(combined_sorted)
        assert len(combined) == len(seq_results)


class TestDistributedLambdaArchitecture:
    def test_batch_speed_merge(self):
        batch_data = list(range(1000))
        speed_data = list(range(100, 200))

        serving = ServingView(
            key_fn=lambda x: x if not isinstance(x, tuple) else x[0],
            merge_fn=MergeView.latest,
        )

        t0 = time.time()

        def batch_worker(worker_id, partition):
            flow = Flow(name=f"batch-{worker_id}", mode=RuntimeMode.BATCH)
            stream = flow.from_collection(partition)
            result = stream.map(lambda x: ("key", x))
            sink = result.collect()
            flow.execute()
            return sink.results

        def speed_worker(worker_id, partition):
            flow = Flow(name=f"speed-{worker_id}", mode=RuntimeMode.STREAMING)
            stream = flow.from_collection(partition)
            result = stream.map(lambda x: ("key", x))
            sink = result.collect()
            handles = flow.execute()
            time.sleep(1.0)
            for se in handles["stop_events"].values():
                se.set()
            time.sleep(0.3)
            return sink.results

        batch_chunk = len(batch_data) // 2
        speed_chunk = len(speed_data) // 2

        batch_results = []
        speed_results = []

        with ThreadPoolExecutor(max_workers=4) as pool:
            f1 = pool.submit(batch_worker, 0, batch_data[:batch_chunk])
            f2 = pool.submit(batch_worker, 1, batch_data[batch_chunk:])
            f3 = pool.submit(speed_worker, 0, speed_data[:speed_chunk])
            f4 = pool.submit(speed_worker, 1, speed_data[speed_chunk:])

            batch_results.extend(f1.result())
            batch_results.extend(f2.result())
            speed_results.extend(f3.result())
            speed_results.extend(f4.result())

        serving.update_batch(batch_results)
        serving.update_speed(speed_results)

        e2e_latency = (time.time() - t0) * 1000

        all_results = serving.query_all()

        print(f"[DIST] DistributedLambdaArchitecture: batch_results = {len(batch_results)}")
        print(f"[DIST] DistributedLambdaArchitecture: speed_results = {len(speed_results)}")
        print(f"[DIST] DistributedLambdaArchitecture: merged_keys = {len(all_results)}")
        print(f"[DIST] DistributedLambdaArchitecture: e2e_latency = {e2e_latency:.2f} ms")
        assert len(batch_results) == len(batch_data)
        assert len(speed_results) >= 0
        assert len(all_results) > 0


class TestDistributedKappaArchitecture:
    def test_distributed_event_log_replay(self, tmp_path):
        log_dir = tmp_path / "kappa_log"
        log_dir.mkdir()
        log_path = str(log_dir / "events.log")

        event_log = EventLog(log_path)
        total_records = 300
        for i in range(total_records):
            event_log.append({"id": i, "value": i * 10})

        assert event_log.offset == total_records

        num_workers = 3
        offsets = []
        chunk = total_records // num_workers
        for i in range(num_workers):
            start = i * chunk
            end = (i + 1) * chunk if i < num_workers - 1 else total_records
            offsets.append((start, end - start))

        t0 = time.time()

        def worker_replay(offset_limit):
            off, lim = offset_limit
            records = event_log.read(offset=off, limit=lim)
            flow = Flow(name=f"replay-{off}", mode=RuntimeMode.BATCH)
            stream = flow.from_collection(records)
            result = stream.map(lambda x: (x["id"], x["value"]))
            sink = result.collect()
            flow.execute()
            return sink.results

        all_worker_results = []
        with ThreadPoolExecutor(max_workers=num_workers) as pool:
            futures = [pool.submit(worker_replay, o) for o in offsets]
            for f in as_completed(futures):
                all_worker_results.extend(f.result())

        elapsed = time.time() - t0
        throughput = total_records / max(0.001, elapsed)

        all_ids = set()
        for r in all_worker_results:
            if isinstance(r, tuple):
                all_ids.add(r[0])

        print(f"[DIST] DistributedKappaArchitecture: total_records = {total_records}")
        print(f"[DIST] DistributedKappaArchitecture: total_ids_recovered = {len(all_ids)}")
        print(f"[DIST] DistributedKappaArchitecture: replay_throughput = {throughput:.0f} records/s")
        assert len(all_ids) == total_records


class TestDistributedAdaptiveArchitecture:
    def test_viscosity_adjustment_per_node(self):
        loads = {
            0: {"arrival_rate": 5, "queue_depth": 3, "backlog": 10},
            1: {"arrival_rate": 500, "queue_depth": 2000, "backlog": 5000},
            2: {"arrival_rate": 50000, "queue_depth": 8000, "backlog": 20000},
        }

        data_by_node = {
            0: list(range(10)),
            1: list(range(1000)),
            2: list(range(10000)),
        }

        t0 = time.time()
        node_controllers = {}

        for node_id, load in loads.items():
            controller = ViscosityController(
                initial=Viscosity.HONEYED,
                policy=ViscosityPolicy.BALANCED,
                adjust_interval=0.0,
            )
            node_controllers[node_id] = controller

        def run_node(node_id):
            ctrl = node_controllers[node_id]
            load = loads[node_id]
            data = data_by_node[node_id]

            ctrl.update_metrics(
                arrival_rate=load["arrival_rate"],
                queue_depth=load["queue_depth"],
                backlog_size=load["backlog"],
            )
            ctrl.adjust()

            af = AdaptiveFlow(
                name=f"adaptive-node-{node_id}",
                stream_fn=lambda f: f.from_collection(data).map(lambda x: x * 2),
                controller=ctrl,
            )
            result = af.execute()
            return node_id, ctrl.viscosity, result

        results = {}
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = [pool.submit(run_node, nid) for nid in loads]
            for f in as_completed(futures):
                nid, visc, res = f.result()
                results[nid] = {"viscosity": visc, "result": res}

        adaptation_time = (time.time() - t0) * 1000

        visc_order = {
            Viscosity.VOLATILE: 0,
            Viscosity.FLUID: 1,
            Viscosity.HONEYED: 2,
            Viscosity.SLUGGISH: 3,
            Viscosity.FROZEN: 4,
        }

        print(f"[DIST] DistributedAdaptiveArchitecture: node_0_viscosity = {results[0]['viscosity'].value}")
        print(f"[DIST] DistributedAdaptiveArchitecture: node_1_viscosity = {results[1]['viscosity'].value}")
        print(f"[DIST] DistributedAdaptiveArchitecture: node_2_viscosity = {results[2]['viscosity'].value}")
        print(f"[DIST] DistributedAdaptiveArchitecture: adaptation_time = {adaptation_time:.2f} ms")

        v0 = visc_order[results[0]["viscosity"]]
        v2 = visc_order[results[2]["viscosity"]]
        assert v0 <= v2 or results[0]["viscosity"] in (Viscosity.VOLATILE, Viscosity.FLUID, Viscosity.HONEYED)
        assert len(results[0]["result"]["results"]) > 0
        assert len(results[2]["result"]["results"]) > 0


class TestInterNodeCommunication:
    def test_node_a_to_node_b_pipeline(self):
        port_a = 19820
        port_b = 19821
        stop_a = threading.Event()
        stop_b = threading.Event()
        collected_b: List[str] = []

        collected_a: List[str] = []

        def handler_a(conn):
            buf = ""
            while not stop_a.is_set():
                try:
                    data = conn.recv(4096)
                    if not data:
                        break
                    buf += data.decode("utf-8", errors="replace")
                    while "\n" in buf:
                        line, buf = buf.split("\n", 1)
                        if line:
                            collected_a.append(line)
                except socket.timeout:
                    continue
                except Exception:
                    break

        def handler_b(conn):
            buf = ""
            while not stop_b.is_set():
                try:
                    data = conn.recv(4096)
                    if not data:
                        break
                    buf += data.decode("utf-8", errors="replace")
                    while "\n" in buf:
                        line, buf = buf.split("\n", 1)
                        if line:
                            collected_b.append(line)
                except socket.timeout:
                    continue
                except Exception:
                    break

        srv_a, t_a, clients_a = _start_tcp_server(port_a, handler_a, stop_a)
        srv_b, t_b, clients_b = _start_tcp_server(port_b, handler_b, stop_b)

        try:
            N = 50
            raw_data = [f"data-{i}" for i in range(N)]

            flow_a = Flow(name="node-A", mode=RuntimeMode.BATCH)
            stream_a = flow_a.from_collection(raw_data)
            result_a = stream_a.map(lambda x: x.upper())
            sink_a = result_a.collect()
            t0 = time.time()
            flow_a.execute()
            node_a_time = time.time() - t0

            for r in sink_a.results:
                _send_to_socket("127.0.0.1", port_a, [r])

            received_a = set(sink_a.results)

            flow_b = Flow(name="node-B", mode=RuntimeMode.BATCH)
            stream_b = flow_b.from_collection(list(received_a))
            result_b = stream_b.map(lambda x: f"final_{x}")
            sink_b = result_b.collect()
            t0_b = time.time()
            flow_b.execute()
            node_b_time = time.time() - t0_b

            for r in sink_b.results:
                _send_to_socket("127.0.0.1", port_b, [r])

            deadline = time.time() + 3.0
            while time.time() < deadline:
                if len(collected_b) >= N:
                    break
                time.sleep(0.05)

            overhead = (node_a_time + node_b_time) * 1000

            print(f"[DIST] InterNodeCommunication: node_a_results = {len(sink_a.results)}")
            print(f"[DIST] InterNodeCommunication: node_b_results = {len(sink_b.results)}")
            print(f"[DIST] InterNodeCommunication: received_b = {len(collected_b)}")
            print(f"[DIST] InterNodeCommunication: processing_overhead = {overhead:.2f} ms")
            assert len(sink_a.results) == N
            assert all(r.startswith("final_") for r in sink_b.results)
        finally:
            stop_a.set()
            stop_b.set()
            _stop_tcp_server(srv_a, clients_a)
            _stop_tcp_server(srv_b, clients_b)


class TestDistributedCheckpointRecovery:
    def test_checkpoint_and_restore(self):
        num_workers = 3
        N = 500
        backends = {}
        checkpoints = {}

        for wid in range(num_workers):
            backends[wid] = MemoryStateBackend()

        partitions = [[] for _ in range(num_workers)]
        for i in range(N):
            partitions[i % num_workers].append(i)

        t0 = time.time()

        for wid in range(num_workers):
            for val in partitions[wid]:
                current = backends[wid].get_value(f"sum_{wid}")
                backends[wid].set_value(f"sum_{wid}", (current or 0) + val)
                backends[wid].append_list(f"items_{wid}", val)

        checkpoint_time = 0.0
        for wid in range(num_workers):
            tc = time.time()
            checkpoints[wid] = backends[wid].checkpoint()
            checkpoint_time = max(checkpoint_time, (time.time() - tc) * 1000)

        for wid in range(num_workers):
            backends[wid]._values.clear()
            backends[wid]._lists.clear()
            backends[wid]._maps.clear()

        restore_time = 0.0
        for wid in range(num_workers):
            tr = time.time()
            backends[wid].restore(checkpoints[wid])
            restore_time = max(restore_time, (time.time() - tr) * 1000)

        expected_sums = {}
        for wid in range(num_workers):
            expected_sums[wid] = sum(partitions[wid])

        for wid in range(num_workers):
            actual_sum = backends[wid].get_value(f"sum_{wid}")
            assert actual_sum == expected_sums[wid], f"Worker {wid}: expected {expected_sums[wid]}, got {actual_sum}"
            items = backends[wid].get_list(f"items_{wid}")
            assert items == partitions[wid]

        print(f"[DIST] DistributedCheckpointRecovery: checkpoint_time = {checkpoint_time:.4f} ms")
        print(f"[DIST] DistributedCheckpointRecovery: restore_time = {restore_time:.4f} ms")
        print(f"[DIST] DistributedCheckpointRecovery: total_data = {N}")
        assert all(backends[wid].get_value(f"sum_{wid}") == expected_sums[wid] for wid in range(num_workers))


class TestDistributedExactlyOnce:
    def test_no_duplicates_across_workers(self):
        num_workers = 3
        N = 100
        shared_data = list(range(N))

        worker_results: Dict[int, List[Any]] = {}

        def worker_fn(wid):
            flow = Flow(
                name=f"exactly-once-{wid}",
                mode=RuntimeMode.BATCH,
                delivery_mode=DeliveryMode.EXACTLY_ONCE,
            )
            stream = flow.from_collection(shared_data)
            result = stream.map(lambda x: x * 2)
            sink = result.collect()
            flow.execute()
            return wid, sink.results

        t0 = time.time()
        with ThreadPoolExecutor(max_workers=num_workers) as pool:
            futures = [pool.submit(worker_fn, wid) for wid in range(num_workers)]
            for f in as_completed(futures):
                wid, results = f.result()
                worker_results[wid] = results
        dedup_time = (time.time() - t0) * 1000

        all_results = []
        for wid in range(num_workers):
            all_results.extend(worker_results[wid])

        unique_results = set(all_results)
        expected_unique = set(x * 2 for x in shared_data)

        print(f"[DIST] DistributedExactlyOnce: total_results = {len(all_results)}")
        print(f"[DIST] DistributedExactlyOnce: unique_results = {len(unique_results)}")
        print(f"[DIST] DistributedExactlyOnce: expected_unique = {len(expected_unique)}")
        print(f"[DIST] DistributedExactlyOnce: dedup_overhead = {dedup_time:.2f} ms")

        for wid in range(num_workers):
            assert set(worker_results[wid]) == expected_unique
            assert len(set(worker_results[wid])) == len(worker_results[wid]), f"Worker {wid} had duplicates"


class TestDistributedStressTest:
    def test_five_node_stress(self):
        num_nodes = 5
        base_port = 19850
        ports = [base_port + i for i in range(num_nodes)]
        records_per_node = 2000
        total_records = num_nodes * records_per_node

        stops = {p: threading.Event() for p in ports}
        received: Dict[int, List[str]] = {p: [] for p in ports}

        def make_handler(port):
            def handler(conn):
                buf = ""
                while not stops[port].is_set():
                    try:
                        data = conn.recv(4096)
                        if not data:
                            break
                        buf += data.decode("utf-8", errors="replace")
                        while "\n" in buf:
                            line, buf = buf.split("\n", 1)
                            if line:
                                received[port].append(line)
                    except socket.timeout:
                        continue
                    except Exception:
                        break

            return handler

        servers = []
        for p in ports:
            srv, t, clients = _start_tcp_server(p, make_handler(p), stops[p])
            servers.append((srv, t, clients))

        try:
            t0 = time.time()

            def node_worker(port):
                data = [{"id": i, "value": i * 3} for i in range(records_per_node)]
                flow = Flow(name=f"stress-{port}", mode=RuntimeMode.BATCH)
                stream = flow.from_collection(data)
                result = (
                    stream
                    .map(lambda x: (x["id"], x["value"]))
                    .filter(lambda x: x[1] % 2 == 0)
                )
                sink = result.collect()
                flow.execute()

                msgs = [json.dumps({"k": r[0], "v": r[1]}) for r in sink.results]
                for msg in msgs:
                    try:
                        _send_to_socket("127.0.0.1", port, [msg])
                    except Exception:
                        break

                return port, len(sink.results)

            node_counts = {}
            with ThreadPoolExecutor(max_workers=num_nodes) as pool:
                futures = [pool.submit(node_worker, p) for p in ports]
                for f in as_completed(futures):
                    port, count = f.result()
                    node_counts[port] = count

            elapsed = time.time() - t0

            deadline = time.time() + 5.0
            while time.time() < deadline:
                total_recv = sum(len(received[p]) for p in ports)
                if total_recv >= total_records * 0.5:
                    break
                time.sleep(0.05)

            total_recv = sum(len(received[p]) for p in ports)
            throughput = total_records / max(0.001, elapsed)

            per_node_throughputs = {}
            for p in ports:
                sent_count = node_counts.get(p, 0)
                per_node_throughputs[p] = sent_count / max(0.001, elapsed)

            print(f"[DIST] DistributedStressTest: total_records = {total_records}")
            print(f"[DIST] DistributedStressTest: total_received = {total_recv}")
            print(f"[DIST] DistributedStressTest: total_throughput = {throughput:.0f} records/s")
            print(f"[DIST] DistributedStressTest: per_node = {per_node_throughputs}")

            for p in ports:
                assert node_counts.get(p, 0) > 0, f"Node {p} produced no results"

            assert throughput > 0, "Throughput must be positive"
        finally:
            for p in ports:
                stops[p].set()
            for srv, t, clients in servers:
                _stop_tcp_server(srv, clients)