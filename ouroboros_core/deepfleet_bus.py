"""
Deepfleet Distributed Transport Bus & Circuit-Protected Worker Grid.
"""

import asyncio
import hashlib
import json
import os
import time
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


class CircuitState(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    """Per-node fault isolation with exponential backoff recovery."""

    def __init__(self, failure_threshold: int = 3, recovery_timeout: float = 5.0) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failures = 0
        self.state = CircuitState.CLOSED
        self.last_failure_time = 0.0

    def allow_request(self) -> bool:
        if self.state == CircuitState.CLOSED:
            return True
        if self.state == CircuitState.OPEN:
            if (time.monotonic() - self.last_failure_time) >= self.recovery_timeout:
                self.state = CircuitState.HALF_OPEN
                return True
            return False
        return True

    def record_success(self) -> None:
        self.failures = 0
        self.state = CircuitState.CLOSED

    def record_failure(self) -> None:
        self.failures += 1
        self.last_failure_time = time.monotonic()
        if self.failures >= self.failure_threshold:
            self.state = CircuitState.OPEN

    def to_dict(self) -> Dict[str, Any]:
        return {
            "state": self.state.value,
            "failures": self.failures,
            "threshold": self.failure_threshold,
            "recovery_timeout": self.recovery_timeout,
        }


class FleetNode:
    """Individual Deepfleet worker node with latency tracking and breaker protection."""

    def __init__(self, node_id: str) -> None:
        self.node_id = node_id
        self.breaker = CircuitBreaker()
        self.processed_cycles = 0
        self.total_latency_ms = 0.0
        self.simulated_fault_next = False

    async def execute_task(self, payload: Dict[str, Any], cycle_id: int) -> Dict[str, Any]:
        if not self.breaker.allow_request():
            raise RuntimeError(f"{self.node_id} circuit breaker is OPEN")

        start_t = time.perf_counter()
        try:
            if self.simulated_fault_next:
                self.simulated_fault_next = False
                raise RuntimeError(f"Simulated fault triggered on {self.node_id}")

            await asyncio.sleep(0.02)
            raw = json.dumps({"node": self.node_id, "cycle": cycle_id, "payload": payload}, sort_keys=True)
            node_signature = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

            elapsed_ms = (time.perf_counter() - start_t) * 1000.0
            self.processed_cycles += 1
            self.total_latency_ms += elapsed_ms
            self.breaker.record_success()

            return {
                "node_id": self.node_id,
                "cycle_id": cycle_id,
                "signature": node_signature,
                "latency_ms": round(elapsed_ms, 3),
                "breaker_state": self.breaker.state.value,
            }
        except Exception:
            self.breaker.record_failure()
            raise

    def health_snapshot(self) -> Dict[str, Any]:
        avg_lat = (
            self.total_latency_ms / self.processed_cycles if self.processed_cycles > 0 else 0.0
        )
        return {
            "node_id": self.node_id,
            "processed_cycles": self.processed_cycles,
            "avg_latency_ms": round(avg_lat, 3),
            "circuit_breaker": self.breaker.to_dict(),
        }


class DeepfleetBus:
    """Manages the distributed fleet worker pool, quorum, and JSON-RPC 2.0 server."""

    def __init__(
        self,
        num_nodes: int = 4,
        rpc_host: str = "127.0.0.1",
        rpc_port: int = 0,
        unix_socket_path: Optional[str] = None,
    ) -> None:
        if num_nodes < 1:
            raise ValueError("DeepfleetBus requires at least 1 fleet node.")
        self.nodes: List[FleetNode] = [
            FleetNode(f"DF-NODE-{i:02d}") for i in range(1, num_nodes + 1)
        ]
        self.rpc_host = rpc_host
        self.rpc_port = rpc_port
        self.bound_port: Optional[int] = None
        self.unix_socket_path = unix_socket_path
        self._telemetry_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        self._tcp_server: Optional[asyncio.AbstractServer] = None
        self._unix_server: Optional[asyncio.AbstractServer] = None
        self._rr_index: int = 0

    async def start(self) -> None:
        self._tcp_server = await asyncio.start_server(
            self._handle_rpc_client, host=self.rpc_host, port=self.rpc_port
        )
        if self._tcp_server.sockets:
            sockname = self._tcp_server.sockets[0].getsockname()
            if isinstance(sockname, tuple):
                self.bound_port = int(sockname[1])
        if self.unix_socket_path and hasattr(asyncio, "start_unix_server"):
            try:
                if os.path.exists(self.unix_socket_path):
                    os.unlink(self.unix_socket_path)
                self._unix_server = await asyncio.start_unix_server(
                    self._handle_rpc_client, path=self.unix_socket_path
                )
            except OSError:
                self._unix_server = None

    async def stop(self) -> None:
        if self._tcp_server is not None:
            self._tcp_server.close()
            await self._tcp_server.wait_closed()
            self._tcp_server = None
        if self._unix_server is not None:
            self._unix_server.close()
            await self._unix_server.wait_closed()
            self._unix_server = None
        if self.unix_socket_path and os.path.exists(self.unix_socket_path):
            try:
                os.unlink(self.unix_socket_path)
            except OSError:
                pass

    async def inject_telemetry(self, packet: Dict[str, Any]) -> int:
        enriched = dict(packet)
        enriched.setdefault("_injected_at", round(time.time(), 4))
        await self._telemetry_queue.put(enriched)
        return self._telemetry_queue.qsize()

    def drain_telemetry(self) -> List[Dict[str, Any]]:
        drained: List[Dict[str, Any]] = []
        while not self._telemetry_queue.empty():
            try:
                drained.append(self._telemetry_queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        return drained

    def quorum_telemetry(self) -> Dict[str, Any]:
        total = len(self.nodes)
        healthy = sum(1 for n in self.nodes if n.breaker.state != CircuitState.OPEN)
        quorum_required = (total // 2) + 1
        return {
            "total_nodes": total,
            "healthy_nodes": healthy,
            "quorum_required": quorum_required,
            "quorum_met": healthy >= quorum_required,
            "health_ratio": round(healthy / total, 4),
            "bound_rpc_port": self.bound_port,
            "unix_socket": self.unix_socket_path,
            "queued_telemetry": self._telemetry_queue.qsize(),
            "nodes": [n.health_snapshot() for n in self.nodes],
        }

    async def dispatch_cycle(
        self, payload: Dict[str, Any], cycle_id: int, direct_bypass: bool = False
    ) -> Dict[str, Any]:
        injected_packets = self.drain_telemetry()
        working_payload = dict(payload)
        if injected_packets:
            existing = list(working_payload.get("external_telemetry", []))
            existing.extend(injected_packets)
            working_payload["external_telemetry"] = existing

        for pkt in injected_packets:
            fault_node_id = pkt.get("trip_node")
            if fault_node_id:
                for node in self.nodes:
                    if node.node_id == fault_node_id:
                        node.simulated_fault_next = True

        node_results: List[Dict[str, Any]] = []
        node_errors: List[Dict[str, str]] = []

        if direct_bypass:
            selected_node: Optional[FleetNode] = None
            for offset in range(len(self.nodes)):
                idx = (self._rr_index + offset) % len(self.nodes)
                if self.nodes[idx].breaker.allow_request():
                    selected_node = self.nodes[idx]
                    self._rr_index = (idx + 1) % len(self.nodes)
                    break
            if selected_node is None:
                selected_node = self.nodes[0]
            try:
                node_results.append(await selected_node.execute_task(working_payload, cycle_id))
            except Exception as exc:
                node_errors.append({"node_id": selected_node.node_id, "error": str(exc)})
        else:
            async def _run_node(n: FleetNode) -> Tuple[str, Optional[Dict[str, Any]], Optional[str]]:
                try:
                    return n.node_id, await n.execute_task(working_payload, cycle_id), None
                except Exception as exc:
                    return n.node_id, None, str(exc)

            outcomes = await asyncio.gather(*(_run_node(n) for n in self.nodes))
            for nid, res, err in outcomes:
                if res is not None:
                    node_results.append(res)
                if err is not None:
                    node_errors.append({"node_id": nid, "error": err})

        return {
            "cycle_id": cycle_id,
            "routing_mode": "DIRECT_BYPASS" if direct_bypass else "QUORUM_FANOUT",
            "injected_packets_merged": len(injected_packets),
            "node_results": node_results,
            "node_errors": node_errors,
            "quorum": self.quorum_telemetry(),
            "merged_payload": working_payload,
        }

    async def _handle_rpc_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while not reader.at_eof():
                line = await reader.readline()
                if not line:
                    break
                raw_req = line.decode("utf-8", errors="replace").strip()
                if raw_req:
                    resp = await self._process_rpc_message(raw_req)
                    writer.write((json.dumps(resp) + "\n").encode("utf-8"))
                    await writer.drain()
        except Exception:
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _process_rpc_message(self, raw_req: str) -> Dict[str, Any]:
        req_id: Any = None
        try:
            req = json.loads(raw_req)
            if not isinstance(req, dict):
                raise ValueError("JSON-RPC payload must be an object.")
            req_id = req.get("id")
            method = req.get("method", "")
            params = req.get("params", {})

            if method == "inject_telemetry":
                if not isinstance(params, dict):
                    raise ValueError("Params must be a JSON object.")
                qsize = await self.inject_telemetry(params)
                return {"jsonrpc": "2.0", "id": req_id, "result": {"status": "queued", "queue_depth": qsize}}

            if method == "quorum_status":
                return {"jsonrpc": "2.0", "id": req_id, "result": self.quorum_telemetry()}

            if method == "trip_node":
                target = params.get("node_id", "") if isinstance(params, dict) else ""
                tripped = False
                for node in self.nodes:
                    if node.node_id == target:
                        node.simulated_fault_next = True
                        tripped = True
                        break
                return {"jsonrpc": "2.0", "id": req_id, "result": {"node_id": target, "tripped": tripped}}

            return {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": f"Method not found: {method}"}}
        except Exception as exc:
            return {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32000, "message": str(exc)}}
