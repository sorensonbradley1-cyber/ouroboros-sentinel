#!/usr/bin/env python3
"""
Asynchronous UDP Telemetry Listener for Ghost Robotics Q-UGV Patrol Bridge.
Receives JSON datagrams via asyncio.DatagramProtocol, validates schema & optional
HMAC-SHA256 signatures, runs deterministic safety checks, and injects into DeepfleetBus.
"""

import argparse
import asyncio
import hashlib
import hmac
import json
import time
from typing import Any, Dict, Optional, Tuple

from ghost_bridge import GhostTelemetryFrame
from ouroboros_core import ArgonGuard, ArgonLevel, AuditLedger, DeepfleetBus


class GhostUDPProtocol(asyncio.DatagramProtocol):
    """Non-blocking UDP datagram protocol handler for live Q-UGV telemetry."""

    def __init__(
        self,
        bus: DeepfleetBus,
        loop: asyncio.AbstractEventLoop,
        hmac_secret: Optional[str] = None,
    ) -> None:
        self.bus = bus
        self.loop = loop
        self.hmac_secret = hmac_secret.encode("utf-8") if hmac_secret else None
        self.packets_accepted = 0
        self.packets_rejected = 0

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        raw_text = data.decode("utf-8", errors="replace").strip()
        if not raw_text:
            return

        try:
            packet = json.loads(raw_text)
            if not isinstance(packet, dict):
                raise ValueError("Datagram root must be a JSON object.")

            # Optional HMAC-SHA256 authentication check against spoofed telemetry
            if self.hmac_secret is not None:
                provided_sig = str(packet.pop("_hmac", ""))
                canonical = json.dumps(packet, sort_keys=True, separators=(",", ":")).encode("utf-8")
                expected_sig = hmac.new(self.hmac_secret, canonical, hashlib.sha256).hexdigest()
                if not hmac.compare_digest(provided_sig, expected_sig):
                    raise PermissionError(f"HMAC signature mismatch from {addr[0]}:{addr[1]}")

            frame = GhostTelemetryFrame(
                unit_id=str(packet.get("unit_id", "GHOST-V60-EXT")),
                seq=int(packet.get("seq", self.packets_accepted + 1)),
                gait=str(packet.get("gait", "TROT_PATROL")),
                battery_pct=float(packet.get("battery_pct", 85.0)),
                bus_voltage=float(packet.get("bus_voltage", 48.0)),
                pitch_deg=float(packet.get("pitch_deg", 0.0)),
                roll_deg=float(packet.get("roll_deg", 0.0)),
                yaw_deg=float(packet.get("yaw_deg", 0.0)),
                actuator_temps_c=dict(packet.get("actuator_temps_c", {})),
                lidar_min_clearance_m=float(packet.get("lidar_min_clearance_m", 2.0)),
                comms_rssi_dbm=int(packet.get("comms_rssi_dbm", -60)),
            )

            safety = frame.evaluate_safety()
            enriched: Dict[str, Any] = {
                "source_addr": f"{addr[0]}:{addr[1]}",
                "received_at": round(time.time(), 4),
                "telemetry": packet,
                "safety_interlock": safety,
            }
            if not safety["safe"]:
                enriched["trip_node"] = "DF-NODE-02"

            self.packets_accepted += 1
            self.loop.create_task(self.bus.inject_telemetry(enriched))
            print(
                f"[UDP RX] {frame.unit_id} seq={frame.seq} gait={safety['recommended_gait']} "
                f"safe={safety['safe']} violations={safety['violations']}"
            )
        except Exception as exc:
            self.packets_rejected += 1
            print(f"[UDP DROP] {addr[0]}:{addr[1]} -> {exc}")


async def run_udp_supervisor(
    host: str = "127.0.0.1",
    udp_port: int = 14550,
    nodes: int = 4,
    tick_interval: float = 1.0,
    max_ticks: int = 0,
    hmac_secret: Optional[str] = None,
    db_path: str = "ghost_udp_ledger.db",
) -> None:
    """Starts the UDP listener and dispatches Ouroboros + Deepfleet cycles on incoming frames."""
    loop = asyncio.get_running_loop()
    guard = ArgonGuard()
    bus = DeepfleetBus(num_nodes=nodes, rpc_port=0)

    await bus.start()
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: GhostUDPProtocol(bus=bus, loop=loop, hmac_secret=hmac_secret),
        local_addr=(host, udp_port),
    )

    print(
        f"[SUPERVISOR ONLINE] Listening UDP {host}:{udp_port} | "
        f"Deepfleet nodes={nodes} | HMAC={'ENABLED' if hmac_secret else 'OFF'}"
    )

    cycle = 0
    try:
        with AuditLedger(db_path=db_path) as ledger:
            while max_ticks == 0 or cycle < max_ticks:
                await asyncio.sleep(tick_interval)
                if bus._telemetry_queue.empty():
                    continue

                cycle += 1
                base_payload = {"platform": "GHOST_ROBOTICS_V60_UDP", "supervisor_cycle": cycle}
                report = guard.validate_and_clamp(base_payload, cycle_id=cycle, requested_level=ArgonLevel.MEDIUM)
                calibrated = getattr(report, "calibrated_payload", base_payload)
                dispatch = await bus.dispatch_cycle(calibrated, cycle_id=cycle)

                record_fn = getattr(ledger, "record_cycle", None)
                if callable(record_fn):
                    try:
                        record_fn(
                            cycle_id=cycle,
                            payload=dispatch["merged_payload"],
                            argon_level=ArgonLevel.MEDIUM.value,
                        )
                    except TypeError:
                        pass

                print(
                    json.dumps(
                        {
                            "cycle": cycle,
                            "packets_merged": dispatch["injected_packets_merged"],
                            "quorum_met": dispatch["quorum"]["quorum_met"],
                            "healthy_nodes": dispatch["quorum"]["healthy_nodes"],
                            "accepted_total": protocol.packets_accepted,
                            "rejected_total": protocol.packets_rejected,
                        }
                    )
                )
    except asyncio.CancelledError:
        pass
    finally:
        transport.close()
        await bus.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Ghost Q-UGV Async UDP Telemetry Supervisor")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Bind IP address")
    parser.add_argument("--udp-port", type=int, default=14550, help="UDP telemetry port")
    parser.add_argument("--nodes", type=int, default=4, help="Deepfleet quorum nodes")
    parser.add_argument("--tick", type=float, default=1.0, help="Cycle sweep interval (seconds)")
    parser.add_argument("--max-ticks", type=int, default=0, help="Stop after N ticks (0 = continuous)")
    parser.add_argument("--hmac-secret", type=str, default=None, help="Optional HMAC-SHA256 shared key")
    args = parser.parse_args()

    try:
        asyncio.run(
            run_udp_supervisor(
                host=args.host,
                udp_port=args.udp_port,
                nodes=args.nodes,
                tick_interval=args.tick,
                max_ticks=args.max_ticks,
                hmac_secret=args.hmac_secret,
            )
        )
    except KeyboardInterrupt:
        print("\n[SUPERVISOR SHUTDOWN] Clean exit.")


if __name__ == "__main__":
    main()
