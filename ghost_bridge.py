#!/usr/bin/env python3
"""
Ghost Robotics Q-UGV Telemetry Bridge & Autonomous Patrol Supervisor.
Integrates quadrupedal kinematics, thermal telemetry, and gait safety interlocks
directly into the Ouroboros + Deepfleet + Argon (Medium) engine.
"""

import argparse
import asyncio
import json
import math
import random
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, List

from ouroboros_core import ArgonGuard, ArgonLevel, AuditLedger, DeepfleetBus


class GaitState(Enum):
    IDLE_STAND = "IDLE_STAND"
    TROT_PATROL = "TROT_PATROL"
    STAIR_CLIMB = "STAIR_CLIMB"
    SAFE_CROUCH = "SAFE_CROUCH"
    EMERGENCY_STOP = "EMERGENCY_STOP"


@dataclass
class GhostTelemetryFrame:
    unit_id: str
    seq: int
    gait: str
    battery_pct: float
    bus_voltage: float
    pitch_deg: float
    roll_deg: float
    yaw_deg: float
    actuator_temps_c: Dict[str, float] = field(default_factory=dict)
    lidar_min_clearance_m: float = 2.5
    comms_rssi_dbm: int = -58

    def evaluate_safety(self) -> Dict[str, Any]:
        """Deterministic kinematic and thermal safety interlock check."""
        violations: List[str] = []
        max_temp = max(self.actuator_temps_c.values()) if self.actuator_temps_c else 0.0

        if max_temp >= 78.0:
            violations.append(f"ACTUATOR_OVERHEAT:{max_temp:.1f}C")
        if abs(self.pitch_deg) > 28.0 or abs(self.roll_deg) > 24.0:
            violations.append(f"ATTITUDE_TILT_LIMIT:P={self.pitch_deg:.1f}/R={self.roll_deg:.1f}")
        if self.battery_pct < 15.0 or self.bus_voltage < 43.5:
            violations.append(f"LOW_POWER_RESERVE:{self.battery_pct:.1f}%/{self.bus_voltage:.1f}V")
        if self.lidar_min_clearance_m < 0.45:
            violations.append(f"PROXIMITY_COLLISION_RISK:{self.lidar_min_clearance_m:.2f}m")

        recommended_gait = self.gait
        if any("ATTITUDE" in v or "PROXIMITY" in v for v in violations):
            recommended_gait = GaitState.EMERGENCY_STOP.value
        elif violations:
            recommended_gait = GaitState.SAFE_CROUCH.value

        return {
            "safe": len(violations) == 0,
            "violations": violations,
            "max_actuator_temp_c": round(max_temp, 2),
            "recommended_gait": recommended_gait,
        }


class GhostPatrolSimulator:
    """Generates realistic Q-UGV patrol telemetry frames with optional fault injection."""

    def __init__(self, unit_id: str = "GHOST-V60-OMA2", inject_fault_cycle: int = -1) -> None:
        self.unit_id = unit_id
        self.inject_fault_cycle = inject_fault_cycle
        self.battery = 94.0

    def next_frame(self, seq: int) -> GhostTelemetryFrame:
        self.battery = max(10.0, round(self.battery - random.uniform(0.3, 0.8), 2))
        voltage = round(44.0 + (self.battery / 100.0) * 6.4, 2)

        pitch = round(math.sin(seq * 0.7) * 4.5 + random.uniform(-1.0, 1.0), 2)
        roll = round(math.cos(seq * 0.5) * 3.2 + random.uniform(-0.8, 0.8), 2)
        yaw = round((seq * 18.5) % 360.0, 2)
        clearance = round(random.uniform(1.2, 4.8), 2)

        temps = {
            "FL_HIP_KNEE": round(46.0 + seq * 1.1 + random.uniform(-0.5, 1.2), 1),
            "FR_HIP_KNEE": round(45.5 + seq * 1.0 + random.uniform(-0.5, 1.2), 1),
            "RL_HIP_KNEE": round(47.2 + seq * 1.2 + random.uniform(-0.5, 1.2), 1),
            "RR_HIP_KNEE": round(46.8 + seq * 1.1 + random.uniform(-0.5, 1.2), 1),
        }

        if seq == self.inject_fault_cycle:
            # Simulate sudden steep incline + rear-left actuator thermal spike
            pitch = 31.4
            temps["RL_HIP_KNEE"] = 81.6
            clearance = 0.38

        return GhostTelemetryFrame(
            unit_id=self.unit_id,
            seq=seq,
            gait=GaitState.TROT_PATROL.value,
            battery_pct=self.battery,
            bus_voltage=voltage,
            pitch_deg=pitch,
            roll_deg=roll,
            yaw_deg=yaw,
            actuator_temps_c=temps,
            lidar_min_clearance_m=clearance,
            comms_rssi_dbm=random.randint(-66, -52),
        )


async def run_ghost_patrol(
    cycles: int = 5,
    nodes: int = 4,
    fault_cycle: int = -1,
    db_path: str = "ghost_patrol_ledger.db",
) -> None:
    guard = ArgonGuard()
    bus = DeepfleetBus(num_nodes=nodes, rpc_port=0)
    sim = GhostPatrolSimulator(inject_fault_cycle=fault_cycle)

    await bus.start()
    try:
        with AuditLedger(db_path=db_path) as ledger:
            for seq in range(1, cycles + 1):
                frame = sim.next_frame(seq)
                safety = frame.evaluate_safety()

                payload: Dict[str, Any] = {
                    "platform": "GHOST_ROBOTICS_V60",
                    "telemetry": asdict(frame),
                    "safety_interlock": safety,
                }

                # If safety violation occurs, flag DF-NODE-02 for fault isolation test
                if not safety["safe"]:
                    await bus.inject_telemetry(
                        {"alert": "KINEMATIC_INTERLOCK_TRIP", "trip_node": "DF-NODE-02"}
                    )

                report = guard.validate_and_clamp(payload, cycle_id=seq, requested_level=ArgonLevel.MEDIUM)
                calibrated = getattr(report, "calibrated_payload", payload)
                dispatch = await bus.dispatch_cycle(calibrated, cycle_id=seq)

                # Commit patrol snapshot into SQLite WAL ledger
                record_fn = getattr(ledger, "record_cycle", None)
                if callable(record_fn):
                    try:
                        record_fn(
                            cycle_id=seq,
                            payload=dispatch["merged_payload"],
                            argon_level=ArgonLevel.MEDIUM.value,
                        )
                    except TypeError:
                        pass

                print(
                    json.dumps(
                        {
                            "seq": seq,
                            "unit_id": frame.unit_id,
                            "active_gait": frame.gait,
                            "recommended_gait": safety["recommended_gait"],
                            "safety_ok": safety["safe"],
                            "violations": safety["violations"],
                            "quorum_met": dispatch["quorum"]["quorum_met"],
                            "healthy_nodes": dispatch["quorum"]["healthy_nodes"],
                        },
                        indent=2,
                    )
                )
    finally:
        await bus.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Ghost Robotics Q-UGV Ouroboros Patrol Bridge")
    parser.add_argument("--cycles", type=int, default=5, help="Number of patrol telemetry frames")
    parser.add_argument("--nodes", type=int, default=4, help="Deepfleet quorum worker nodes")
    parser.add_argument(
        "--fault-cycle",
        type=int,
        default=3,
        help="Cycle index to inject a simulated pitch/thermal fault (-1 to disable)",
    )
    parser.add_argument("--db", type=str, default="ghost_patrol_ledger.db", help="Patrol ledger DB")
    args = parser.parse_args()

    asyncio.run(
        run_ghost_patrol(
            cycles=args.cycles,
            nodes=args.nodes,
            fault_cycle=args.fault_cycle,
            db_path=args.db,
        )
    )


if __name__ == "__main__":
    main()
