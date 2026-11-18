#!/usr/bin/env bash
set -e
cd ~/ouroboros_project
python3 -m py_compile ghost_bridge.py ghost_udp_listener.py ouroboros_core/*.py
echo "=== 1. COMPILE PASSED ==="
python3 ghost_bridge.py --cycles 5 --nodes 4 --fault-cycle 3
echo "=== 2. STARTING UDP TEST ==="
python3 ghost_udp_listener.py --udp-port 14550 --tick 1.0 --max-ticks 3 &
sleep 0.5
python3 -c 'import socket, json; s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); pkt = {"unit_id":"GHOST-V60-01","seq":1,"gait":"TROT_PATROL","battery_pct":88.5,"bus_voltage":49.1,"pitch_deg":30.2,"roll_deg":4.1,"actuator_temps_c":{"FL_HIP_KNEE":52.0,"RL_HIP_KNEE":79.5},"lidar_min_clearance_m":0.40}; s.sendto(json.dumps(pkt).encode(), ("127.0.0.1", 14550))'
wait
echo "=== 3. ALL TESTS COMPLETE ==="
