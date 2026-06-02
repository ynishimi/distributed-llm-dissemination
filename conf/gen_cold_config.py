#!/usr/bin/env python3
"""
Generate cold start config.json with given private IPs.

Usage:
    python3 gen_cold_config.py <priv_ip0> <priv_ip1> <priv_ip2> <priv_ip3>

Changes from warm config:
  - Addr: ":8080" -> "<private_ip>:8080" for each node
  - InitialLayers source type: "1" (SSD, 200MB/s) -> "0" (HuggingFace, 15.5MB/s)
"""

import json
import sys
from pathlib import Path


def main():
    if len(sys.argv) < 5:
        print(
            "Usage: python3 gen_cold_config.py <priv_ip0> <priv_ip1> <priv_ip2> <priv_ip3>")
        sys.exit(1)

    private_ips = sys.argv[1:5]

    config_path = Path(__file__).parent / "config.json"
    with open(config_path) as f:
        conf = json.load(f)

    for node in conf["Nodes"]:
        node_id = node["Id"]
        node["Addr"] = f"{private_ips[node_id]}:8080"

    with open(config_path, "w") as f:
        json.dump(conf, f, indent=4)

    print(f"Updated config.json with private IPs: {private_ips}")


if __name__ == "__main__":
    main()
