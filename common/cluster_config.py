"""
CONCEPT: The Static Broker Roster
-----------------------------------
A cluster needs to know which brokers exist before any of them are running.
That is a bootstrap problem: you cannot discover peers from a service you have
not started yet.

We solve it the boring way — a file every broker reads at startup:

    {
      "brokers": [
        {"node_id": 1, "host": "127.0.0.1", "port": 9092},
        {"node_id": 2, "host": "127.0.0.1", "port": 9093},
        {"node_id": 3, "host": "127.0.0.1", "port": 9094}
      ]
    }

WHY STATIC AND NOT DISCOVERY
------------------------------
etcd or Consul would give dynamic membership — and *they* then need consensus,
so you have replaced one distributed-systems problem with the same problem
plus an extra process to operate. Gossip (SWIM) avoids the central list at the
cost of a protocol substantially larger than everything else in this phase.

The interesting problems here are **liveness** and **election**. A fixed roster
lets us study those without also building service discovery, which is a
different project.

The cost is real and worth naming: adding a broker means editing this file on
every machine and restarting. Real Kafka's KRaft has the same property for its
controller quorum, for the same reason — you cannot bootstrap consensus from
nothing.

WHAT node_id IS FOR
---------------------
Identity that survives a machine. Leadership, replica sets and assignments are
all expressed as broker IDs, never as host:port — a broker can be moved to new
hardware and keep its id, and every reference to it stays valid. `METADATA` has
reported `node_id` since Phase 1 precisely so this phase adds peers rather than
a new concept.
"""

import json
import os


class ClusterConfigError(Exception):
    """The roster is missing, malformed, or self-contradictory."""


class BrokerAddress:
    """One broker's identity and location."""

    __slots__ = ('node_id', 'host', 'port')

    def __init__(self, node_id: int, host: str, port: int):
        self.node_id = node_id
        self.host = host
        self.port = port

    def as_dict(self) -> dict:
        return {'node_id': self.node_id, 'host': self.host, 'port': self.port}

    def __repr__(self) -> str:
        return f"Broker({self.node_id} @ {self.host}:{self.port})"


class ClusterConfig:
    """The roster: every broker that is *supposed* to exist."""

    def __init__(self, brokers: list):
        if not brokers:
            raise ClusterConfigError("cluster must contain at least one broker")

        ids = [b.node_id for b in brokers]
        if len(set(ids)) != len(ids):
            # Two brokers with one id is catastrophic and silent: leadership,
            # heartbeats and metadata all key on node_id, so the cluster would
            # believe two machines are the same machine.
            raise ClusterConfigError(f"duplicate node_id in cluster: {sorted(ids)}")

        self.brokers = {b.node_id: b for b in brokers}

    # ── Loading ────────────────────────────────────────────────

    @classmethod
    def from_file(cls, path: str) -> 'ClusterConfig':
        if not os.path.exists(path):
            raise ClusterConfigError(f"no cluster file at {path}")
        try:
            # 'utf-8-sig', not 'utf-8': it accepts a byte-order mark and strips
            # it, and reads plain UTF-8 unchanged.
            #
            # This is not hypothetical tidiness. Windows editors and
            # PowerShell's `Set-Content -Encoding utf8` write a BOM by default,
            # and a BOM'd cluster.json fails with
            # "Unexpected UTF-8 BOM (decode using utf-8-sig)" — which is at
            # least a clear message, but it stops three brokers from starting
            # over an invisible character. A config file read by humans on
            # mixed platforms should tolerate how those platforms write files.
            with open(path, encoding='utf-8-sig') as fh:
                raw = json.load(fh)
        except (OSError, json.JSONDecodeError) as exc:
            raise ClusterConfigError(f"could not read {path}: {exc}") from exc
        return cls.from_dict(raw)

    @classmethod
    def from_dict(cls, raw: dict) -> 'ClusterConfig':
        brokers = []
        for entry in raw.get('brokers', []):
            try:
                brokers.append(BrokerAddress(
                    int(entry['node_id']), entry['host'], int(entry['port'])))
            except (KeyError, TypeError, ValueError) as exc:
                raise ClusterConfigError(
                    f"malformed broker entry {entry!r}: {exc}") from exc
        return cls(brokers)

    @classmethod
    def single_node(cls, node_id: int, host: str, port: int) -> 'ClusterConfig':
        """
        A one-broker "cluster".

        Not a special case in the code — a cluster of one behaves exactly like a
        cluster of three with two brokers down. That uniformity is deliberate:
        it means the single-broker path used by every Phase 0-4 test exercises
        the same membership and election code as a real cluster, rather than a
        separate branch that could rot.
        """
        return cls([BrokerAddress(node_id, host, port)])

    # ── Access ─────────────────────────────────────────────────

    def get(self, node_id: int):
        return self.brokers.get(node_id)

    def node_ids(self) -> list:
        return sorted(self.brokers)

    def peers(self, self_id: int) -> list:
        """Every broker except me — who I send heartbeats to."""
        return [b for node_id, b in sorted(self.brokers.items()) if node_id != self_id]

    def as_list(self) -> list:
        return [self.brokers[n].as_dict() for n in self.node_ids()]

    def __len__(self) -> int:
        return len(self.brokers)

    def __contains__(self, node_id: int) -> bool:
        return node_id in self.brokers

    def __repr__(self) -> str:
        return f"ClusterConfig({len(self.brokers)} brokers: {self.node_ids()})"
