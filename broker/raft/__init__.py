"""
Raft: quorum consensus for cluster metadata.

    state.py    currentTerm and votedFor, fsynced — the safety proof rests here
    log.py      the replicated log; the election restriction lives in it
    machine.py  what committed entries MEAN: brokers, topics, placement
    node.py     follower / candidate / leader, and the two RPCs

Read them in that order. `state.py` and `log.py` are what make the algorithm
safe; `node.py` is what makes it run.
"""

from broker.raft.log import LogEntry, RaftLog
from broker.raft.machine import MetadataStateMachine, registration_command
from broker.raft.node import CANDIDATE, FOLLOWER, LEADER, RaftNode
from broker.raft.state import PersistentState

__all__ = ['RaftNode', 'RaftLog', 'LogEntry', 'PersistentState',
           'MetadataStateMachine', 'registration_command',
           'FOLLOWER', 'CANDIDATE', 'LEADER']
