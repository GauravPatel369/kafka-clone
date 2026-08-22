"""
Shared pytest fixtures.

CONCEPT: We test against a REAL broker, not a mock.
------------------------------------------------------
Every test in this suite starts an actual `Broker`, binds an actual TCP
socket, and writes actual files to disk.

That is a deliberate choice. A mock broker would only prove that our code
matches our *assumptions* about the broker — and in a networked system, the
assumptions are precisely what tend to be wrong. The bugs that matter here are
things like "recv() returned fewer bytes than expected" and "the file handle
was still buffered", and a mock cannot produce either of them.

The cost is speed: each test pays a few milliseconds for a socket and a thread
instead of microseconds. For a suite this size that is a trade worth making
every time.

TWO TRICKS MAKE THIS PRACTICAL:

  1. **port=0** — asks the OS for any free port. Tests never collide with a
     dev broker already running on 9092, and the suite can run in parallel
     with itself. Hardcoding a port in a test suite guarantees a flaky CI run
     eventually.

  2. **tmp_path** — pytest's per-test temporary directory. Every test gets a
     completely fresh data directory, so tests cannot leak state into each
     other. A test that passes alone and fails in the suite is almost always
     shared state.
"""

import asyncio
import threading

import pytest

from broker.broker import Broker
from common.net import SyncConnection


@pytest.fixture
def broker(tmp_path):
    """
    A running broker on an ephemeral port with a fresh data directory.

    Yields the Broker instance; `broker.port` holds the real port the OS
    assigned.

    WHY THE THREAD: the broker is an asyncio server, and `asyncio.run()`
    blocks its thread until the loop stops. The test needs to run *while* the
    broker serves, so the broker gets a background thread with its own event
    loop and the test keeps the main thread.

    The thread is a daemon so a hung broker can never wedge the whole test
    run — worst case the process exits and takes it along.
    """
    instance = Broker(host='127.0.0.1', port=0, data_dir=str(tmp_path / 'data'))

    thread = threading.Thread(
        target=lambda: asyncio.run(instance.start()),
        name='test-broker',
        daemon=True,
    )
    thread.start()

    # Wait for the "bound and accepting" signal rather than sleeping a
    # hopeful 0.1 seconds. Sleep-and-hope is the number one source of flaky
    # integration tests: it is too slow on fast machines and too short on
    # loaded CI runners.
    assert instance.ready.wait(timeout=5), "Broker failed to start within 5s"

    yield instance

    instance.stop()
    thread.join(timeout=5)


@pytest.fixture
def conn(broker):
    """A connected SyncConnection to the test broker."""
    with SyncConnection('127.0.0.1', broker.port, timeout=5.0) as connection:
        yield connection


@pytest.fixture
def offsets_dir(tmp_path):
    """A fresh directory for consumer offset files."""
    path = tmp_path / 'offsets'
    path.mkdir()
    return str(path)
