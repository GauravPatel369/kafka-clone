# Phase 1 — Protocol v2: Requests, Responses, and Correlation

> **Goal:** Upgrade the wire protocol from basic JSON messages to a structured, multiplexed protocol with API keys, versions, correlation IDs, and numeric error codes.

---

## 1. The Problem with Phase 0 Protocol

In Phase 0, the binary header was simple:
```
┌────────────┬────────────┬────────────┬──────────────────┐
│  magic 4B  │ length 4B  │  type 4B   │   body (JSON)    │
│   0xCAFE   │            │            │                  │
└────────────┴────────────┴────────────┴──────────────────┘
```

This simple format introduced key limitations:
1. **No Request/Response Pipelining:** Clients cannot send multiple requests in flight over a single TCP connection because responses cannot be matched to requests without a `correlation_id`.
2. **No API Versioning:** Modifying request fields risks breaking older clients without an `api_key` and `api_version`.
3. **String Error Messages:** Errors returned as text strings (`status: error, reason: ...`) prevent clients from programmatically handling retries (e.g., retrying transient network errors vs failing hard on topic non-existence).

---

## 2. Phase 1 Goals

1. **Correlation IDs:** Add a 4-byte `correlation_id` header field so clients can multiplex requests over one connection.
2. **API Keys & API Versions:** Standardize `api_key` (Produce = 0, Fetch = 1, Metadata = 3) and `api_version` for protocol evolution.
3. **Numeric Error Codes:** Replace string errors with enum-style numeric codes (e.g., `NONE = 0`, `UNKNOWN_TOPIC_OR_PARTITION = 3`, `CORRUPT_MESSAGE = 7`).
4. **Pipelined Client Connection:** Enable non-blocking pipelined requests in `SyncConnection` / async connection wrappers.

---

## 3. Wire Protocol Specification (v2)

### Fixed Binary Header (16 Bytes)
```
┌──────────┬──────────┬──────────┬──────────┬────────────────┬─────────────────┐
│ magic 4B │ len 4B   │ api_key  │ api_ver  │ correlation_id │ body (JSON/Bin) │
│  0xCAFE  │          │   2B     │    2B    │       4B       │                 │
└──────────┴──────────┴──────────┴──────────┴────────────────┴─────────────────┘
```

---

## 4. Next Implementation Steps

- Update `common/protocol.py` to support v2 header encoding/decoding.
- Add error code definitions in `common/errors.py`.
- Update `broker/broker.py` request routing logic to map `api_key` and echo `correlation_id`.
- Update `producer/producer.py` and `consumer/consumer.py` to process v2 responses and handle numeric error codes.
