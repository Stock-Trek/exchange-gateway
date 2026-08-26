# exchange-gateway

A Rust gateway for communicating with cryptocurrency exchanges, modeled as
unified **connectors**. A connector bundles the transports (HTTP, WebSocket,
Iris), signing (HMAC/Ed25519/ECDSA, sessions), credentials, rate limiting, and
listeners needed to talk to an exchange through one typed interface.

Currently ships with a Binance (spot) specification built on
[`exchange-types`](https://shipyard.rs/crates/exchange-types/0.4.4).

> **Status: v0.9.0 — not yet production ready.** The session-based WebSocket
> flow works end to end, but signed order placement (REST and per-request-signed
> WebSocket) is still blocked by a serialization bug in the `query-params`
> dependency. See [Production readiness](#production-readiness) for the full
> findings and what was fixed in this release.

## Features

| Feature   | Enables                                                    |
| --------- | ---------------------------------------------------------- |
| (default) | Core gateway, signing, credentials, rate limiting          |
| `iris`    | Iris (WebSocket messaging) transport, serde support        |
| `reqwest` | HTTP transport via `reqwest`                               |
| `serde`   | Serialization support for signed messages                  |

## Quick start

```toml
[dependencies]
exchange-gateway = { version = "0.9", features = ["iris", "reqwest", "serde"] }
exchange-types = { version = "0.4", registry = "shipyard" }
```

### WebSocket session flow (works)

The connector is built from your converters: an `ExternalReq` is turned into a
`BinanceWebsocketUnsignedRequest`, signed if credentials are supplied (and
`use_session = false`), sent over the transport, and responses are converted
back to your message type for the listener.

```rust
use exchange_gateway::prelude::*;
use exchange_gateway::urls::TradingMode;
use exchange_types::binance::websocket::BinanceWebsocketUnsignedRequest;
use std::sync::Arc;

let listener: Arc<dyn ListenerTrait<TMessage = String>> = Arc::new(MyListener);

let connector = Connect.binance_websocket(
    TradingMode::Paper,                       // testnet
    |base_url, listener| IrisWebsocketClient::new(base_url, listener),
    |request: MyRequest| Ok(to_unsigned(request)), // -> BinanceWebsocketUnsignedRequest
    |request| Ok(to_transport(request)),            // -> TransportReq
    |message| Ok(to_binance_response(message)),     // -> BinanceWebsocketResponse
    |response| Ok(to_external(response)),           // -> String
    listener,
    Some(credentials),                        // ApiKeyCredentials, or None
    true,                                     // use_session: logon once, then unsigned
);

connector.connect().await?;
connector.send(my_request, signed, Duration::from_secs(10)).await?;
```

A complete, runnable REST example is in
[`examples/binance_http.rs`](examples/binance_http.rs):

```sh
cargo run --example binance_http --features reqwest,iris,serde
```

> Note: Binance geo-blocks some cloud/regional IPs with HTTP `451`, so the
> example needs an unrestricted network to run live.

## Architecture

```
                    ┌────────────────────────────────────────────┐
   external message │  ConnectorImpl                             │
   ───────────────► │  to_unsigned_request ─► EGUnsignedRequest  │
                    │  signer (optional)    ─► EGRequest          │
                    │  rate limiter                               │
                    │  transport (HTTP | WebSocket | Iris)        │
                    │  listener ◄────────── external responses    │
                    └────────────────────────────────────────────┘
```

Every connector is a small composition of `Arc`-wrapped functions, so users
supply their own converters between the gateway's generic message types and
their domain model.

## Production readiness

Review findings for v0.9.0, verified against the live Binance API
documentation and, where possible, empirically.

### Fixed in v0.9.0

- **WebSocket signature payload now includes `apiKey`.** Binance's documented
  `ws-api` signing rule is to sign all params except `signature` — *including*
  `apiKey`, unlike REST where `apiKey` travels in a header. The payload was
  previously computed from the request params alone, so every signed
  `ws-api` request — `session.logon` above all — produced an invalid signature
  and the session-based flow could not authenticate. The fix is covered by a
  regression test that reproduces Binance's canonical documented example
  (`apiKey=...&timestamp=...` → the exact hex signature from the docs).
- **`ws-api` request weights corrected.** `exchangeInfo` is documented as
  weight 20 (was 1) and `session.logon` as weight 2 (was 1) on the WebSocket
  API. Under-counting let the rate limiter admit more traffic than the
  exchange allows, risking HTTP `429` bans.
- **`exchange-types` bumped 0.4.2 → 0.4.4**, pulling in upstream fixes that
  unblock parsing of real API traffic:
  - `BinanceError.code` is now `i64`; Binance sends numeric codes, so error
    responses previously failed serde and surfaced as timeouts instead of
    clean errors.
  - `BinanceHttpResponse` is now an untagged enum (`Result | Error`) instead
    of a struct with `flatten` + untagged `Option`, which silently swallowed
    parse failures and dropped the HTTP status.
  - `BinanceRateLimit.count` is optional and `RAW_REQUESTS` was added to
    `BinanceRateLimitType`, so `exchangeInfo` responses (which include
    `RAW_REQUESTS` limits without `count`) now deserialize. Verified against a
    realistic response payload.

### Remaining blockers (dependencies, not fixable in this repository)

1. **Signed orders are rejected: the `type` field serializes as `r%23type`.**
   The `query-params` derive used by `exchange-types` serializes the raw
   identifier `r#type` as `r%23type` instead of `type`
   (verified empirically against both 0.4.2 and 0.4.4). This corrupts the
   query string used for the signing payload, so:
   - REST signed orders (`POST /api/v3/order`) always fail — either the
     signature mismatches or Binance rejects the unknown `r%23type`
     parameter (`-1104`).
   - Per-request signed WebSocket orders (`use_session = false`) fail the
     same way.
   - Orders after a successful `session.logon` (`use_session = true`) are
     sent unsigned per the docs and are **not** affected.
   Fixing this requires a change to `query-params`/`exchange-types` upstream,
   after which signed orders should be re-tested end to end.
2. **`AssetLimits` is unusable as modeled.** The unit variant maps to the real
   signed `/api/v3/myFilters` endpoint (`symbol`, `timestamp`, `signature`
   are all required) but the gateway treats it as an unsigned request, so it
   is always rejected. It needs a signed request shape (`symbol`, params) and
   a correct response model.

### Verified working

- `session.logon` request shape and signing match the docs; omitting
  signatures after logon is correct per docs.
- `ws-api` responses include `rateLimits` by default, so the mandatory
  non-`Option` field is fine.
- Testnet URL `wss://ws-api.testnet.binance.vision/ws-api/v3` matches the
  official docs.
- `exchangeInfo` (REST and ws-api) parses correctly with `exchange-types`
  0.4.4, including `RAW_REQUESTS` rate limits and numeric error codes.
- `cargo test`, `cargo test --all-features`, `cargo fmt`, and
  `cargo clippy --all-features` pass.

### Operational notes

- `connect()` for HTTP is a no-op flag flip; there is no handshake. For
  WebSocket, connection is delegated to the transport (Iris handles
  reconnect/backoff via its own config).
- Failed logon/request error bodies now surface as `EGError::BadResponse`
  (or the transport error) rather than timing out.
