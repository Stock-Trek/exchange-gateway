# exchange-gateway

A Rust gateway for communicating with cryptocurrency exchanges, modeled as
unified **connectors**. A connector bundles the transports (HTTP, WebSocket,
Iris), signing (HMAC/Ed25519/ECDSA, sessions), credentials, rate limiting, and
listeners needed to talk to an exchange through one typed interface.

Currently ships with a Binance (spot) specification built on
[`exchange-types`](https://shipyard.rs/crates/exchange-types/0.4.3).

> **Status: v0.9.0 — not yet production ready.** See
> [Production readiness](#production-readiness) for the concrete findings and
> bugs discovered during review. The session-based WebSocket flow works, and
> `exchange-types` 0.4.3 fixes the error-model and `exchangeInfo` parsing bugs,
> but signed orders are still broken by the raw-identifier bug in the
> `query-params` derive, and `AssetLimits` is still unusable.

## Features

| Feature  | Enables                                                    |
| -------- | ---------------------------------------------------------- |
| (default) | Core gateway, signing, credentials, rate limiting          |
| `iris`   | Iris (WebSocket messaging) transport, serde support        |
| `reqwest`| HTTP transport via `reqwest`                               |
| `serde`  | Serialization support for signed messages                  |

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
use exchange_types::binance::logon::BinanceLogonParams;
use exchange_types::binance::websocket::BinanceWebsocketUnsignedRequest;
use std::sync::Arc;

let listener: Arc<dyn ListenerTrait<TMessage = String>> = Arc::new(MyListener);

let connector = Connect.binance_websocket(
    TradingMode::Paper,                       // testnet
    |base_url, listener| IrisTransport::new(base_url, listener),
    |request: MyRequest| Ok(to_unsigned(request)), // -> BinanceWebsocketUnsignedRequest
    |request| Ok(to_transport(request)),            // -> IrisMessage
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

Review findings against the live Binance spot API and testnet, as of v0.9.0
with `exchange-types` 0.4.3.

### Remaining bugs

These still block the listed flows. Fixing them requires changes to
`exchange-types` / its `query-params` derive, after which the gateway should be
re-tested end to end.

1. **Signed order parameters serialize `type` as `r%23type`** (raw-identifier
   bug in the `query-params` derive; still present in `query-params` 1.0.1).
   The gateway signs over this broken string, so Binance rejects every signed
   order with `-1022`. Affects REST orders and WebSocket orders signed
   per-request (`use_session = false`).
2. **`AssetLimits` is unusable.** The model's unit variant maps to the real
   signed `/api/v3/myFilters` endpoint (`symbol`, `timestamp`, `signature` are
   required) but the gateway treats it as an unsigned request, so it is always
   rejected.

### Fixed in `exchange-types` 0.4.3

These bugs were raised in the review, fixed upstream, and verified here by
regression tests (`specs::binance::tests::exchange_types_0_4_3`):

1. **`BinanceError.code` was typed `String`, but Binance sends numeric codes**
   (`{"code":-2014,"msg":"..."}`). Real error responses failed serde
   deserialization, so failed logons/requests surfaced as timeouts instead of
   clean errors. `code` is now `i64`, so error responses parse cleanly and the
   `#91` logon filter rejects failed logons properly.
2. **HTTP responses silently deserialized as `result: None, error: None`**.
   `BinanceHttpResponse` is now an untagged `Result | Error` enum, so a parse
   failure is no longer swallowed; a successful `exchangeInfo` request now
   returns the payload instead of an empty result.
3. **`exchangeInfo` responses could never parse.** `BinanceRateLimit.count` is
   now optional (exchangeInfo responses omit it) and `BinanceRateLimitType`
   gained the `RAW_REQUESTS` variant returned by the live API.

### Fixed in this repository

- **WebSocket signature payload now includes `apiKey`** (Binance's documented
  signing rule for `ws-api`: all params except `signature`, including
  `apiKey`, sorted and joined). Previously the payload was computed before
  `apiKey` was appended, so every signed WebSocket request — including
  `session.logon` — produced an invalid signature. With this fix the
  session-based flow works end to end. Covered by a regression test that
  reproduces Binance's canonical HMAC example.
- **`exchangeInfo` request weight 1 → 20**, matching the measured weight of
  the exact request shape the library sends (`permissions=SPOT&
  symbolStatus=TRADING`).
- **Rate-limit weight is refunded** when signing or sending fails; `refund`
  can no longer push the limiter above its configured capacity.
- **`wait_for_response` no longer spuriously times out** when a response
  arrives exactly at the deadline (waiter is polled before the delay).
- **`Connector` trait is exported from the prelude**, so `connect`/`send`/
  `disconnect` are usable without a manual trait import.

### Verified working

- `session.logon` request shape matches the docs (params carry `timestamp`,
  `apiKey`, `signature`); omitting signatures after logon is correct per docs.
- Binance includes `rateLimits` in `ws-api` responses by default, so the
  mandatory non-`Option` field is fine.
- Testnet URL `wss://ws-api.testnet.binance.vision/ws-api/v3` matches the
  official docs.
- `cargo test`, `cargo test --all-features`, `cargo fmt`, and
  `cargo clippy --all-features` pass.
