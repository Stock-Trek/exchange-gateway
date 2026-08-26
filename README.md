# exchange-gateway

A Rust gateway for communicating with cryptocurrency exchanges, modelled as
unified **connectors**. A connector bundles the transports (HTTP, WebSocket,
Iris), signing (HMAC-SHA256, Ed25519, ECDSA P-256/P-384), credentials, rate
limiting, and listeners needed to talk to an exchange through one typed
interface.

Currently ships with a Binance (spot) specification built on
[`exchange-types`](https://shipyard.rs/crates/exchange-types/0.4.7).

> **Status: v0.9.0 — not yet production ready.** Signed order placement is
> blocked by a bug in the upstream `query-params` derive that this repository
> cannot fix (see [Known blockers](#known-blockers)). Everything else found
> during the production-readiness review has been fixed in this release; see
> [Production readiness](#production-readiness).

## Features

| Feature   | Enables                                             |
| --------- | --------------------------------------------------- |
| (default) | Core gateway, signing, credentials, rate limiting   |
| `iris`    | Iris (WebSocket messaging) transport, serde support |
| `reqwest` | HTTP transport via `reqwest`                        |
| `serde`   | Serialization support for signed messages           |

## Quick start

```toml
[dependencies]
exchange-gateway = { version = "0.9", features = ["iris", "reqwest", "serde"] }
exchange-types = { version = "0.4", registry = "shipyard" }
```

### REST connector

```rust
use exchange_gateway::prelude::*;
use exchange_gateway::urls::TradingMode;
use std::sync::Arc;

let listener: Arc<dyn ListenerTrait<TMessage = String>> = Arc::new(MyListener);

let connector = Connect.binance_http(
    TradingMode::Paper,                             // testnet
    ReqwestHttpClient::new,                         // or your own HttpClientTrait
    Arc::new(to_unsigned_request),                  // MyRequest -> BinanceHttpUnsignedRequest
    Arc::new(to_transport_request),                 // BinanceHttpRequest -> HttpRequest
    Arc::new(to_binance_response),                  // HttpResponse -> BinanceHttpResponse
    Arc::new(to_external_response),                 // BinanceHttpResponse -> String
    listener,
    Some(credentials),                              // ApiKeyCredentials, or None
);

connector.connect().await?;
connector.send(my_request, signed, Duration::from_secs(10)).await?;
```

A complete, runnable REST example is in
[`examples/binance_http.rs`](examples/binance_http.rs):

```sh
cargo run --example binance_http --features reqwest,iris,serde
```

### WebSocket connector

```rust
let connector = Connect.binance_websocket(
    TradingMode::Paper,
    |base_url, listener| IrisWebsocketClient::new(base_url, listener),
    Arc::new(to_unsigned_request),                   // MyRequest -> BinanceWebsocketUnsignedRequest
    Arc::new(to_transport_request),                  // BinanceWebsocketRequest -> WebsocketReq
    Arc::new(to_binance_response),                   // WebsocketRes -> BinanceWebsocketResponse
    Arc::new(to_external_response),                  // BinanceWebsocketResponse -> String
    listener,
    Some(credentials),
    true,                                            // use_session: logon + authenticated session
);
```

In session mode the connector sends `session.logon` on connect and, once
authenticated, allows requests through a signer that attaches no signature
(the WebSocket API permits omitting `apiKey`/`signature` after a successful
logon). Note that `apiKey` inside order params is then redundant; set it to
`None` to avoid sending it alongside the authenticated session.

## Production readiness

Reviewed against the official Binance documentation
([rest-api.md](https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md),
[web-socket-api.md](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-api.md),
[errors.md](https://github.com/binance/binance-spot-api-docs/blob/master/errors.md))
and the `exchange-types` 0.4.7 sources.

### Fixed in this release

- **REST `myFilters` could never authenticate.** `/api/v3/myFilters` is a
  signed (USER_DATA) endpoint, but the gateway produced no signature payload
  for it. It now signs `timestamp` (+ `recvWindow`/`symbols`).
- **REST signatures were computed over a payload that included `apiKey`.**
  Binance sends the api key in the `X-MBX-APIKEY` header; including it in the
  signed query either invalidates the signature or is rejected as an unknown
  parameter. The REST payload now excludes it (the WebSocket API correctly
  keeps it).
- **Panics on invalid input.** `RateLimiterState` asserted instead of refusing
  when a request cost exceeded bucket capacity; connectors panicked on
  `expect("Failed to create signer")` when given invalid credentials (or when
  `use_session` was requested without credentials). All of these now return
  `Err(EGError::...)`.
- **Silent empty-URL requests.** A request for a missing REST endpoint
  produced `""` instead of failing. It now returns
  `EGError::UnknownEndpoint`.
- **Rate limits modelled incorrectly.** The single 1200-weight/min bucket was
  four times laxer than Binance's `REQUEST_WEIGHT` (6000/min) for orders, and
  made `exchangeInfo` polls compete with the order budget. The connector now
  has separate `weight` (6000/min) and `orders` (50 per 10 s + 160000/day)
  buckets, matching Binance's real limits. The `r#type`/`apiKey` payload
  fixes above are covered by unit tests.

### Known blockers

- **Signed order placement does not work end-to-end (upstream).** The
  `query-params` derive (v1.0.1, used by `exchange-types` 0.4.7) serializes
  the raw identifier `r#type` as `r%23type` instead of `type`. Empirically:

  ```
  query_params(true) = apiKey=...&price=100&quantity=1&r%23type=LIMIT&side=BUY&...
  ```

  Binance expects `type=LIMIT`, so the request is rejected (`-1103` unknown
  parameter) and the signature over `r%23type` never matches a canonical
  query (`-1022`). The gateway now keeps its *signed payload* canonical by
  normalising `r%23type=` → `type=`, but the request query string your
  transport layer builds must be normalised the same way, e.g.:

  ```rust
  let mut query = params.query_params(true);
  query = query.replace("r%23type=", "type="); // until the derive is fixed
  ```

  The definitive fix is upstream: the derive must emit `type` for `r#type`
  (e.g. via `syn::Ident::unraw`). Until then, signed orders — REST and
  WebSocket API alike — cannot be placed.

### Remaining limitations (by design or out of scope)

- **Rate limits are local, per-process.** Binance's `REQUEST_WEIGHT` limit is
  per IP and `ORDERS` is per account; multiple processes/instances behind one
  IP can still exceed them, and the library does not parse `429` /
  `Retry-After` responses (it refunds the estimated cost on transport
  failure instead).
- **HTTP status codes are not checked by the gateway.** `BinanceHttpResponse`
  is an untagged `Result<...> | BinanceError`; the response-to-`BinanceHttpResponse`
  converter you provide must map non-2xx statuses (see the example).
- **`exchangeInfo` response fields are a subset.** The REST response type
  models `rateLimits` and a reduced `permissions` set; the upstream type does
  not include all `ExchangeInfo` fields (e.g. `timezone`, `serverTime`).
- **WebSocket API `exchangeInfo` carries REST-only params.** The upstream
  `exchangeInfo` params type includes `permissions`, which the WebSocket API
  rejects; omit it (`None`) there.
- **No `session.logout`** is modelled; sessions are closed by dropping the
  connection.
- **`send` timeouts** are passed to the transport; the Iris transport
  currently ignores them.
