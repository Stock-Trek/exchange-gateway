# Design: moving to a send-and-wait paradigm with a listener only for partial responses

Issue: #209 (design task — no code changes)

## 1. Summary

The gateway connector currently exposes a *fire-and-forget* `send()` that returns
`()`, while *every* response — request/response replies and push data alike — is
delivered to a user-supplied listener. This document investigates what it would
take to move to a *send-and-wait* paradigm where `send()` returns the exchange's
response and the listener is reserved for *partial responses* (push/streaming
messages that are not replies to a specific request).

The good news: **most of the machinery already exists.** The transport layer
already has `send_and_wait_for(request, timeout, filter)`, the WebSocket
listener already routes correlated responses to waiters (consuming them before
they reach the user's listener), and `sync_clock()`/`authenticate()` already use
send-and-wait. The main work is (a) changing the public `send()` signature and
return path, (b) defining how a response is correlated to a user request
(filter/id strategy), and (c) deciding the fate of the listener parameter
per transport.

## 2. Current paradigm (fire-and-forget + listener for all responses)

### Public API

- `Connector<ExternalReq, ExternalRes>` (`src/connector.rs`):
  `async fn send(&self, request: ExternalReq, signed: bool, timeout: Duration) -> EGResult<()>`.
  `ExternalRes` appears in the trait only via the listener plumbing, never as a
  return value.
- `Connect::binance_http` / `Connect::binance_websocket` (`src/connect.rs`) take
  `to_unsigned_request`, `to_external_response`, and a `listener:
  impl ListenerTrait<TMessage = ExternalRes>`.
- Users call `send(...).await?`, get `()`, and observe results in their
  listener's `on_message`.

### Transport level

`TransportTrait` (`src/transports/transport.rs`) already offers both styles:

```rust
async fn fire_and_forget(&self, request: EGReq, timeout: Duration) -> EGResult<()>;
async fn send_and_wait_for(&self, request: EGReq, timeout: Duration,
                           filter: ArcPredicate<EGRes>) -> EGResult<EGRes>;
```

- **HTTP** (`src/transports/http.rs`): `fire_and_forget` performs the round
  trip, applies rate-limit feedback, converts the response, and forwards it to
  `self.listener.on_message(response)`; the caller still only gets `()`.
  `send_and_wait_for` performs the same round trip and returns the converted
  response (subject to a filter). HTTP is strictly request/response — there are
  no partial responses.
- **WebSocket** (`src/transports/websocket.rs`): `fire_and_forget` only sends the
  frame and returns; the reply arrives later through the `WebsocketListener`.
  `send_and_wait_for` registers a waiter with the listener
  (`waiter_for_filtered_response(filter)`), sends, and awaits the waiter with a
  timeout.

### The WebSocket listener already routes responses

`WebsocketListener::on_message` (`src/listeners/websocket_listener.rs`):

1. Applies rate-limit feedback and converts the transport message.
2. Checks registered `ResponseHandler`s (waiters). If a handler's filter
   matches, the message is delivered to that waiter's `WaiterForResponse`
   future and **is not** forwarded to the delegate listener.
3. Otherwise the message is forwarded to the delegate (the user's listener
   wrapped in `ConvertListener`).

Waiters are self-cleaning: `WaiterForResponse::drop` removes the handler from
the list, so timeouts/cancellations do not leak handlers.

### Send-and-wait is already used internally

`ConnectorImpl` (`src/connector_impl.rs`) uses `send_and_wait_for` for:

- `sync_clock()` — a `Time` request whose filter matches the response by id
  (`synchronization` in `src/specs/binance/http.rs` / `websocket.rs`).
- `authenticate()` — each `AuthenticateLeg` builds a `Logon` request with a
  fresh internally-generated UUID id and a filter matching `response.id == id`.

Only the user-facing `send()` uses `fire_and_forget`.

## 3. Target paradigm

- `send()` (or a new equivalent) performs a send-and-wait: it returns the
  exchange's response, or an error (`TimedOut`, `ApiError`, `RateLimited`,
  `HttpError`, `BadResponse`, ...).
- The listener receives only *partial responses* — messages that are not the
  reply to any user request (e.g. Binance WebSocket API push streams: user-data
  execution reports, order/trade updates, tickers, klines, depth, bookTicker).
- For HTTP there are no partial responses, so the listener becomes unused there.

Because `WebsocketListener::on_message` already consumes waiter-matched messages
before reaching the delegate, **routing user requests through send-and-wait
automatically restricts the listener to partial responses** with no change to
the listener's dispatch logic.

## 4. What the change involves (code touch points)

1. **`Connector` trait (`src/connector.rs`)**: `send()` must return the
   response — either change `send` to `EGResult<ExternalRes>` (breaking) or add
   a second method (see §5).

2. **`ConnectorImpl` (`src/connector_impl.rs`)**:
   - `send()` switches from `transport.fire_and_forget(...)` to
     `transport.send_and_wait_for(..., filter)`.
   - The response returned by the transport is `EGRes` (e.g.
     `BinanceHttpResponse` / `BinanceWebsocketResponse`); the connector must
     apply the user's `to_external_response` conversion before returning
     `ExternalRes`. Today that conversion lives only in the listener path
     (`ConvertListener`), so `ConnectorImpl` needs access to it (new field or
     parameter).
   - A `filter` for the request must be produced. This requires either a new
     injected correlation function (e.g. `to_filter: fn(&EGUnsignedReq) ->
     ArcPredicate<EGRes>`) or internally-generated ids (§5.3).
   - On a send failure before/at `send_message`, any registered waiter must be
     dropped so it cannot consume a later response (the existing `Drop` impl
     handles this).
   - Rate-limit handling stays as-is: acquire before sending, apply feedback on
     the response; refunds continue to apply on `RateLimited`. `TimedOut`
     intentionally does not refund — the request was accepted by the exchange.

3. **HTTP spec (`src/specs/binance/http.rs`)**: `send` returns the response via
   the existing `HttpTransport::send_and_wait_for`; the filter for HTTP should
   simply accept any response (`|_| true`) since HTTP replies are always
   correlated. The listener parameter becomes redundant and can be dropped or
   kept as an unused hook (see Option C).

4. **WebSocket spec (`src/specs/binance/websocket.rs`)**: `send` returns the
   response via the existing `WebsocketTransport::send_and_wait_for` with an
   id-based filter. Partial responses (unmatched messages) continue to flow to
   the user listener through the existing delegate path. The
   `WebsocketListener` machinery needs **no changes**.

5. **`Connect` (`src/connect.rs`) and examples**: public constructor signatures
   and the example (`examples/binance_http.rs`) must follow the chosen API
   shape.

## 5. Design options

### Option A — Minimal change: swap `send` to send-and-wait, keep everything else

- Change `Connector::send` to return `EGResult<ExternalRes>`.
- `ConnectorImpl::send` uses `send_and_wait_for` with an injected filter and
  applies `to_external_response`.
- Keep the listener parameter in both constructors (HTTP: never called;
  WebSocket: partial responses only).
- Pros: smallest diff; reuses the existing waiter/filter machinery and the
  existing sync-clock/auth path; partial-response filtering falls out for free.
- Cons: breaking `send()` return type; HTTP keeps a vestigial listener in the
  API; listener remains mandatory even for users who only want request/response.

### Option B — Add `send_and_wait` alongside `fire_and_forget`

- Keep the current `send` (fire-and-forget) and add e.g.
  `send_and_wait(request, signed, timeout) -> EGResult<ExternalRes>`.
- Pros: fully backward compatible; callers choose the paradigm per call.
- Cons: two code paths and two response-delivery models to support; the old
  "listener for all responses" behaviour remains available, so the paradigm
  shift is opt-in rather than enforced; the `WebsocketListener` delegate path
  stays "all responses" for fire-and-forget users.

### Option C — Restructure the public API: optional, partial-response-only listener

- `send()` returns `EGResult<ExternalRes>` (as in A).
- Make the listener optional per transport:
  - HTTP: remove the listener parameter entirely (no partial responses exist).
  - WebSocket: the listener is required only when the caller wants push data;
    otherwise it can be omitted.
- Pros: cleanest user-facing contract; matches the target paradigm exactly;
  no dead listener path for HTTP.
- Cons: largest breaking change; HTTP and WebSocket constructor signatures
  diverge; requires deciding how to express "no listener" (e.g. an
  `Option<impl ListenerTrait>` parameter or a no-op default listener).

### Option D — Subscription model (larger conceptual change)

- Model *requests* (send-and-wait) and *subscriptions* (persistent push
  registrations delivered via the listener) as distinct concepts, with explicit
  subscribe/unsubscribe APIs for partial data.
- Pros: matches how exchange APIs actually model push data; makes "partial
  responses" first-class and self-describing.
- Cons: significant API surface and state management (subscription registry,
  resubscribe-on-reconnect, idempotency); beyond the scope of the immediate
  ask. Worth revisiting later; Options A–C are compatible stepping stones.

### 5.1 Implementation decision made

Implementation decision made: Option A. Minimal change: swap `send` to send-and-wait, keep everything else.

### 5.2 Response correlation / filter strategy (needed for A/B/C)

For Binance's WebSocket API every request carries a `metadata.id` that the
server echoes in the reply; partial responses carry ids that never match an
in-flight request (or none at all). Sub-options:

- **(i) Internally-generated ids**: the spec generates a fresh UUID id per
  request (the existing `id()` helper in `src/specs/binance/common.rs`) and
  stamps it on the request after `to_unsigned_request`; the filter is
  `response.id == request_id`. Safe for Binance because the signature covers
  only `params`, not `metadata.id`. Pros: no caller burden, guaranteed
  uniqueness (important with concurrent sends). Cons: the connector overrides
  the caller-supplied id.
- **(ii) Caller-supplied ids + injected filter**: keep the caller's id and
  inject a correlation function (`to_filter`) into `ConnectorImpl`. Pros:
  flexible for future exchanges. Cons: caller must generate and manage unique
  ids; uniqueness bugs silently steal responses between concurrent waiters.
- **(iii) Hybrid**: keep caller ids for requests that already carry them, but
  generate internally when absent. Most flexible; more surface area.

Implementation decision made: **(i)** for Binance (internally-generated ids, filter supplied
by the spec), expressed generically as an injected `to_filter` so other
exchanges can plug in their own correlation rules.

## 6. Key decisions / open questions

1. **Breaking change**: change `send()`'s return type (A/C) vs add a method (B).
   Decision made: Go with option A and change the return type.
2. **Listener ownership of ids**: does the caller keep supplying
   `metadata.id`, or does the connector own id generation (5.3)?
   Decision made: The connector shall own id generation, overwrite the user's id.
3. **HTTP listener**: remove it (C), keep it as an unused hook (A), or keep a
   fire-and-forget path for it (B)?
   Decision made: Keep it as an unused hook.
4. **Who converts the response**: `ConnectorImpl::send` must apply
   `to_external_response` on the return path; decide whether the transport
   should convert to `ExternalRes` directly or the connector does it after
   `send_and_wait_for` (the latter keeps transports exchange-agnostic).
   Decision made: Keep transports exchange-agnostic, the connector should convert the response.
5. **Fire-and-forget retention**: keep `TransportTrait::fire_and_forget` for
   internal/legacy use, or remove once unused.
   Decision made: Remove fire_and_forget if it becomes unused.
6. **Versioning**: the change is a semver-major API break for the library
   (`send` return type, constructor signatures); plan a release accordingly.
   Decision made: This is an internal tool which isn't yet in use, do not worry about this.

## 7. Risks and edge cases

- **Concurrent sends**: each send registers its own waiter; id uniqueness (5.3i)
  is required so waiters never match each other's responses. HTTP is
  unaffected (request/response is inherently correlated).
  Decision made: Id generation will be handled by the connector so uniqueness is almost mathematically guaranteed.
- **Connection loss with pending waiters**: today a waiter whose request was
  sent just before a disconnect waits out the full timeout. Consider failing
  pending waiters promptly on `WebsocketListener::on_disconnected` (e.g. a
  `NotConnected`/connection-lost variant in `WaiterState`, which currently only
  carries `filtered_response` and `rate_limited`).
  Decision made: Fail pending waiters promptly on `WebsocketListener::on_disconnected`.
- **Reconnect / re-auth**: a response to a pre-reconnect request cannot arrive
  on the fresh connection (the server answers in order per connection), so the
  waiter times out and `TimedOut` is surfaced; the existing `AuthGate` epoch
  logic already forces re-authentication on reconnect. No change expected, but
  the interplay should be tested.
  Decision made: Add tests to ensure correct expected behaviour.
- **Error replies on WebSocket**: Binance replies to bad requests with a
  matching-id response carrying `error`; under send-and-wait this reaches the
  caller via the normal return path (the spec's `validate_response`-style
  mapping must be applied to user sends, not just logon/time).
  Decision made: Apply the `validate_response` style mapping to user sends.
- **Rate-limit accounting**: requests that time out still consumed weight on the
  exchange; do not refund on `TimedOut` (current refund logic only fires on
  `RateLimited`, which is correct).
  Decision made: Keep the correct logic.
- **Response conversion errors**: `to_external_response` failing on the return
  path must map to a `send()` error, not a listener crash.
  Decision made: Yes, map a conversion error to a `send()` error.
- **Waiter leak on send failure**: if `send_message` fails (e.g.
  `ConnectionClosed` while reconnecting), the registered waiter must be dropped
  before returning the error — `WaiterForResponse::drop` already removes the
  handler, so the design must ensure the waiter is constructed only after a
  successful send (or dropped on the error path).
  Decision made: Yes, construct the waiter only after a successful send (or dropped on the error path).

## 8. Implementation approach

1. Adopt **Option A** as the core change because the transport and listener
   machinery already support send-and-wait end-to-end:
   - `Connector::send` returns `EGResult<ExternalRes>`.
   - `ConnectorImpl` gains a `to_filter` (correlation) function and applies
     `to_external_response` on the return path.
   - Binance WebSocket spec generates request ids internally (5.3i).
   - HTTP spec passes an always-true filter.
2. Keep the `WebsocketListener` dispatch untouched; verify with tests that
   waiter-matched responses no longer reach the user listener and that push
   messages still do.
3. Add prompt failure of pending waiters on disconnect (edge case above) in the
   same change.
4. Update `Connect` and `examples/binance_http.rs`.
