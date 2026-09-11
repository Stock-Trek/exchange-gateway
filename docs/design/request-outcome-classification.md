# Design: classifying request outcomes (succeeded / failed / not sent / unknown)

Issue: #350 (investigation — options, no code changes)

## 1. Summary

`Connector::send_http` and `Connector::send_websocket` return `EGResult<Response>`.
Callers need an easy, reliable way to tell, for every request, which of four
things happened:

1. **Definitely succeeded** — a valid response was received.
2. **Definitely failed** — the exchange received and rejected the request.
3. **Failed to send** — the request never left the client.
4. **Sent with unknown outcome** — the request may have reached the exchange,
   but no usable response was received (e.g. a timeout or a dropped connection).

Today `EGError` is transport-oriented, not outcome-oriented, so this
classification is unreliable:

- A **mid-request HTTP timeout** surfaces as `EGError::External`
  (`src/clients/reqwest.rs`), indistinguishable from a signing, serialisation or
  decoding failure — and it is not classed like `NotSent`.
- `EGError::NotSent` is only produced for a subset of pre-send failures (HTTP
  connect errors and WebSocket send errors). Clock, signing and rate-limit
  pre-flight failures are plain errors even though the request was definitely
  not sent.
- `EGError::TimedOut` is **overloaded**: `src/clients/iris.rs` wraps a
  send-phase timeout as `NotSent(TimedOut)` (definitely not sent), while
  `src/connector.rs::send_wait` returns a bare `TimedOut` when the message was
  already handed to the transport (unknown outcome). A caller cannot tell the
  two apart.
- After a WebSocket `TimedOut`, `WaiterForResponse::drop`
  (`src/websocket_listener.rs`) removes the handler from the listener. A response
  that arrives later is **silently dropped** and there is no API to observe it,
  so the caller never learns whether, for example, an order landed.

This document audits the current behaviour and presents options. It does not
change any code.

> **Update after master merge (#354/#355).** While this investigation was in
> progress, master added a bounded automatic retry for idempotent requests to
> `send_http` / `send_websocket` (the constructors now take
> `max_retry_attempts`, and errors are filtered through `is_retryable`). That
> implements a conservative first slice of Option E and is audited in §3.4; the
> core classification ask (Option A) is still outstanding. The rest of this
> document has been updated to reflect the merged behaviour.

## 2. Desired outcome taxonomy

The four classes above can be modelled as a small public enum. `Ok(response)`
maps to `Succeeded`; every `Err(EGError)` maps to exactly one of the other three.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestOutcome {
    /// A valid response was received.
    Succeeded,
    /// The exchange received the request and rejected it; it did not take effect.
    Failed,
    /// The request definitely never left the client.
    NotSent,
    /// The request may have been sent; the outcome is unknown.
    Unknown,
}
```

Three important observations frame the whole design:

- **"Definitely not sent" and "unknown" cannot always be decided from the HTTP
  client's error alone.** `reqwest` exposes `Error::is_connect()`,
  `is_timeout()`, `is_request()`, `is_body()` and `is_decode()`. A connect error
  (or a request-build error) proves the request never reached the wire; once the
  connection is established, a timeout or body error only proves it *might*
  have. The classification must be applied at the transport boundary where this
  information still exists, not inferred later from a generic variant.
- **An unknown outcome cannot be resolved by classification alone.** For order
  placement the only authoritative answer is to reconcile with the exchange
  (e.g. query by client order id). Classification tells the caller *when* to
  reconcile; it cannot tell them the result. This is addressed as Option D.
- **Whether an unknown outcome is safe to retry depends on the request, not the
  error.** The `exchange-types` version this crate depends on now exposes
  `ETRequest::is_idempotent()` (defaulting to `false`), so the connector can
  attach a retry-safety hint to an unknown outcome without inventing new
  per-request metadata. Master has already used this to auto-retry idempotent
  requests (§3.4); classifying the outcome (Option A) and deciding whether to
  retry (Option E) remain separate concerns.

## 3. Audit of current behaviour

### 3.1 HTTP send path (`send_http`, `sync_clock_http`)

| Stage / failure | Current error | True outcome |
| --- | --- | --- |
| `server_time_estimate` fails | `ClockNotSynced`, `SystemTimeBeforeUnixEpoch`, `MutexPoisoned` | not sent (plain error) |
| `validate_rate_limits` fails | `RateLimited` (local), `RequestExceedsRateLimit`, `InvalidRateLimit*`, `MutexPoisoned` | not sent (plain error) |
| `request.try_into_http` fails | `External` | not sent (plain error) |
| `ReqwestHttpClient::send`, `error.is_connect()` | `NotSent(External)` | not sent |
| `ReqwestHttpClient::send`, other error incl. timeout | `External` | **unknown** (classification bug) |
| `response.bytes()` fails after headers | `External` | **unknown** (server responded) |
| `handle_http_response` sees `Retry-After` | `RateLimited` | sent; exchange throttled |
| `handle_http_response` sees 429 | `RateLimited` | failed (server responded; throttled) |
| `handle_http_response` other non-2xx | `HttpError { status, body }` | failed (server responded) |
| `handle_http_response` parsing fails | `HttpParseError { source }` | response received; semantics unclear |
| `set_rate_limits` sees `Retry-After` | `RateLimited` | sent; exchange throttled |
| success | `Ok(response)` | succeeded |

The key defect is the fifth row: `reqwest` already knows whether the error was a
connect error (`is_connect()`) or a timeout after the connection was
established (`is_timeout()`), but the mapping collapses everything that is not a
connect error into `External`.

### 3.2 WebSocket send path (`send_websocket`, `send_wait`, `sync_clock_websocket`)

| Stage / failure | Current error | True outcome |
| --- | --- | --- |
| clock / rate-limit / signing pre-flight | same as HTTP | not sent (plain error) |
| `waiter_for_filtered_response` fails | `MutexPoisoned` | not sent |
| no listener configured | `WebsocketListenerMissing` | not sent |
| `IrisWebsocketClient::send_message_with_delay` times out before `client.send` resolves | `NotSent(TimedOut)` | not sent |
| `IrisWebsocketClient::send`, `ConnectionClosed` / `SendMessage` | `NotSent(External)` | not sent (assumed) |
| `IrisWebsocketClient::send`, other error | `External` | unknown (assumed) |
| `send_wait` waiter times out after a successful send | `TimedOut` | **unknown** (may have executed) |
| `Response::try_from_websocket` fails | `WebsocketParseError { source }` | response received; semantics unclear |
| `set_rate_limits` sees `Retry-After` | `RateLimited` | sent; throttled |
| success | `Ok(response)` | succeeded |

The `send_wait` waiter-timeout row is the second defect: `TimedOut` here means
"sent with unknown outcome", but it shares a variant with the send-phase timeout
that means "definitely not sent".

### 3.3 Late responses

`Connector::send_wait` registers a waiter with the listener, sends, then races
the waiter against a `futures_timer::Delay`. If the delay wins, it returns
`EGError::TimedOut`; the `WaiterForResponse` is dropped when the stack unwinds,
and `WaiterForResponse::drop` removes the handler:

```rust
impl Drop for WaiterForResponse {
    fn drop(&mut self) {
        if let Ok(mut handlers) = self.handlers.lock() {
            handlers.retain(|handler| !Arc::ptr_eq(&handler.state, &self.state));
        }
    }
}
```

Any matching message that arrives after the timeout therefore finds no handler,
is not delivered to any waiter, and is forwarded to the user listener only if
the user happens to have registered one for that traffic — which is exactly the
case the correlation machinery is supposed to prevent. The response is
effectively lost, and the caller has no way to ask for it later. There is no
HTTP equivalent, but an HTTP timeout leaves the same uncertainty about whether
the exchange acted.

### 3.4 Automatic retry for idempotent requests (`send_http` / `send_websocket`)

Master added a bounded retry loop to `Connector::send_http` and
`Connector::send_websocket` (issue #354, PR #355). The constructors now take a
`max_retry_attempts: u8`, and each send acquires its rate-limit cost once up
front and then:

1. reads `request.is_idempotent()` and `request.is_signed()`;
2. sets `retries_remaining = max_retry_attempts` only when the request is
   idempotent, so non-idempotent requests are never retried;
3. attempts the send, and on error retries while `retries_remaining > 0` and
   `is_retryable(error)` is true;
4. re-acquires the rate-limit cost for each retry whose previous error was not
   `NotSent` (a `NotSent` error never reached the wire, so it is not charged
   again);
5. on the final error calls `on_send_error`, which refunds only `NotSent`.

`is_retryable` is currently variant-based:

| Error | Retryable? |
| --- | --- |
| `NotSent(_)` | yes |
| `TimedOut` | yes |
| `External(_)` | yes |
| `HttpError { status: 408 }` | yes |
| `HttpError { status >= 500 }` | yes |
| `RateLimited` | no |
| `HttpParseError` / `WebsocketParseError` | no |
| clock / signing / rate-limit pre-flight errors | no |

Restricting retries to idempotent requests makes retrying an unknown outcome
(the waiter `TimedOut`, or a post-send `External`) safe in that case. It also
shows why outcome classification is still needed: the policy is expressed in
terms of *error variants* rather than *outcomes*, because `TimedOut` and
`External` still conflate "definitely not sent" with "sent, outcome unknown".
For non-idempotent requests nothing is retried, so the caller must be able to
tell "the request was never sent" from "the request may have executed"
(Option A) in order to decide whether to reconcile.

## 4. Options

### Option A — Additive classification: a `RequestOutcome` enum and `EGError::outcome()`

Keep the existing return type and error variants, but classify them explicitly
and make the classification correct at the point each error is created.

1. Add the public `RequestOutcome` enum from §2 and an accessor:

   ```rust
   impl EGError {
       pub fn request_outcome(&self) -> RequestOutcome { /* Failed | NotSent | Unknown */ }
   }
   // or, ergonomically, on the result:
   pub trait ResultOutcomeExt<T> {
       fn request_outcome(&self) -> RequestOutcome;
   }
   ```

2. Add a dedicated variant for "sent, outcome unknown", e.g.
   `EGError::UnknownOutcome(Box<EGError>)`, and use it for:
   - HTTP `is_timeout()` once the connection is established,
   - HTTP `is_body()` / `is_decode()` errors,
   - WebSocket waiter timeouts,
   - any other post-send transport error we cannot prove is not-sent.

   Keep `NotSent(Box<EGError>)` as the single wrapper for definitely-not-sent
   errors and produce it consistently for connect errors, request-build errors,
   clock/signing pre-flight failures and pre-send rate-limit failures. Preserve
   the cause inside the wrapper.

3. Classify the variants that are genuinely ambiguous (`RateLimited`,
   `External`, `HttpParseError`, `WebsocketParseError`) explicitly and document
   them. Where a variant is overloaded, split it:
   - `TimedOut` becomes `NotSent(TimedOut)` for the send phase and
     `UnknownOutcome(TimedOut)` for the wait phase;
   - `RateLimited` produced by a local pre-flight check is not sent, while
     `RateLimited` produced from a response (an HTTP 429 or a `Retry-After`
     header) is sent (arguably unknown).

4. Map `reqwest` errors at the boundary in `src/clients/reqwest.rs`:

   ```rust
   .map_err(|error| {
       if error.is_connect() {
           EGError::NotSent(Box::new(EGError::External(Box::new(error))))
       } else if error.is_timeout() || error.is_body() || error.is_decode() {
           EGError::UnknownOutcome(Box::new(EGError::External(Box::new(error))))
       } else {
           EGError::External(Box::new(error))
       }
   })
   ```

   `response.bytes()` errors likewise become `UnknownOutcome`, because headers
   were already received.

**Pros:** small, mostly additive (`EGError` is `#[non_exhaustive]`), directly
answers the ask, gives callers a single method instead of a fragile `match`, and
can be introduced without a major version bump. The `NotSent`/`UnknownOutcome`
distinction is enforced where the transport still has the information.

**Cons:** the classification is *derived* from the error, so it can drift as new
variants are added; `NotSent`/`UnknownOutcome` wrapping changes what existing
callers match on; it does not address late responses.

### Option B — Typed send failure: `SendFailure { outcome, source }`

Make the outcome impossible to miss by attaching it structurally to every error
produced by a send path:

```rust
pub struct SendFailure {
    pub outcome: RequestOutcome, // Failed | NotSent | Unknown
    pub source: Box<EGError>,
}
pub enum EGError {
    // ...
    #[error("{source}")]
    Send(#[source] SendFailure),
}
```

Every `send_http` / `send_websocket` error is funnelled through constructors
that require an outcome; non-send operations (`connect`, clock sync, rate-limit
introspection) keep the existing variants. A variant of this option changes the
public send signature to return `Result<Response, SendFailure>` instead of
`EGResult<Response>`, keeping the two error types separate.

**Pros:** compiler-enforced and impossible for a caller to misread; no reliance
on a derived mapping; the source error is preserved.

**Cons:** the largest change to the public surface — every caller that matches
on `EGError` needs updating; wrapping alters `Display` and source chains unless
carefully done; overkill if most callers only need the classification
occasionally. It also has to answer what "definitely failed" means for a
`HttpParseError` (see §6, open question 3).

### Option C — Observability for late WebSocket responses

This is orthogonal to A/B and addresses the second half of the issue. Three
sub-options, in increasing API surface:

**C1 — Global late-response callback.** Expose
`Connector::on_late_response(Fn(&serde_json::Value))` (or on the listener). On a
waiter timeout the handler is *not* removed; it is marked expired for a TTL
(e.g. the request timeout again, bounded), and a subsequent matching message
invokes the callback instead of the normal listener; the handler is reaped at
the TTL.

- *Pros:* smallest change; lets the caller log or reconcile; reuses the matcher.
- *Cons:* the callback does not know which request the value belongs to; a
  matching message is swallowed from the normal listener; needs TTL/bounds to
  avoid unbounded handler growth.

**C2 — Per-request tracked handle.** Add a tracked send that returns a handle
owning the waiter across the timeout:

```rust
let mut pending = connector.send_websocket_tracked(request);
match pending.wait().await {
    Ok(response) => { /* succeeded */ }
    Err(EGError::UnknownOutcome(_)) => {
        // The waiter stays registered; optionally keep waiting for a late reply.
        if let Some(response) = pending.wait_late(Duration::from_secs(30)).await {
            // reconcile: the order did land
        }
    }
    Err(error) => { /* not sent / failed */ }
}
```

- *Pros:* per-request correlation (the id is generated internally by
  `send_websocket` already); the caller controls how long to wait; no global
  state; race-free because the waiter is only dropped when the handle is.
- *Cons:* new public API type; needs careful lifetime/drop semantics and must
  ensure a late wait cannot race an ordinary wait.

**C3 — Bounded late-response store keyed by request id.** The connector exposes
the correlation id it already generates, keeps timed-out waiters (or their
matchers) in a bounded, TTL-reaped map, and offers
`Connector::take_late_response(id) -> Option<serde_json::Value>`.

- *Pros:* no callback; the caller polls when convenient and can persist the id
  for later reconciliation.
- *Cons:* requires exposing the id (API change); memory bounds; the id is
  exchange-specific and may not be meaningful to the caller.

For any of C1–C3, a design decision is needed on memory: handlers/messages must
be bounded and reaped on a TTL, and must not survive a reconnect that makes a
reply impossible.

### Option D — Reconcile at the exchange layer (complementary, recommended)

Classification can tell a caller that an order's outcome is unknown; it cannot
tell them the order's state. The robust operational pattern for order placement
is:

1. Stamp every order with a client-generated id (client order id), which
   `exchange-types` request types already support for most exchanges.
2. On `UnknownOutcome`, do not blindly retry; query the exchange by that id
   (order status / open orders / trades) and reconcile.
3. Only mark read-only requests (market data, balances) as safe to retry on
   `UnknownOutcome`.

This is exchange-specific and belongs partly in the exchange integrations, but
it is the only way to answer "did the order land?". The outcome taxonomy is what
tells the caller when to invoke it. It should be documented as the recommended
recovery path regardless of which of A–C is chosen.

### Option E — Split by request safety instead of by error

The request metadata this option needs now exists. `exchange-types` provides it
on the request trait itself, so no new flag or exchange-specific extension is
required:

```rust
pub trait ETRequest: Serialize {
    // ... existing methods ...
    /// Whether a request whose outcome is unknown is safe to retry.
    /// Defaults to `false`, so requests are presumed unsafe unless an
    /// exchange integration opts in.
    fn is_idempotent(&self) -> bool {
        false
    }
}
```

Instead of (or in addition to) classifying errors, expose the distinction
through this metadata: mark requests as *idempotent/safe to retry* or *not safe
to retry*, and have the connector return a "retryable" classification for
`UnknownOutcome`. This makes the actionable decision explicit for callers who do
not want to reason about transport phases.

**Partially implemented in master (#354/#355).** `send_http` and
`send_websocket` now consult `is_idempotent()` and automatically retry up to
`max_retry_attempts` times when `is_retryable(error)` holds (§3.4). What is not
yet implemented is surfacing the same decision to the caller: there is no
`RequestOutcome` and no retry-safety field on the returned error, so a
non-idempotent caller sees the same raw variants the issue complains about.

- *Pros:* directly supports automatic retry logic (already done for idempotent
  requests); connects the outcome to the request type where safety is actually
  known; the trait already carries the metadata (default `false`), so surfacing
  it on the outcome needs no request-spec or trait changes.
- *Cons:* the `false` default means read-only requests will be treated as
  unsafe unless each request overrides it; it does not replace A/B because it
  does not classify not-sent vs failed, and it says nothing about the
  late-response problem in Option C. The current retry filter (`is_retryable`)
  keys off error variants rather than outcomes, so it will need revisiting if
  Option A splits the overloaded variants.

## 5. Recommendation

- **Adopt Option A now.** It is the smallest change that satisfies the core ask
  ("distinguish the four outcomes") without a breaking rewrite, and it puts the
  not-sent vs unknown decision where the transport still has enough information
  (HTTP `is_connect` / `is_timeout` / `is_body`, WebSocket send phase vs wait
  phase). It also fixes the specific reported bug that a mid-request HTTP
  timeout is reported as `External` rather than as an unknown outcome.
- **Follow up with Option C2 (tracked handle).** The issue explicitly calls out
  the silently dropped late WebSocket response; a per-request handle is the
  cleanest way to observe it without global state or an id-exposing API. C1 is
  an acceptable interim if a smaller change is preferred, and C3 is the better
  fit if the caller needs to persist correlation ids for reconciliation.
- **Document Option D as the operational answer for orders.** Classification
  plus reconciliation is what actually resolves "did the order land?".
- **Build on Option E (`ETRequest::is_idempotent()`), which master has already
  wired into automatic retry.** `send_http` / `send_websocket` now perform a
  bounded retry (configured by `max_retry_attempts`) for idempotent requests
  only, using the variant-based `is_retryable` filter (§3.4). Option A is the
  complement: once `UnknownOutcome` is a separate variant, the retry policy can
  be expressed in terms of outcomes, and the returned error can carry the
  retry-safety hint (or callers can consult `is_idempotent()` themselves) so
  non-idempotent callers know to reconcile rather than retry. Because the
  default is `false`, exchange integrations must opt read-only requests in
  explicitly; automatic retry is bounded by `max_retry_attempts` and is not a
  substitute for reconciliation.

Option B is the most robust long-term shape but is a semver-major change to
every call site; it is worth revisiting if callers are found to be
misclassifying errors in practice.

## 6. Key decisions / open questions

1. **Additive vs typed (A vs B).** A keeps the public API and is the
   recommended first step; B is stronger but breaking.
2. **Late-response mechanism (C1 vs C2 vs C3).** C2 is recommended; the
   decision depends on whether callers prefer callbacks, handles, or id-keyed
   polling.
3. **`HttpParseError` classification.** A response was received, but we could
   not interpret it. For a 2xx this could be a successful order with an
   unexpected body — so should it be `Failed` or `Unknown`? Recommendation:
   `Unknown` (conservative; the exchange may have acted).
4. **`RateLimited` classification.** A local pre-flight rejection is not sent; a
   `Retry-After` on a received response means the request was sent and
   (typically) processed. Recommendation: split the two, or classify post-send
   `RateLimited` as `Unknown` and keep pre-flight as not sent.
5. **`NotSent` wrapping breadth.** Do we wrap *all* pre-flight failures (clock,
   signing, rate limit) in `NotSent`, or only transport-level ones and let
   `request_outcome()` classify the rest? Recommendation: produce `NotSent` only
   at the transport boundary; have `request_outcome()` classify the pre-flight
   variants as not sent, so existing `match` arms on `ClockNotSynced` etc. keep
   working.
6. **Late-response TTL and bounds.** What TTL and maximum number of retained
   late handlers/messages? Must be bounded; recommend a multiple of the request
   timeout with a hard cap.
7. **Versioning.** Option A is additive; any change that renames `TimedOut` or
   changes the meaning of existing variants is a breaking change even though the
   enum is `#[non_exhaustive]`, and needs a release note.
8. **Idempotency metadata semantics.** `ETRequest::is_idempotent()` defaults to
   `false`, so an unannotated request is treated as unsafe to retry. Master
   already uses it to drive automatic retries (#354/#355), but it is not exposed
   to callers. Should the connector expose it as a field on `UnknownOutcome`
   (e.g. `UnknownOutcome { retryable: bool }`) or as a separate accessor that
   takes the original request? Either way the transport outcome and the retry
   decision should remain independent, so callers keep control of
   reconciliation.

## 7. Implementation touch points (for Options A + C2 + E)

Option E's automatic retry is already merged; its remaining "surface the hint"
step and Options A/C2 are still to do.

- `src/error.rs`: add `RequestOutcome`, `UnknownOutcome`, `request_outcome()`
  and the `ResultOutcomeExt` helper.
- `src/clients/reqwest.rs`: map `is_connect()` → `NotSent`, 
  `is_timeout()`/`is_body()`/`is_decode()` → `UnknownOutcome`; map
  `response.bytes()` failures to `UnknownOutcome`.
- `src/clients/iris.rs`: keep `NotSent(TimedOut)` for the pre-send timeout;
  ensure post-send failures are not wrapped as `NotSent`; route the wait-phase
  `TimedOut` in `send_wait` to `UnknownOutcome`.
- `src/connector.rs`: `send_wait` returns `UnknownOutcome(TimedOut)` on waiter
  timeout; consider wrapping the pre-flight clock/rate-limit/signing errors
  where appropriate; keep the `on_send_error` refund logic refunding on
  `NotSent` only (as it does today) and not on `UnknownOutcome` (the request may
  have consumed weight).
- `src/connector.rs`: `Request::is_idempotent()` is already read before the
  request is cloned into `try_into_http` / `try_into_websocket` and drives the
  merged bounded retry loop (`max_retry_attempts`, `is_retryable`); the
  constructors take `max_retry_attempts: u8` and the send methods require
  `Clone`. For the remaining Option E work, attach the retry-safety hint to any
  `UnknownOutcome` returned from the send path (or expose a companion accessor).
  Keep the outcome classification and the retry decision independent, and do not
  use `is_idempotent()` to refund rate-limit weight.
- `src/websocket_listener.rs`: for C2, add a way for a waiter to survive its
  timeout (e.g. an owner-held registration or an explicit `release`/`wait_late`
  path) instead of `Drop` always removing the handler; keep `Drop` as the final
  cleanup.
- `src/auto_resync_connector.rs`: propagate the new outcome accessor to the
  auto-resync wrapper's `send_http` / `send_websocket` (the merged retry loop's
  `Clone` bound is already forwarded).
- Tests: unit tests for the `reqwest` error mapping (connect vs timeout vs body),
  `EGError::request_outcome()` for every variant, and an `is_idempotent()`
  propagation test that an unknown outcome from an idempotent request carries
  the retry hint while a non-idempotent one does not, plus a
  `websocket_listener` test that a late matching message reaches `wait_late`
  after a timeout rather than being dropped.

## 8. Risks and edge cases

- **`is_connect()` and `is_timeout()` can both be true** for a connect timeout;
  check `is_connect()` first so a connection that never opened is `NotSent`.
- **Partial writes.** A write error after part of the request was flushed is
  technically unknown, but `reqwest` does not distinguish it from a
  before-write error; the conservative mapping (`is_request()` → `NotSent`) is a
  small risk that should be documented.
- **Body-read errors after a non-2xx status.** Headers carry the status, so a
  body error on a 4xx could be classified `Failed` rather than `Unknown`; the
  current signature reads headers and body separately, so this is possible but
  not required for the first cut.
- **Rate-limit refunds.** `Connector::on_send_error` refunds on `NotSent` only,
  and pre-flight failures refund explicitly before returning. With the new
  taxonomy this is the desired behaviour: `UnknownOutcome` must **not** refund,
  because the exchange may have consumed the weight.
- **Waiter lifetime for C2.** The handle must own the waiter and keep it
  registered until the caller drops it or the TTL expires; a second `wait()` on
  the same handle must be defined (probably returns an error or is consumed).
- **Reconnect.** A reply to a pre-reconnect request cannot arrive on a fresh
  connection, so a late-response TTL must not outlive a reconnect; otherwise C1/C2
  wait for something that can never arrive. Reap or fail late waiters on
  disconnect, matching the existing "fail pending waiters on disconnect"
  decision from the send-and-wait design.
- **Memory growth.** Any late-response retention (C1–C3) must be bounded and
  TTL-reaped; an unbounded list of expired matchers is a denial-of-service risk.
- **Semver.** Splitting `TimedOut` and wrapping existing errors changes the
  observable error values even though `EGError` is `#[non_exhaustive]`; call this
  out in the release notes.
- **Idempotency is not a licence to retry blindly.** `is_idempotent()` describes
  the request's semantics, not the transport, and defaults to `false`; it says
  nothing about whether the exchange has capacity, whether a rate-limit budget
  was already consumed, or whether a retry will race a late response. Master now
  applies a bounded automatic retry (`max_retry_attempts`) to idempotent
  requests via `is_retryable`, with no backoff, so retry should still be treated
  as a policy input (bound the attempts, add backoff, reconcile non-idempotent
  requests). Because `is_retryable` currently treats `TimedOut` and `External`
  (overloaded "unknown outcome" variants) as retryable, an unknown outcome is
  retried automatically for idempotent requests; once Option A lands, that
  decision should be expressed per-outcome.
