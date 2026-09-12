# Design: ergonomic handling of late responses

Issue: #356 (investigation task — no production code changes required)

## 1. Summary

When a request is slow, times out, or the connection drops, the caller of
`Connector::send_http` / `Connector::send_websocket` receives an error but the
exchange may still process the request and send a reply later. Today that *late
response* has nowhere to go: the waiter is dropped, the id that would have
matched it is discarded, and the caller is left with an indeterminate outcome
and no ergonomic way to find out what actually happened.

The key observation from the issue is that **every request can carry its own
submission id, and that id can be freely exposed to the user**. This document
investigates options for using that id to make late/indeterminate responses
ergonomic, and recommends a staged design:

1. a per-submission id that is stable across internal retries and is exposed to
   the caller;
2. a `submit`-style API that hands the id back *before* the response is awaited;
3. a bounded, TTL'd late-response store so a response that arrives after the
   caller has given up is retained instead of discarded;
4. an explicit indeterminate outcome that always carries the submission id; and
5. stable-id retries / exchange reconciliation as the way to actually resolve an
   indeterminate outcome.

No code changes are made by this task; this is an investigation and design
document.

## 2. Current behaviour

### 2.1 Send-and-wait

`Connector` (`src/connector.rs`) exposes two request/response methods:

- `send_http<Response>(&self, request: impl ETHttpRequest<...> + Clone) -> EGResult<Response>`
- `send_websocket<Response>(&self, request: impl ETWebsocketRequest<...> + Clone) -> EGResult<Response>`

Both perform the same shape of work:

1. validate/acquire rate-limit capacity (`validate_rate_limits`);
2. take a server-time estimate and stamp it on the request;
3. convert the request into a transport request;
4. send it, wait for the response, and convert it;
5. retry if the request is idempotent and the error `is_retryable()`;
6. refund rate-limit capacity only when the error `was_not_sent()`.

### 2.2 Where the submission id lives

For WebSocket requests, a submission id already exists: `ETWebsocketRequest::
try_into_websocket(signer, id: ETWebsocketId)` stamps the id on the wire and
returns a `WebsocketResponseMatcher` that matches the echoed id
(`exchange_types::websocket_id::ETWebsocketId`).

Crucially, the connector generates that id **internally and never exposes it**:

```rust
let id = ETWebsocketId::Str(uuid::Uuid::new_v4().to_string());
let (websocket_request, response_matcher) =
    request.clone().try_into_websocket(&self.signer, id)?;
```

Two consequences:

- On any failure the caller has no id, so it cannot correlate anything later.
- The id is regenerated **inside the retry loop**, so even the same logical
  request uses a different id on each attempt. Stable-id idempotency is not
  possible today.

For HTTP there is no generic submission id at all. The request carries whatever
the exchange spec puts in it (e.g. a client order id), but the gateway has no
notion of one.

### 2.3 What happens to a late response

WebSocket path (`send_wait` → `websocket_listener.rs`):

- A `WaiterForResponse` is registered with the listener.
- `send_wait` races the waiter against `Delay::new(remaining)`.
- If the timeout wins, it returns `EGError::send_unknown(EGError::TimedOut)`.
- On return the waiter is dropped; `WaiterForResponse::drop` removes the handler
  from `WebsocketListener::handlers`, so a response that arrives afterwards is
  never matched by any handler and is silently discarded.

HTTP path:

- `ReqwestHttpClient::send` applies the timeout to the `reqwest` request and
  maps a timeout after connection to `EGError::send_unknown_external(error)`.
- The HTTP request/response exchange is inherently correlated, but once the
  future is cancelled there is no background task retaining the eventual reply.

### 2.4 The error taxonomy

`EGError::Send { failure: SendFailure, source: Box<EGError> }` already
distinguishes the important cases (`src/error.rs`):

- `SendFailure::NotSent` — the request definitely never reached the exchange;
- `SendFailure::Failed` — the exchange definitively rejected/failed it;
- `SendFailure::Unknown` — the outcome is indeterminate.

`Unknown` is exactly the late-response case, but it carries no id and there is
no follow-up API.

## 3. Problem statement

For an indeterminate send the caller needs to be able to answer: *did my request
execute, and if so, what was the response?* Today they cannot, and the natural
reaction (retry) can execute a non-idempotent request twice. Any solution must:

1. expose a **submission id** to the caller, including in the error case;
2. either **retain the late response** for later collection, or make
   **reconciliation by id** possible;
3. remain **bounded** in memory and time (a late response may never arrive);
4. keep the happy path ergonomic — callers who only want the response should not
   pay for the machinery;
5. work for both transports, without forcing exchange-specific concepts into
   `Connector` (which is generic over `ETExchange`).

## 4. Requirements

| # | Requirement |
|---|-------------|
| R1 | A submission id is assigned once per logical request and exposed to the caller. |
| R2 | The id is stable across internal retries so retries can be idempotent. |
| R3 | An indeterminate outcome always surfaces the id. |
| R4 | A late response can be observed, with bounded memory and a TTL. |
| R5 | Existing `send_*` ergonomics are preserved for the simple case. |
| R6 | The design stays transport- and exchange-agnostic. |
| R7 | Rate-limit accounting and refund semantics are unchanged. |

## 5. Options

### Option A — Carry the submission id on the error only

Add an id field to `EGError::Send` (or a new `EGError::Indeterminate { id, .. }`).
The caller learns the id when they get `Unknown` and can use it with a new
reconciliation method.

- Pros: tiny change; no new public types; direct mapping onto the existing
  `SendFailure::Unknown`.
- Cons: the id is only available *after* failure; on cancellation (the caller
  drops the future) there is no error and no id; still no place for the late
  response to land. Solves naming but not handling.

### Option B — `submit_*` handle that exposes the id up front

Split the send API so the id is available before awaiting:

```rust
pub struct Submission<Response> {
    id: SubmissionId,
    // future that completes with the response
}

impl<Response> Submission<Response> {
    pub fn id(&self) -> &SubmissionId;
    pub async fn wait(self) -> EGResult<Response>;
}

// on Connector:
pub fn submit_http<Response>(&self, request: impl ETHttpRequest<...> + Clone)
    -> EGResult<Submission<Response>>;
```

- Pros: R1 is satisfied even if the caller drops the future; cancellation still
  leaves a known id; natural building block for the other options.
- Cons: changes the shape of the public API. If the whole send loop (rate limit,
  clock, signing, retries) is inside the returned future, the id cannot be known
  until the loop starts; so the id must be generated eagerly and passed in. The
  handle must also be `Send` and own the retry loop.

### Option C — Bounded late-response store

Keep orphaned responses instead of discarding them. When the caller's wait
deadline elapses, hand the outstanding waiter to a `LateResponseStore` keyed by
submission id, with a TTL (e.g. the request timeout, or a configurable grace
period) and a capacity cap (LRU/FIFO eviction). Provide collection APIs:

```rust
pub fn take_late_response<Response>(&self, id: &SubmissionId)
    -> Option<EGResult<Response>>;
pub fn poll_late_response(...);
```

plus an optional push callback:

```rust
pub fn on_late_response<F>(&self, callback: F)
where F: Fn(&SubmissionId, EGResult<serde_json::Value>) + Send + Sync + 'static;
```

- Pros: directly solves the "response arrives later" case for WebSocket; the
  caller can recover the actual response without an exchange round trip.
- Cons: needs a store, eviction policy, and a decision about the raw-vs-typed
  response (the store sits at the transport/JSON level, but `take_late_response`
  wants a typed `Response`, which requires keeping the conversion function
  alongside the waiter). HTTP requires keeping the request alive in a background
  task after the caller's deadline (see §6).

### Option D — Stable caller-supplied ids + idempotent retry

Allow the caller to supply the submission id and have both internal retries and
caller-initiated retries reuse it:

```rust
pub struct SendOptions {
    pub submission_id: Option<SubmissionId>,
    // ...
}
```

On `Unknown`, the caller retries with the *same* id. If the exchange dedupes by
submission id (e.g. a client order id), the retry is safe.

- Pros: turns an indeterminate request into a safely retryable one; lean, no new
  background state; composes with A/B.
- Cons: only as good as the exchange's dedupe support; the gateway cannot
  guarantee it generically. Requires `exchange-types` to expose a way to stamp an
  id on HTTP requests (today only WebSocket requests take an `ETWebsocketId`).

### Option E — Reconciliation by id (query what happened)

After `Unknown`, the caller queries the exchange for the status of the
submission id (e.g. order status by client order id). The gateway can provide a
generic hook, but the actual query is exchange-specific.

- Pros: the only *authoritative* resolution for non-idempotent operations whose
  response was truly lost; already how mature trading clients handle lost acks.
- Cons: requires exchange-specific support; must live above/alongside
  `exchange-types`; not something `Connector` can provide generically today.

### Option F — Explicit outcome enum

Replace the "response or error" return with an explicit three/four-way outcome:

```rust
pub enum SubmissionOutcome<Response> {
    Confirmed(Response),
    Rejected(EGError),               // exchange definitively rejected
    NotSent(EGError),                // never reached the exchange
    Indeterminate(SubmissionId),     // unknown; reconcile or retry by id
}
```

- Pros: makes the missing state impossible to ignore; always carries the id
  (R3); pairs naturally with B/C/D.
- Cons: a breaking, more verbose API; returning errors as values loses `?`
  ergonomics unless a convenience `into_result()` is offered.

### Option G — Keep waiters alive after caller timeout (no separate store)

Instead of a store, change the timeout semantics: the caller's `send_*` keeps
the waiter registered for a grace period and only then abandons it, delivering a
late response to a callback if one is registered. This is a variant of C with
lifetime tied to the caller's future rather than a central registry.

- Pros: no global store; lifetime is scoped.
- Cons: the caller has already been told the request failed; coupling the
  handler lifetime to a dropped future is awkward, and the grace period is hard
  to express ergonomically.

## 6. Transport-specific considerations

### WebSocket

The machinery already exists and is close to what Option C needs. The waiter is
registered before the send and removed on drop. To retain late responses we
would:

- split the waiter lifetime from the caller future: when the deadline fires,
  move the `WaiterForResponse` (or its state) into the store rather than
  dropping it, and keep the response matcher alive;
- keep the conversion closure (`Response::try_from_websocket`) with the stored
  entry so `take_late_response` can return a typed value;
- evict entries by TTL/capacity.

### HTTP

There is no push channel, so a late HTTP reply can only be observed if the
request is *not* cancelled. `ReqwestHttpClient::send` cancels when its future is
dropped. To make Option C work for HTTP, `send_http` would have to run the
request in a `tokio::spawn`ed task and race the join handle against the
deadline, keeping the join handle's result for the store on timeout. That adds
`Send + 'static` bounds and a task per request; alternatively, HTTP callers rely
on Option D/E (safe retry or status query) and the store is WebSocket-only.

## 7. Comparison

| Option | Exposes id up front | Retains late response | Bounded | Generic | Cost |
|--------|--------------------|-----------------------|---------|--------|------|
| A — id on error | no | no | n/a | yes | trivial |
| B — `submit` handle | yes | no | n/a | yes | medium API change |
| C — late-response store | via B | yes | yes | mostly (HTTP caveat) | high |
| D — stable-id retry | via B/A | no | n/a | exchange-dependent | medium |
| E — reconcile by id | via B/A | no | n/a | no (exchange-specific) | high |
| F — outcome enum | yes | no | n/a | yes | medium breaking |

## 8. Recommendation

Adopt **A + B + C + F as the core**, with **D** as an opt-in and **E** documented
as the authoritative fallback:

1. **Introduce a `SubmissionId`** in the gateway, wrapping/serialising to
   `ETWebsocketId` where needed. Generate it once per logical submission and
   reuse it across internal retries (fixes R2 and the current per-retry UUID).
2. **Add `submit_http` / `submit_websocket`** returning a `Submission<Response>`
   handle that exposes `id()` immediately and is awaitable for the response.
   Keep `send_http` / `send_websocket` as thin convenience wrappers so existing
   callers are unaffected (R5).
3. **Add a bounded, TTL'd late-response store** keyed by `SubmissionId`. When a
   wait deadline elapses, move the outstanding waiter into the store instead of
   dropping it. Expose `take_late_response`/`poll_late_response` and an optional
   `on_late_response` callback (R4). Start WebSocket-only; leave the WebSocket
   path untouched otherwise.
4. **Add the id to indeterminate errors** (`SendFailure::Unknown`) and/or expose
   the explicit `SubmissionOutcome<Response>` from the handle, so no caller can
   observe `Unknown` without the id (R3).
5. **Allow caller-supplied ids and stable-id retries** (`SendOptions`) for
   exchanges that dedupe, and document reconciliation by id as the way to
   resolve non-idempotent operations (R2/D/E).

This preserves the current happy path while giving a clear, low-ceremony path
for the failure case:

```rust
let submission = connector.submit_websocket(request)?;
let id = submission.id().clone();
match submission.wait().await {
    Ok(response) => { /* confirmed */ }
    Err(error) if error.is_unknown() => {
        // later, possibly after a reconnect:
        if let Some(late) = connector.take_late_response::<Response>(&id) { /* ... */ }
        // or: connector.retry_with_id(request, id).await
        // or: connector.reconcile(id).await
    }
    Err(other) => { /* NotSent / Failed */ }
}
```

## 9. Risks and edge cases

- **Race at the deadline**: a response can arrive between the timeout firing and
  the waiter being moved into the store. The move must be atomic with respect to
  `WebsocketListener::on_message` (both take the `handlers` lock), or the store
  must be checked after registration. Mitigation: perform the "retain" under the
  same lock, or register with the store and the listener as one operation.
- **Unbounded growth**: a store of never-answered ids leaks memory. Must enforce
  both a capacity cap with eviction and a TTL sweep.
- **Typed vs raw responses**: the store must keep the conversion path so
  `take_late_response` can return `Response`, not just `serde_json::Value`.
- **Retry id stability**: today the connector regenerates a UUID per attempt and
  on retry; the id must move out of the loop and be assigned once.
- **Duplicate in-flight ids**: caller-supplied ids must be rejected or deduped
  to avoid two waiters matching one response.
- **Rate-limit accounting**: a late response still carries rate-limit usage and
  possibly `Retry-After`; the store path must still apply `set_rate_limits`, or
  capacity accounting drifts. Refund semantics on `Unknown` stay as-is.
- **Multi-exchange ids**: `ETWebsocketId` is `Int | Str`; expose a
  `SubmissionId` that can represent both and convert losslessly.
- **Backpressure / cancellation**: dropped `Submission` handles must not leave
  waiters registered forever; dropping without calling `wait` should still hand
  ownership to the store (or clean up if the store is full).
- **HTTP cancellation**: capturing a late HTTP response requires backgrounding
  the request; document that HTTP recovery is retry/reconcile-based unless this
  cost is accepted.

## 10. Open questions

1. Should the late-response store be opt-in (constructed with a capacity/TTL) or
   always present with a default? Opt-in keeps zero overhead for callers who do
   not want it.
2. Should `send_*` be deprecated in favour of `submit_*`, or kept indefinitely as
   the simple path?
3. Should `SubmissionOutcome` be the primary return type, or an accessor on the
   handle (with `wait()` returning `EGResult<Response>` for ergonomics)?
4. Does `exchange-types` need a generic way to stamp a submission id on HTTP
   requests, or is that left to each exchange spec?
5. What is the default grace TTL for a late response, and should it be
   configurable per connector or per call?
