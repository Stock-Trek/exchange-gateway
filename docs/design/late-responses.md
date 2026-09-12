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
3. an explicit indeterminate outcome that always carries the submission id; and
4. exchange reconciliation as the way to actually resolve an indeterminate
   outcome.

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
2. make **reconciliation by id** possible;
3. keep the happy path ergonomic — callers who only want the response should not
   pay for the machinery;
4. work for both transports, without forcing exchange-specific concepts into
   `Connector` (which is generic over `ETExchange`).

## 4. Requirements

| # | Requirement |
|---|-------------|
| R1 | A submission id is assigned once per logical request and exposed to the caller. |
| R2 | The id is stable across internal retries so retries can be idempotent. |
| R3 | An indeterminate outcome always surfaces the id. |
| R4 | Existing `send_*` ergonomics are preserved for the simple case. |
| R5 | The design stays transport- and exchange-agnostic. |
| R6 | Rate-limit accounting and refund semantics are unchanged. |

## 5. Options

### Option B — `submit_*` handle that exposes the id up front

Split the send API so the id is available before awaiting:

```rust
pub struct Submission<Response> {
    id: SubmissionId,
    // future that completes with the response
}

impl<Response> Submission<Response> {
    pub fn id(&self) -> &SubmissionId;
    pub async fn wait(self) -> EGResult<SubmissionOutcome<Response>>;
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

### Option E — Reconciliation by id (query what happened)

After `Unknown`, the caller queries the exchange for the status of the
submission id (e.g. order status by client order id). The gateway can provide a
generic hook, but the actual query is exchange-specific.

- Pros: the only *authoritative* resolution for non-idempotent operations whose
  response was truly lost; already how mature trading clients handle lost acks.
- Cons: requires exchange-specific support; must live above/alongside
  `exchange-types`; not something `Connector` can provide generically today.

### Option F — Explicit outcome enum

Replace the "response or error" return with an explicit two-way outcome, leaving
definitive failures as ordinary `Err` values and wrapping the whole thing in
`EGResult`:

```rust
pub enum SubmissionOutcome<Response> {
    Confirmed(Response),
    Indeterminate(SubmissionId),     // unknown; reconcile by id
}

// callers receive EGResult<SubmissionOutcome<Response>>:
//   Ok(Confirmed(response))   -> exchange definitively accepted
//   Ok(Indeterminate(id))     -> outcome unknown; reconcile by id
//   Err(EGError::Send { .. }) -> definitively rejected or never sent
```

- Pros: makes the indeterminate state impossible to ignore; always carries the
  id (R3); definitive failures keep `?` ergonomics; pairs naturally with B.
- Cons: a breaking, more verbose API.

## 6. Comparison

| Option | Exposes id up front | Generic | Cost |
|--------|--------------------|---------|------|
| B — `submit` handle | yes | yes | medium API change |
| E — reconcile by id | via B | no (exchange-specific) | high |
| F — outcome enum | yes | yes | medium breaking |

## 7. Recommendation

Adopt **B + F as the core**, with **E** documented as the authoritative
fallback:

1. **Introduce a `SubmissionId`** in the gateway, wrapping/serialising to
   `ETWebsocketId` where needed. Generate it once per logical submission and
   reuse it across internal retries (fixes R2 and the current per-retry UUID).
2. **Add `submit_http` / `submit_websocket`** returning a `Submission<Response>`
   handle that exposes `id()` immediately and is awaitable for the response.
   Keep `send_http` / `send_websocket` as thin convenience wrappers so existing
   callers are unaffected (R4).
3. **Return `EGResult<SubmissionOutcome<Response>>`** from the handle's
   `wait()`, so indeterminate outcomes always carry the submission id while
   definitive rejections and not-sent errors stay ordinary `Err` values (R3).
4. **Document reconciliation by id** as the way to resolve non-idempotent
   operations whose response was truly lost (E).

This preserves the current happy path while giving a clear, low-ceremony path
for the failure case:

```rust
let submission = connector.submit_websocket(request)?;
let id = submission.id().clone();
match submission.wait().await {
    Ok(SubmissionOutcome::Confirmed(response)) => { /* confirmed */ }
    Ok(SubmissionOutcome::Indeterminate(id)) => {
        // later, possibly after a reconnect:
        connector.reconcile(id).await
    }
    Err(other) => { /* rejected / NotSent */ }
}
```

## 8. Risks and edge cases

- **Retry id stability**: today the connector regenerates a UUID per attempt and
  on retry; the id must move out of the loop and be assigned once.
- **Rate-limit accounting**: if a response does arrive before the deadline it
  still carries rate-limit usage and possibly `Retry-After`, so the normal
  `set_rate_limits` path must apply. Refund semantics on `Unknown` stay as-is.
- **Multi-exchange ids**: `ETWebsocketId` is `Int | Str`; expose a
  `SubmissionId` that can represent both and convert losslessly.
- **Cancellation**: dropping a `Submission` without calling `wait` must still
  deregister its waiter; it cannot be left registered forever.
- **Lost responses**: with no retention, an indeterminate outcome can only be
  resolved by exchanging the submission id with the venue; callers must treat
  reconciliation as authoritative and must not blindly retry.

## 9. Open questions

1. Should `send_*` be deprecated in favour of `submit_*`, or kept indefinitely as
   the simple path?
2. Should `SubmissionOutcome` be the primary return type, or an accessor on the
   handle (with `wait()` returning `EGResult<Response>` for ergonomics)?
3. Does `exchange-types` need a generic way to stamp a submission id on HTTP
   requests, or is that left to each exchange spec?
