# Streamkit Pre-Production Code Audit

**Auditor:** GitHub Copilot (Claude Opus 4.6)  
**Date:** 2026-02-06  
**Scope:** Full codebase — client, server, storage, transport, internal packages  
**Methodology:** Manual line-by-line code review of all production source files

---

## Issue #1: WAL Recovery Fails on Already-Inserted Entries — Store Permanently Unopenable

**Severity:** Critical  
**Category:** Data Corruption / Event Sourcing  
**Location:** `pkg/storage/azurekit/store.go` — `recoverWAL`, `fanoutTransaction`, `writeBatch`

**Problem:**

The WAL recovery mechanism uses INSERT semantics (`AddEntityBatch` → HTTP POST) for fanout. If a crash occurs **after** `fanoutTransaction` writes segment+space entities but **before** `cleanupWAL` deletes the WAL entry, recovery will attempt to re-insert already-existing entities. Azure Table Storage returns 409 Conflict for duplicate PK+RK inserts, failing the entire batch. This causes `recoverWAL` to return an error, which causes `NewAzureStore` to fail — the store becomes **permanently unopenable**.

The code comments claim "Fanout is at-least-once and is driven by WAL transactions; duplicates are expected to be tolerated or deduped using sequence numbers." But the actual implementation uses `POST` (insert-only), not `PUT` (upsert), contradicting this invariant.

**Impact:**

A single crash at the wrong moment permanently corrupts the store. No data can be read or written until manual intervention (deleting the orphaned WAL entity from Azure Table Storage).

**Reproduction:**

1. Start a Produce operation that writes a WAL transaction entity
2. `fanoutTransaction` succeeds (segment + space entities written)
3. Kill the process before `cleanupWAL` runs
4. Restart — `NewAzureStore` calls `recoverWAL` → `fanoutTransaction` → `writeBatch` → `AddEntityBatch` → 409 Conflict → error → store creation fails

**Fix:**

Change `AddEntityBatch` to use upsert semantics, or handle 409 Conflict as success during fanout:

```go
// In AddEntityBatch, change per-entity verb from POST (insert) to PUT (upsert):
fmt.Fprintf(buf, "PUT %s/%s(PartitionKey='%s',RowKey='%s') HTTP/1.1\r\n",
    c.endpoint, c.tableName,
    url.QueryEscape(e.PartitionKey), url.QueryEscape(e.RowKey))
```

**Priority:** Fix NOW — blocks production use. Single crash causes permanent data loss.

---

## Issue #2: Publish Returns Success Before Server Confirms Write

**Severity:** Critical  
**Category:** Event Sourcing / Data Loss  
**Location:** `pkg/client/client.go` — `Publish()` (~line 570)

**Problem:**

`Publish` sends a record, calls `bidi.CloseSend(nil)`, then spawns a **fire-and-forget goroutine** to consume the server's status response. It immediately returns `nil` (success) without waiting for server confirmation:

```go
go func() {
    _ = enumerators.Consume(enumerator)  // Error silently discarded
}()
return nil  // Returns success BEFORE server confirms!
```

Additionally, `defer bidi.Close(nil)` runs when `Publish` returns, potentially closing the stream before the goroutine reads the response.

**Impact:**

- Caller believes write succeeded when it may have failed (sequence conflict, storage error, quota exceeded)
- Event sourcing guarantees are violated — "acknowledged" events can be silently lost
- The fire-and-forget goroutine leaks if `bidi.Close(nil)` closes the stream before it finishes

**Fix:**

Wait for server acknowledgment before returning:

```go
func (c *client) Publish(...) error {
    // ... existing code through bidi.CloseSend(nil) ...

    enumerator := api.NewStreamEnumerator[*SegmentStatus](bidi)
    defer enumerator.Dispose()

    // Wait for at least one status confirmation
    if !enumerator.MoveNext() {
        if err := enumerator.Err(); err != nil {
            return fmt.Errorf("produce failed: %w", err)
        }
        return fmt.Errorf("no status confirmation from server")
    }
    return nil
}
```

**Priority:** Fix NOW — silent data loss in event sourcing system.

---

## Issue #3: Concurrent Produce to Same Segment Causes Data Corruption (PebbleDB)

**Severity:** Critical  
**Category:** Concurrency / Data Corruption  
**Location:** `pkg/storage/pebblekit/store.go` — `Produce()`

**Problem:**

`PebbleStore.Produce` calls `Peek` to get the last sequence number, then validates incoming records against it. Two concurrent Produce calls to the same segment will both Peek the same sequence, both pass validation, and both commit batches with `pebble.Batch.Set` (upsert). The second writer **silently overwrites** entries from the first writer at identical sequence+key positions with different payloads and TRX IDs.

There is no optimistic concurrency control, no locking, and no atomic check-and-set.

```
Thread 1:                        Thread 2:
lastEntry = Peek() → seq=5      lastEntry = Peek() → seq=5
validates records 6,7,8          validates records 6,7,8
batch.Set(segOffset6, data_A)    batch.Set(segOffset6, data_B)
batch.Commit(Sync) ✓             batch.Commit(Sync) ✓  ← OVERWRITES data_A!
```

**Impact:**

Silent data corruption. Different producers write conflicting entries at the same sequence numbers. Event ordering guarantees destroyed.

**Fix:**

Add per-segment write serialization at the store level:

```go
type PebbleStore struct {
    db        *pebble.DB
    cache     *cache.ExpiringCache
    closeOnce sync.Once
    segLocks  sync.Map // per-segment write serialization
}

func (s *PebbleStore) Produce(ctx context.Context, args *api.Produce, records enumerators.Enumerator[*api.Record]) enumerators.Enumerator[*api.SegmentStatus] {
    key := args.Space + ":" + args.Segment
    lockI, _ := s.segLocks.LoadOrStore(key, &sync.Mutex{})
    mu := lockI.(*sync.Mutex)
    mu.Lock()
    defer mu.Unlock()
    // ... rest of Produce ...
}
```

**Priority:** Fix NOW — data corruption.

---

## Issue #4: DecodeTransaction Silently Swallows Entry Decode Errors

**Severity:** Critical  
**Category:** Data Corruption / Error Handling  
**Location:** `internal/codec/codec.go` — `DecodeTransaction()` (~line 279)

**Problem:**

```go
entry := &api.Entry{}
if DecodeEntry(entryData, entry) != nil {
    return err  // BUG: returns `err` from readBytes (nil!), NOT the DecodeEntry error
}
```

The condition checks if `DecodeEntry` returned an error, but the `return` statement returns the **wrong variable** — `err` from the previous `readBytes` call, which is `nil` since readBytes had succeeded. The actual DecodeEntry error is silently discarded, and the function returns `nil` with a partially-initialized transaction struct.

**Impact:**

Corrupted entries in WAL transactions are silently treated as valid. During WAL recovery, garbage `api.Entry` structs get written to the main data tables.

**Fix:**

```go
entry := &api.Entry{}
if decErr := DecodeEntry(entryData, entry); decErr != nil {
    return decErr
}
```

**Priority:** Fix NOW — silent data corruption during WAL recovery.

---

## Issue #5: readMap Silently Swallows Deserialization Errors

**Severity:** High  
**Category:** Data Corruption / Error Handling  
**Location:** `internal/codec/codec.go` — `readMap()` (~line 330)

**Problem:**

```go
for i := uint32(0); i < length; i++ {
    k, _ := readString(buf)  // Error discarded
    v, _ := readString(buf)  // Error discarded
    m[k] = v
}
```

If the binary data is truncated or corrupted, `readString` returns an error that is silently discarded. This produces garbage keys/values in the metadata map without any error indication.

**Impact:**

Corrupted metadata attached to entries. Could cause downstream processing failures or security issues if metadata drives access control.

**Fix:**

```go
for i := uint32(0); i < length; i++ {
    k, err := readString(buf)
    if err != nil {
        return nil, err
    }
    v, err := readString(buf)
    if err != nil {
        return nil, err
    }
    m[k] = v
}
```

**Priority:** Fix before next release.

---

## Issue #6: BidiStreamEnumerator Nil Pointer Panic in Current()

**Severity:** Critical  
**Category:** Crash  
**Location:** `pkg/api/bidi_stream_enumerator.go` — `Current()` (~line 37)

**Problem:**

```go
func (e *BidiStreamEnumerator[T]) Current() (T, error) {
    return *e.current, e.err  // PANIC if e.current is nil
}
```

When `MoveNext()` returns false (stream ended or decode error), `e.current` is set to `nil`. A subsequent call to `Current()` dereferences the nil pointer, causing an unrecoverable panic.

**Impact:**

Any consumer that calls `Current()` after `MoveNext()` returns false will crash the process. This can be triggered by enumerator combinators (`TakeWhile`, `Filter`, etc.) or cleanup code.

**Fix:**

```go
func (e *BidiStreamEnumerator[T]) Current() (T, error) {
    if e.current == nil {
        var zero T
        return zero, e.err
    }
    return *e.current, e.err
}
```

**Priority:** Fix NOW — production crash.

---

## Issue #7: InProcBidiStream.Encode Has Data Race on closeErr

**Severity:** High  
**Category:** Concurrency / Data Race  
**Location:** `pkg/transport/inprockit/bidi_stream.go` — `Encode()` (~line 53)

**Problem:**

```go
func (s *InProcBidiStream) Encode(m any) error {
    if s.closeErr != nil {     // READ without any lock
        return s.closeErr
    }
    // ...
}
```

`closeErr` is written in `closeSend()` under `sendClosed.Do` and `sendCloseMu.Lock()`, but read in `Encode()` with **no synchronization**. This is a data race detectable by `go test -race`.

**Impact:**

Undefined behavior under the Go memory model. Could read torn values or see stale data.

**Fix:**

Remove the closeErr check and rely on the `closed` channel:

```go
func (s *InProcBidiStream) Encode(m any) error {
    select {
    case <-s.closed:
        return io.ErrClosedPipe
    case s.sendChan <- m:
        return nil
    }
}
```

**Priority:** Fix before next release.

---

## Issue #8: pseudoRandState Global Data Race

**Severity:** High  
**Category:** Concurrency / Data Race  
**Location:** `pkg/client/resilience.go` — `pseudoRand()` (~line 230)

**Problem:**

```go
var pseudoRandState int64 = time.Now().UnixNano()

func pseudoRand(max int64) int64 {
    pseudoRandState = (pseudoRandState*1103515245 + 12345) & 0x7fffffff
    return pseudoRandState % max
}
```

`pseudoRandState` is a package-level mutable variable read and written from multiple goroutines (via `applyBackoffJitter` → `pseudoRand`) with **no synchronization**. This is a data race.

**Impact:**

Undefined behavior. Crashes under `-race` flag. In practice, corrupted PRNG state.

**Fix:**

```go
import "sync/atomic"

var pseudoRandState atomic.Int64

func init() {
    pseudoRandState.Store(time.Now().UnixNano())
}

func pseudoRand(max int64) int64 {
    if max <= 0 {
        return 0
    }
    for {
        old := pseudoRandState.Load()
        next := (old*1103515245 + 12345) & 0x7fffffff
        if pseudoRandState.CompareAndSwap(old, next) {
            return next % max
        }
    }
}
```

**Priority:** Fix before next release.

---

## Issue #9: PebbleStore.Close() Leaks Cache Cleanup Goroutine

**Severity:** High  
**Category:** Resource Leak / Goroutine Leak  
**Location:** `pkg/storage/pebblekit/store.go` — `Close()`

**Problem:**

Each `PebbleStore` is created with `cache.NewExpiringCache()`, which starts a background cleanup goroutine. `PebbleStore.Close()` only closes the PebbleDB — it never calls `s.cache.Close()`. The cache goroutine runs forever.

**Impact:**

Every store open+close leaks a goroutine. In a long-running server that creates/destroys stores via `NodeManager`, this causes unbounded goroutine growth.

**Fix:**

```go
func (s *PebbleStore) Close() {
    s.closeOnce.Do(func() {
        s.cache.Close() // Stop cache cleanup goroutine
        if err := s.db.Close(); err != nil {
            slog.Error("pebble: close failed", "err", err)
        }
    })
}
```

**Priority:** Fix before next release.

---

## Issue #10: PebbleStore.Close() Leaks Goroutine on Timeout

**Severity:** High  
**Category:** Goroutine Leak  
**Location:** `pkg/storage/pebblekit/store.go` — `Close()` (~line 47)

**Problem:**

```go
go func() {
    if err := s.db.Close(); err != nil { ... }
    close(done)
}()
select {
case <-done:
case <-time.After(10 * time.Second):
    slog.Warn("pebble: close timeout after 10s, forcing shutdown")
    // ← Goroutine with s.db.Close() continues running FOREVER
}
```

If the 10s timeout fires, `Close()` returns but the goroutine calling `s.db.Close()` is leaked indefinitely, holding references to the `PebbleStore` and `pebble.DB`.

**Impact:**

Leaked goroutine holding database resources, preventing GC and potentially leaving files locked.

**Fix:**

Call `db.Close()` synchronously (PebbleDB close is not cancellable, so wrapping it in a goroutine just hides the problem):

```go
func (s *PebbleStore) Close() {
    s.closeOnce.Do(func() {
        s.cache.Close()
        if err := s.db.Close(); err != nil {
            slog.Error("pebble: close failed", "err", err)
        }
    })
}
```

**Priority:** Fix before next release.

---

## Issue #11: WebSocket Server Auth Bypass — Missing Return After Unauthorized

**Severity:** Critical  
**Category:** Security / Crash  
**Location:** `pkg/transport/wskit/server.go` — `connect()` (~line 35)

**Problem:**

```go
func (s *webSocketServer) connect(c mux.RouteContext) {
    session, err := NewServerMuxerSession(c.User())
    if err != nil {
        c.Unauthorized()
        // BUG: Missing return! Execution continues with nil session
    }
    handler := &webSocketHandler{
        session: session,  // nil when auth fails
    }
    websocket.Handler(handler.handle).ServeHTTP(c.Response(), c.Request())
}
```

After `c.Unauthorized()`, there's no `return`. Code continues to create a WebSocket handler with a nil `session`. When the muxer calls `session.CanAccessStore()`, it panics with nil pointer dereference.

**Impact:**

- Unauthenticated clients can crash the server
- Potential security bypass if the WebSocket upgrade proceeds despite 401

**Fix:**

```go
if err != nil {
    c.Unauthorized()
    return
}
```

**Priority:** Fix NOW — security vulnerability + crash.

---

## Issue #12: WAL Monitor Infinite Restart Loop on Persistent Panic

**Severity:** High  
**Category:** Goroutine Bomb  
**Location:** `pkg/storage/azurekit/store.go` — `walMonitorLoop()` (~line 460)

**Problem:**

```go
defer func() {
    if r := recover(); r != nil {
        time.Sleep(time.Second)
        go s.walMonitorLoop(ctx)  // Restarts recursively
    }
}()
```

If the WAL monitor consistently panics (e.g., nil pointer, corrupted data), it enters: panic → recover → sleep 1s → new goroutine → panic → ... This creates **unbounded goroutines** — one per second.

**Impact:**

Goroutine bomb. Server OOM within minutes if a persistent bug triggers repeated panics.

**Fix:**

Add a restart counter with a maximum:

```go
func (s *AzureStore) walMonitorLoop(ctx context.Context) {
    const maxRestarts = 5
    restartCount := 0
    for restartCount < maxRestarts {
        func() {
            defer func() {
                if r := recover(); r != nil {
                    restartCount++
                    slog.ErrorContext(ctx, "WAL monitor panicked",
                        slog.Any("panic", r), slog.Int("restarts", restartCount))
                }
            }()
            // ... monitor logic ...
        }()
        if restartCount >= maxRestarts {
            slog.ErrorContext(ctx, "WAL monitor exceeded max restarts, stopping permanently")
            return
        }
        time.Sleep(time.Duration(restartCount) * time.Second)
    }
}
```

**Priority:** Fix before next release.

---

## Issue #13: Handler Timeout Leaks Goroutines in Subscriptions

**Severity:** High  
**Category:** Goroutine Leak  
**Location:** `pkg/client/client.go` — `subscribeStream()` (~line 690)

**Problem:**

When handler timeout fires, the subscription loop continues to the next message, but the **handler goroutine keeps running**. If the handler is blocked (on I/O, lock, or slow computation), each timeout spawns a new leaked goroutine.

With 30s timeout and a perpetually blocked handler, after 1 hour: 120 leaked goroutines, each holding the handler's closure and stack.

**Impact:**

Unbounded goroutine accumulation and memory growth proportional to timeout rate.

**Fix:**

Add a cap on concurrent handler goroutines to bound the leak:

```go
var activeHandlers atomic.Int32
const maxActiveHandlers = 10

if activeHandlers.Load() >= maxActiveHandlers {
    slog.Warn("subscription: too many active handlers, skipping message")
    continue
}
activeHandlers.Add(1)
go func() {
    defer activeHandlers.Add(-1)
    handler(&status)
}()
```

**Priority:** Fix before production.

---

## Issue #14: Azure Batch Response Not Parsed for Per-Entity Failures

**Severity:** High  
**Category:** Data Loss  
**Location:** `pkg/storage/azurekit/http_client.go` — `AddEntityBatch()` (~line 490)

**Problem:**

```go
if resp.StatusCode != http.StatusAccepted {
    body, _ := io.ReadAll(resp.Body)
    return parseAzureError(resp, body)
}
return nil  // Assumes ALL entities in batch succeeded
```

Azure batch returns `202 Accepted` for the envelope, but individual operations can fail within the multipart response body. The response body is **never parsed**. Per-entity failures are silently treated as success.

**Impact:**

Individual entity writes can silently fail within a "successful" batch. Data loss without error indication.

**Fix:**

Parse the multipart/mixed response body to check for per-entity status codes.

**Priority:** Fix before production.

---

## Issue #15: signRequest Silently Continues Without Auth on Token Failure

**Severity:** High  
**Category:** Security  
**Location:** `pkg/storage/azurekit/http_client.go` — `signRequest()` (~line 930)

**Problem:**

```go
if c.useBearerToken && c.managedCred != nil {
    token, err := c.managedCred.GetToken(req.Context())
    if err != nil {
        slog.Error("failed to acquire managed identity token", ...)
        return  // Request proceeds WITHOUT any Authorization header!
    }
```

On token failure, the HTTP request is sent without authentication. It will receive 401, but the error surfaces as "Unauthorized" rather than "token acquisition failed."

**Impact:**

Confusing error diagnostics. The 401 may or may not be classified as retryable, potentially causing unnecessary retry storms.

**Fix:**

Set a sentinel error on the request or return an error from `signRequest`:

```go
// Minimal fix: at least mark the failure
req.Header.Set("X-Auth-Error", "token acquisition failed")
```

Better: refactor `signRequest` to return `error`.

**Priority:** Fix before production.

---

## Issue #16: retryableRequest time.Sleep Ignores Context Cancellation

**Severity:** Medium  
**Category:** Context Handling  
**Location:** `pkg/storage/azurekit/http_client.go` — `retryableRequest()` (~line 170)

**Problem:**

```go
time.Sleep(delay)  // Blocks up to 10s, ignoring ctx.Done()
```

During retry backoff, context cancellation is not checked. Shutdown takes up to 10 extra seconds.

**Fix:**

```go
select {
case <-time.After(delay):
case <-ctx.Done():
    return nil, ctx.Err()
}
```

**Priority:** Fix before production.

---

## Issue #17: Azure HTTP Methods Don't Use retryableRequest

**Severity:** Medium  
**Category:** Resilience  
**Location:** `pkg/storage/azurekit/http_client.go` — `AddEntity()`, `UpsertEntity()`, `DeleteEntity()`, `FetchPage()`

**Problem:**

`retryableRequest` implements retry with exponential backoff for transient failures, but NONE of the main API methods use it. All call `c.httpClient.Do(req)` directly. Only the `processChunkWithRetry` layer retries, covering only the Produce path.

`updateInventory`, `cleanupWAL`, `writeLastEntry`, and `FetchPage` have **no retry logic** for transient Azure errors.

**Impact:**

A single 429/503 from Azure during inventory update, WAL cleanup, or page fetch causes immediate failure. Makes the system brittle under load.

**Fix:**

Route all HTTP operations through `retryableRequest`.

**Priority:** Fix before production.

---

## Issue #18: fanoutTransaction Returns Only First Error, Discards Second

**Severity:** Medium  
**Category:** Error Handling  
**Location:** `pkg/storage/azurekit/store.go` — `fanoutTransaction()` (~line 527)

**Problem:**

```go
for err := range errChan {
    if err != nil {
        return err  // Returns first error only
    }
}
```

If both segment and space writes fail, only the first error is returned.

**Fix:**

```go
var errs []error
for err := range errChan {
    if err != nil {
        errs = append(errs, err)
    }
}
if len(errs) > 0 {
    return errors.Join(errs...)
}
```

**Priority:** Fix before production.

---

## Issue #19: calculateTimeBounds Inconsistency Between Pebble and Azure Backends

**Severity:** Medium  
**Category:** Correctness  
**Location:** `pkg/storage/pebblekit/store.go` vs `pkg/storage/azurekit/store.go`

**Problem:**

With `MaxTimestamp == 0` (the common default):

- **Pebble:** `bounds.Max = math.MaxInt64` — includes all future entries
- **Azure:** `bounds.Max = current` — clamped to current timestamp

Identical queries return different results depending on the backend.

**Impact:**

Backend portability broken. Tests pass with one backend but fail with the other.

**Fix:**

Align both to clamp at current timestamp (Azure behavior):

```go
// In pebblekit calculateTimeBounds:
if max == 0 || max > current {
    bounds.Max = current
}
```

**Priority:** Fix before production.

---

## Issue #20: No Server-Side Concurrency Control for Produce Operations

**Severity:** High  
**Category:** Concurrency / Event Sourcing  
**Location:** `pkg/server/node.go`, all Store implementations

**Problem:**

Client-side `produceLocks` only protect `Publish` (single record), not `Produce` (bulk). They are also per-client-instance — multiple client processes produce to the same segment without coordination. The server and store layers have NO locking for concurrent Produce.

**Impact:**

For PebbleDB: silent data overwrite (see Issue #3).
For Azure: first writer succeeds, second may get 409 on segments but succeed on space writes — inconsistent state.

**Fix:**

Add per-segment locking at the storage layer (see Issue #3 for PebbleDB). For Azure, the WAL provides some serialization, but concurrent WAL writes for the same segment are still possible.

**Priority:** Fix before production.

---

## Issue #21: MuxerBidiStream recvChan Never Closed or Drained — Memory Leak

**Severity:** Medium  
**Category:** Memory Leak  
**Location:** `pkg/transport/wskit/bidi_stream.go` — `Close()`

**Problem:**

`recvChan` (buffered to 256) is never closed and never drained on stream close. Messages buffered at close time leak their payloads until GC collects the `MuxerBidiStream`.

**Impact:**

Memory leak proportional to `256 × message size` per closed stream. With frequent stream churn, significant.

**Fix:**

Drain the channel during close.

**Priority:** Fix before production.

---

## Issue #22: WebSocketMuxer shutdown Final Write Without Lock

**Severity:** Medium  
**Category:** Concurrency  
**Location:** `pkg/transport/wskit/muxer.go` — `shutdown()` (~line 790)

**Problem:**

```go
// In shutdown():
_ = m.sendJSON(m.conn, &MuxerMsg{ControlType: ControlTypeClose})
// This write happens WITHOUT m.writeMu lock!
```

The final close message is sent without the write mutex, after the write pump has been stopped. If any goroutine is still in `sendJSONWithLock`, there could be a concurrent websocket write.

**Impact:**

Corrupted WebSocket frames during shutdown.

**Fix:**

```go
m.writeMu.Lock()
_ = m.sendJSON(m.conn, &MuxerMsg{ControlType: ControlTypeClose})
m.writeMu.Unlock()
```

**Priority:** Fix before production.

---

## Issue #23: Subscription Spawns 2 Goroutines Per Message for Timeout

**Severity:** Medium  
**Category:** Performance  
**Location:** `pkg/client/client.go` — subscription handler (~line 700)

**Problem:**

For every subscription message, two goroutines and one channel are created:
1. Goroutine to run the handler
2. Goroutine to bridge `wg.Wait()` to a channel

At 1000 msg/sec, this creates 2000 goroutines/sec and 1000 channels/sec, all needing GC.

**Fix:**

Use a single goroutine with a done channel:

```go
handlerDone := make(chan struct{})
go func() {
    defer close(handlerDone)
    handler(&status)
}()
select {
case <-handlerDone:
case <-handlerCtx.Done():
    activeSub.handlerTimeouts.Add(1)
}
```

**Priority:** Fix before production.

---

## Issue #24: context.WithValue Uses String Key

**Severity:** Low  
**Category:** Best Practices  
**Location:** `pkg/client/client.go` — `ConsumeSpace()`, `ConsumeSegment()`, `Consume()`

**Problem:**

```go
ctx = context.WithValue(ctx, "request_id", requestID)
```

Plain string context keys can collide with keys from other packages.

**Fix:**

```go
type contextKey struct{ name string }
var requestIDKey = contextKey{"request_id"}
ctx = context.WithValue(ctx, requestIDKey, requestID)
```

**Priority:** Technical debt.

---

## Issue #25: Provider reconnect loop creates muxer with context.Background()

**Severity:** Medium  
**Category:** Resource Leak  
**Location:** `pkg/transport/wskit/provider.go` — `startReconnectLoop()` (~line 250)

**Problem:**

```go
m2 := p.newClientMuxer(context.Background(), NewClientMuxerSession(), conn)
```

Muxers created during reconnection use `context.Background()`, which means their goroutines (readLoop, heartbeat, writePump) can never be cancelled via context ancestry. They only stop when the websocket fails or `shutdown()` is called explicitly.

**Impact:**

If the provider is not properly closed via `Close()`, these muxer goroutines leak permanently.

**Fix:**

Use the reconnect context:

```go
m2 := p.newClientMuxer(p.reconnectCtx, NewClientMuxerSession(), conn)
```

**Priority:** Fix before production.

---

## Summary Table

| # | Severity | Category | Issue | Priority |
|---|----------|----------|-------|----------|
| 1 | **Critical** | Data Corruption | WAL recovery fails — store permanently unopenable | Fix NOW |
| 2 | **Critical** | Data Loss | Publish returns success before server confirms | Fix NOW |
| 3 | **Critical** | Data Corruption | Concurrent Produce overwrites data (Pebble) | Fix NOW |
| 4 | **Critical** | Data Corruption | DecodeTransaction swallows entry decode errors | Fix NOW |
| 6 | **Critical** | Crash | BidiStreamEnumerator nil pointer panic | Fix NOW |
| 11 | **Critical** | Security/Crash | Missing return after auth failure in WS server | Fix NOW |
| 5 | **High** | Data Corruption | readMap swallows deserialization errors | Before release |
| 7 | **High** | Data Race | InProcBidiStream.Encode reads closeErr unsynchronized | Before release |
| 8 | **High** | Data Race | pseudoRandState global data race | Before release |
| 9 | **High** | Goroutine Leak | PebbleStore.Close doesn't close cache goroutine | Before release |
| 10 | **High** | Goroutine Leak | DB close goroutine leaked on timeout | Before release |
| 12 | **High** | Goroutine Bomb | WAL monitor infinite restart on persistent panic | Before release |
| 13 | **High** | Goroutine Leak | Handler timeout doesn't stop handler goroutine | Before release |
| 14 | **High** | Data Loss | Batch response per-entity failures not checked | Before release |
| 15 | **High** | Security | signRequest proceeds without auth on token failure | Before release |
| 20 | **High** | Concurrency | No server-side locking for concurrent Produce | Before production |
| 16 | **Medium** | Context | retryableRequest sleeps ignoring context | Before production |
| 17 | **Medium** | Resilience | Storage HTTP methods don't retry on transient errors | Before production |
| 18 | **Medium** | Error Handling | fanoutTransaction returns only first error | Before production |
| 19 | **Medium** | Correctness | Time bounds inconsistency between backends | Before production |
| 21 | **Medium** | Memory Leak | recvChan never drained on close | Before production |
| 22 | **Medium** | Concurrency | Shutdown writes without lock | Before production |
| 23 | **Medium** | Performance | Subscription spawns 2 goroutines per message | Before production |
| 25 | **Medium** | Resource Leak | Reconnect muxer uses context.Background() | Before production |
| 24 | **Low** | Best Practices | context.WithValue uses string key | Post-release |

**Critical items requiring immediate attention:** 6 issues  
**High-priority items for next release:** 10 issues  
**Medium-priority items for production readiness:** 8 issues  
**Low-priority (technical debt):** 1 issue
