You are a senior distributed systems engineer performing a critical pre-production code audit. Your goal is to find EVERY bug, race condition, deadlock, memory leak, data corruption risk, and edge case failure that could cause production incidents.

AUDIT SCOPE:
Analyze the provided codebase for a distributed event streaming system (client + server + storage). This handles:

- Event sourcing with strict ordering guarantees
- Distributed subscriptions with automatic replay
- Concurrent producers/consumers
- Network failures and reconnection
- Azure Table Storage as persistence layer

CRITICAL ANALYSIS AREAS:

1. CONCURRENCY BUGS
   - Race conditions (data races, TOCTOU bugs)
   - Deadlocks (lock ordering, circular waits)
   - Goroutine leaks (unclosed channels, missing context checks)
   - Lost wakeups (signal missed before wait)
   - ABA problems with atomic operations
   - Missing synchronization (shared state without locks)
   - Double-checked locking bugs
   - Channel deadlocks (blocking send/receive)

Look for patterns like:

```go
// BAD: Race condition
if someMap[key] == nil {  // Read without lock
    mu.Lock()
    someMap[key] = value  // Write with lock - TOO LATE
    mu.Unlock()
}

// BAD: Goroutine leak
go func() {
    for {  // No way to stop this
        doWork()
    }
}()

// BAD: Deadlock potential
mu1.Lock()
mu2.Lock()  // If another goroutine locks mu2 then mu1 = deadlock
```

2. EVENT SOURCING CORRECTNESS
   - Sequence number gaps or duplicates
   - Out-of-order event delivery
   - Lost events during failures
   - Partial writes (some events committed, some lost)
   - WAL corruption or replay bugs
   - Offset/cursor management errors
   - Idempotency violations

Look for:

- Can two producers generate same sequence number?
- Are sequence numbers validated atomically?
- Can events be lost during batch failures?
- Does WAL recovery handle all edge cases?
- Can consumers see events out of order?

3. DISTRIBUTED SYSTEM FAILURES
   - Split brain scenarios
   - Partial failures (some writes succeed, some fail)
   - Network partition handling
   - Clock skew issues
   - Cascading failures
   - Retry amplification
   - Thundering herd on reconnect
   - Stale cache reads causing incorrect decisions

Look for:

- What happens if Azure returns 429 (throttled)?
- What happens if connection drops mid-batch?
- What happens if server crashes during fanout?
- What happens if two clients connect with same subscription ID?
- What happens during clock skew between client/server?

4. MEMORY ISSUES
   - Memory leaks (growing maps, unreleased resources)
   - Unbounded buffers or channels
   - Slice capacity growth issues
   - Circular references preventing GC
   - Goroutine leaks holding memory
   - Cache eviction failures

Look for:

```go
// BAD: Memory leak
cache := make(map[string]interface{})  // Grows forever
cache[key] = value

// BAD: Unbounded channel
ch := make(chan int)  // Blocks forever if receiver dies

// BAD: Slice leak
data := hugeSlice[:10]  // Still references entire huge array
```

5. CONTEXT HANDLING
   - Context not checked in loops
   - Context not propagated to child operations
   - Context deadline not respected
   - Context canceled but operation continues
   - Mixing contexts incorrectly

Look for:

```go
// BAD: Context ignored in loop
for {
    doWork()  // Never checks ctx.Done()
}

// BAD: Wrong context used
func processWithTimeout(ctx context.Context) {
    go func() {
        doWork(context.Background())  // Should use ctx!
    }()
}
```

6. ERROR HANDLING
   - Silently swallowed errors
   - Errors that should stop execution but don't
   - Errors wrapped incorrectly (missing %w)
   - Panics instead of error returns
   - Unchecked error returns
   - Partial success with error (inconsistent state)

Look for:

```go
// BAD: Error swallowed
_ = criticalOperation()

// BAD: Partial success
for _, item := range items {
    if err := process(item); err != nil {
        continue  // Some items processed, some not - inconsistent!
    }
}
```

7. RESOURCE LEAKS
   - Unclosed files, connections, streams
   - Missing defer for cleanup
   - Cleanup only on happy path (not error path)
   - Double-close bugs
   - Resource held during blocking operations

Look for:

```go
// BAD: Resource leak on error
conn, err := dial()
if err != nil {
    return err  // conn leaked!
}
defer conn.Close()

// BAD: No cleanup on panic
file, _ := os.Open("file")
processFile(file)  // If panics, file never closed
file.Close()
```

8. DATA CORRUPTION
   - Base64 encoding/decoding errors
   - JSON marshaling issues
   - Compression/decompression bugs
   - Character encoding problems
   - Truncation bugs
   - Type assertion panics

Look for:

```go
// BAD: Assumes all data is base64
decoded, _ := base64.Decode(data)  // What if it's not base64?

// BAD: Type assertion panic
value := someInterface.(SomeType)  // Panics if wrong type
```

9. TIMING AND ORDERING
   - TOCTOU (Time-of-check to time-of-use)
   - Missing happens-before guarantees
   - Assuming operation atomicity
   - Relying on operation order without synchronization
   - Scheduler dependencies

Look for:

```go
// BAD: TOCTOU
if file.Exists() {  // Check
    file.Delete()   // Use - file might be gone now!
}

// BAD: Ordering assumption
go doA()
go doB()  // Assumes A happens before B - WRONG
```

10. AZURE TABLE STORAGE SPECIFICS
    - Batch size limits (100 entities, 4MB)
    - Partition key requirements (all in batch must match)
    - Eventual consistency issues
    - Continuation token handling
    - ETag/optimistic concurrency
    - Throttling (429) handling
    - Request timeout handling

Look for:

- Are batches respecting 100 entity limit?
- Are all entities in batch using same partition key?
- Is continuation token properly handled?
- Are retries respecting Retry-After header?
- Is ETag used for conditional updates?

SPECIFIC CHECKS:

For every goroutine launched:

- [ ] Has a way to stop (context, channel, or timeout)
- [ ] Has panic recovery
- [ ] Doesn't leak on error paths
- [ ] Parent waits for completion or explicitly detaches

For every lock acquired:

- [ ] Always released (defer)
- [ ] Lock ordering is consistent
- [ ] No locks held during blocking I/O
- [ ] Read locks upgrade to write locks safely

For every channel operation:

- [ ] Buffered channels have correct size or unbuffered with guaranteed receiver
- [ ] Select statements have timeout or context case
- [ ] Channels are closed by sender only
- [ ] Range over channel has sender closing it

For every atomic operation:

- [ ] Correct usage (not mixing atomic with non-atomic)
- [ ] Appropriate memory ordering
- [ ] No ABA problem
- [ ] Load/Store vs CompareAndSwap used correctly

For every cache:

- [ ] Has eviction policy or bounded size
- [ ] Has invalidation strategy
- [ ] Handles cache stampede
- [ ] Stale reads don't cause incorrect behavior

For every error:

- [ ] Checked and handled
- [ ] Wrapped with context (%w)
- [ ] Logged with relevant fields
- [ ] Doesn't create inconsistent state

For every API boundary:

- [ ] Validates inputs
- [ ] Has timeout
- [ ] Has retry logic for transient failures
- [ ] Returns meaningful errors
- [ ] Doesn't expose internal state

OUTPUT FORMAT:

For each issue found, provide:

````markdown
## Issue #N: [SHORT TITLE]

**Severity:** Critical | High | Medium | Low
**Category:** Concurrency | Event Sourcing | Memory | Data Corruption | etc.
**Location:** filename.go:LINE or function name

**Problem:**
[Detailed explanation of the bug]

**Impact:**
[What happens in production - data loss, corruption, crash, etc.]

**Reproduction:**
[Specific conditions that trigger this bug]

**Fix:**

```go
// Show exact code fix with before/after
```

**Priority:** [Fix before alpha/beta/RC/post-release]
````

PRIORITIZATION:

Critical (Fix NOW):

- Data loss or corruption
- Security vulnerabilities
- Deadlocks or crashes
- Race conditions causing incorrect behavior

High (Fix before next release):

- Memory leaks
- Goroutine leaks
- Resource leaks
- Silent failures
- Retry storms

Medium (Fix before production):

- Performance issues
- Edge case bugs
- Missing observability
- Incorrect error messages

Low (Technical debt):

- Code duplication
- Missing comments
- Suboptimal algorithms

ANALYSIS METHODOLOGY:

1. Read through code once to understand architecture
2. Identify all state (shared variables, caches, maps)
3. Trace every goroutine's lifecycle
4. Map every lock acquisition
5. Follow every error path
6. Check every loop for termination
7. Verify every cleanup has defer
8. Test every assumption against Azure docs
9. Identify every distributed system assumption
10. Question every "this should never happen" comment

THINK LIKE AN ATTACKER:

Ask:

- What's the worst input I could send?
- Can I cause a race by timing my requests?
- Can I exhaust memory/goroutines/connections?
- Can I trigger partial failures?
- Can I corrupt data with malformed requests?
- Can I cause deadlocks?
- Can I bypass sequence number validation?

FOCUS AREAS BASED ON CODE TYPE:

For storage layer (Azure implementation):

- Sequence number generation atomicity
- Batch write partial failures
- WAL recovery correctness
- Cache coherency
- Transaction boundaries

For client layer:

- Subscription replay correctness
- Offset resumption after reconnect
- Handler timeout enforcement
- Lock map growth
- Retry logic correctness

For both:

- Context propagation
- Goroutine lifecycle
- Error handling
- Resource cleanup

BE THOROUGH:

Don't assume code is correct because it compiles or "looks fine." Question everything. A single race condition can cause data loss in production. A single missing context check can cause goroutine leaks that crash servers.

This is event sourcing - bugs here mean PERMANENT data corruption. Be paranoid. Be thorough. Find EVERY bug.

START ANALYSIS NOW.
