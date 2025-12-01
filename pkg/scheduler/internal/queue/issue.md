[Scheduler] backoffQ uses incorrect sorting function, potentially blocking ready-to-retry bindings

**What happened**:
I was reading source code of [scheduling_queue.go](https://github.com/karmada-io/karmada/blob/master/pkg/scheduler/internal/queue/scheduling_queue.go#L139) and noticed that `backoffQ` is initialized used the `Less` function:

```go
backoffQ: heap.NewWithRecorder(BindingKeyFunc, Less, metrics.NewBackoffBindingsRecorder())
```

The `Less` function (defined in [types.go](https://github.com/karmada-io/karmada/blob/master/pkg/scheduler/internal/queue/types.go#L64)) sorts bindings based on *Priority* and *Timestamp*(added to scheduling queue):

```go
func Less(bInfo1, bInfo2 *QueuedBindingInfo) bool {
	p1 := bInfo1.Priority
	p2 := bInfo2.Priority
	return (p1 > p2) || (p1 == p2 && bInfo1.Timestamp.Before(bInfo2.Timestamp))
}
```

However, `backoffQ` is designed to hold bindings that are waiting for a backoff period to expire. Bindings which have completed backoff are poped from `backoffQ` heap and will be moved to `ActiveQ` for scheduling. 
The flushing logic in `flushBackoffQCompleted` relies on the heap property to efficiently find expired bindings: 

```go
func (bq *prioritySchedulingQueue) flushBackoffQCompleted() {
    // ...
    for {
        bInfo, ok := bq.backoffQ.Peek()
        // ...
        // If the head of the heap is still backing off, we assume subsequent elements are too
        // and break the loop.
        if bq.isBindingBackingoff(bInfo) {
            break
        }
        // ...
    }
}
```

**What you expected to happen**:

`backoffQ` should be sorted by Backoff Expiry Time. If `backoffQ` is sorted by priority, a high-priority binding with a long backoff duration could sit at the top of the heap. This would cause `flushBackoffQCompleted` to `break` early, blocking other lower-priority bindings that have already completed their backoff period from being moved to `activeQ`.

**How to reproduce it (as minimally and precisely as possible)**:

**Anything else we need to know?**:

**Environment**:
- Karmada version:
- kubectl-karmada or karmadactl version (the result of `kubectl-karmada version` or `karmadactl version`):
- Others:
