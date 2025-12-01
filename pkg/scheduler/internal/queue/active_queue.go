/*
Copyright 2025 The Karmada Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package queue

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/karmada-io/karmada/pkg/scheduler/internal/heap"
	metrics "github.com/karmada-io/karmada/pkg/scheduler/metrics/queue"
)

// ActiveQueue defines the interface of activeQ related operations.
type ActiveQueue interface {
	Push(bindingInfo *QueuedBindingInfo)
	Pop() (*QueuedBindingInfo, bool)
	Len() int
	Done(bindingInfo *QueuedBindingInfo)
	Has(key string) bool
	ShutDown()
}

// NewActiveQueue builds a instance of ActiveQueue.
func NewActiveQueue(metricRecorder metrics.MetricRecorder) ActiveQueue {
	q := &activequeue{
		activeBindings:     heap.NewWithRecorder[*QueuedBindingInfo](BindingKeyFunc, Less, metricRecorder),
		dirtyBindings:      sets.Set[string]{},
		processingBindings: sets.Set[string]{},
		cond:               sync.NewCond(&sync.Mutex{}),
	}

	return q
}

/*
activeBindings (Heap):

角色: 真正的“队列”（实际上是堆）。
作用: 存放当前可以被消费者立即取走的任务。只有在这里面的元素，才能被Pop()出来。
特性: 有序（按优先级）。

dirtyBindings (Set):

角色: “待处理”集合。
作用: 标记所有需要被处理的元素。
特性: 只要一个元素被 Push进来，它就一定在 dirty 集合中，直到它被 Pop 出去开始处理。
它的主要作用是去重——如果一个元素已经在 dirty 集中，再次 Push 它是无效的（或者说是被折叠了）。

processingBindings (Set):
角色: “正在处理”集合。
作用: 标记所有正在被消费者处理（已被 Pop 但未 Done ）的元素。
特性: 防止同一个元素被多个消费者并发处理（虽然目前 Karmada 调度器是单 worker，但这个设计是并发安全的）。
更重要的是，它配合 dirty 集合实现了“处理中更新”的逻辑。
*/

// activequeue is a priority work queue, which implements a ActiveQueue.
type activequeue struct {
	// activeBindings defines the order in which we will work on items. Every
	// element of queue should be in the dirtyBindings set and not in the
	// processing set.
	activeBindings *heap.Heap[*QueuedBindingInfo]

	// dirtyBindings defines all of the items that need to be processed.
	dirtyBindings sets.Set[string] // 待处理状态的元素

	// Things that are currently being processed are in the processingBindings set.
	// These things may be simultaneously in the dirtyBindings set. When we finish
	// processingBindings something and remove it from this set, we'll check if
	// it's in the dirtyBindings set, and if so, add it to the queue.
	processingBindings sets.Set[string]

	cond *sync.Cond

	shuttingDown bool
}

// Push marks item as needing processing.
func (q *activequeue) Push(bindingInfo *QueuedBindingInfo) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if q.shuttingDown {
		return
	}
	// 如果dirtyBindings中已经存在，说明该bindingInfo已经被Push过，直接返回
	if q.dirtyBindings.Has(bindingInfo.NamespacedKey) {
		return
	}

	now := time.Now()
	bindingInfo.Timestamp = now
	if bindingInfo.InitialAttemptTimestamp == nil {
		bindingInfo.InitialAttemptTimestamp = &now
	}
	q.dirtyBindings.Insert(bindingInfo.NamespacedKey)
	if q.processingBindings.Has(bindingInfo.NamespacedKey) { // S1:如果正在被处理，不加到activeBindings避免被并发处理
		return
	}

	// 如果没有正在被处理，就将该对象加入到堆中（表示ready被处理），等待被处理
	q.activeBindings.AddOrUpdate(bindingInfo)
	q.cond.Signal()
}

// Len returns the current queue length, for informational purposes only. You
// shouldn't e.g. gate a call to Push() or Pop() on Len() being a particular
// value, that can't be synchronized properly.
func (q *activequeue) Len() int {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	return q.activeBindings.Len()
}

// Pop blocks until it can return an item to be processed. If shutdown = true,
// the caller should end their goroutine. You must call Done with item when you
// have finished processing it.
func (q *activequeue) Pop() (bindingInfo *QueuedBindingInfo, shutdown bool) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	for q.activeBindings.Len() == 0 && !q.shuttingDown {
		q.cond.Wait()
	}
	if q.activeBindings.Len() == 0 {
		// We must be shutting down.
		return nil, true
	}

	bindingInfo, _ = q.activeBindings.Pop()
	bindingInfo.Attempts++
	q.processingBindings.Insert(bindingInfo.NamespacedKey)
	q.dirtyBindings.Delete(bindingInfo.NamespacedKey) // binding正在被处理，从dirtyBindings中删除

	return bindingInfo, false
}

// Done marks item as done processing, and if it has been marked as dirty again
// while it was being processed, it will be re-added to the queue for
// re-processing.
func (q *activequeue) Done(bindingInfo *QueuedBindingInfo) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	q.processingBindings.Delete(bindingInfo.NamespacedKey) // 如果处理完了，就从processingBindings中删除
	if q.dirtyBindings.Has(bindingInfo.NamespacedKey) {    // S1 callback: 如果在被处理期间又有相同的元素被加入到这个优先级队列中，
		// 由于在加入时该元素已经在processingBindings中，所以并没有被加入到activeBindings中
		// 这里就需要把它重新加入到activeBindings中，等待被处理
		bindingInfo.Timestamp = time.Now()
		q.activeBindings.AddOrUpdate(bindingInfo)
		q.cond.Signal()
	} else if q.processingBindings.Len() == 0 {
		q.cond.Signal()
	}
}

// ShutDown will cause q to ignore all new items added to it and
// immediately instruct the worker goroutines to exit.
func (q *activequeue) ShutDown() {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	q.shuttingDown = true
	q.cond.Broadcast()
}

// Has inform if bindingInfo exists in the queue.
func (q *activequeue) Has(key string) bool {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	return q.dirtyBindings.Has(key)
}
