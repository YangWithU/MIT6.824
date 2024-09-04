package kvraft

import (
	"log"
	"sync"
	"time"
)

const maxWaitingTime = 500 * time.Millisecond

type Notifier struct {
	done              sync.Cond
	maxRegisteredOpId int
}

func (kv *KVServer) makeNotifier(op *Op) {
	kv.getNotifier(op, true)
	kv.makeAlarm(op)
}

// wakes g after maxWaitingTime
func (kv *KVServer) makeAlarm(op *Op) {
	go func() {
		<-time.After(maxWaitingTime)
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.notify(op)
	}()
}

func (kv *KVServer) wait(op *Op) {
	log.Printf("S%v waits op(C=%v Id=%v) to apply", kv.me, op.ClerkId, op.OpId)
	for !kv.killed() {
		if notifier := kv.getNotifier(op, false); notifier != nil {
			notifier.done.Wait()
		} else {
			break
		}
	}
}

// getNotifier(op, true) to register a new notifier for op
// there is only one notifier for one clerk
func (kv *KVServer) getNotifier(op *Op, forced bool) *Notifier {
	if notifier, ok := kv.opNotifier[op.ClerkId]; ok {
		notifier.maxRegisteredOpId = max(notifier.maxRegisteredOpId, op.OpId)
		return notifier
	}

	if !forced {
		return nil
	}
	notifier := &Notifier{
		done:              *sync.NewCond(&kv.mu),
		maxRegisteredOpId: op.OpId,
	}
	kv.opNotifier[op.ClerkId] = notifier
	return notifier
}

func (kv *KVServer) notify(op *Op) {
	if notifier := kv.getNotifier(op, false); notifier != nil {
		// OpId是clerk最后
		if op.OpId == notifier.maxRegisteredOpId {
			delete(kv.opNotifier, op.ClerkId)
		}

		notifier.done.Broadcast()

		log.Printf("S%v notifies applied (C=%v Id=%v)", kv.me, op.ClerkId, op.OpId)
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
