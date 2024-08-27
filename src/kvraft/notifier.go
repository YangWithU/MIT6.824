package kvraft

import "sync"

type Notifier struct {
	done sync.Cond
}

func (kv *KVServer) getNotifier(op *Op, forced bool) *Notifier {
	if _, ok := kv.opNotifier[op.ClerkId]; ok {
		return kv.opNotifier[op.ClerkId]
	}
	if !forced {
		return nil
	}
	notifier := &Notifier{
		done: *sync.NewCond(&kv.mu),
	}
	kv.opNotifier[op.ClerkId] = notifier
	return notifier
}

func (kv *KVServer) notify(op *Op) {
	if notifier := kv.getNotifier(op, false); notifier != nil {
		notifier.done.Broadcast()
		delete(kv.opNotifier, op.ClerkId)
	}
}
