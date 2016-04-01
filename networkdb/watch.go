package networkdb

import "github.com/docker/go-events"

type opType uint8

const (
	opCreate opType = 1 + iota
	opUpdate
	opDelete
)

type event struct {
	Table     string
	NetworkID string
	Key       string
	Value     []byte
}

type CreateEvent event
type UpdateEvent event
type DeleteEvent event

func (nDB *NetworkDB) Watch(tname, nid, key string) (chan events.Event, func()) {
	var matcher events.Matcher

	if tname != "" || nid != "" || key != "" {
		matcher = events.MatcherFunc(func(ev events.Event) bool {
			var evt event
			switch ev := ev.(type) {
			case CreateEvent:
				evt = event(ev)
			case UpdateEvent:
				evt = event(ev)
			case DeleteEvent:
				evt = event(ev)
			}

			if tname != "" && evt.Table != tname {
				return false
			}

			if nid != "" && evt.NetworkID != nid {
				return false
			}

			if key != "" && evt.Key != key {
				return false
			}

			return true
		})
	}

	ch := events.NewChannel(0)
	sink := events.Sink(events.NewQueue(ch))

	if matcher != nil {
		sink = events.NewFilter(sink, matcher)
	}

	nDB.broadcaster.Add(sink)
	return ch.C, func() {
		nDB.broadcaster.Remove(sink)
		ch.Close()
		sink.Close()
	}
}

func makeEvent(op opType, tname, nid, key string, value []byte) events.Event {
	ev := event{
		Table:     tname,
		NetworkID: nid,
		Key:       key,
		Value:     value,
	}

	switch op {
	case opCreate:
		return CreateEvent(ev)
	case opUpdate:
		return UpdateEvent(ev)
	case opDelete:
		return DeleteEvent(ev)
	}

	return nil
}
