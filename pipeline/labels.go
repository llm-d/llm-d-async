package pipeline

// Labels is the framework's first-class label set. Subscriptions carry a
// static Labels map declared in TopicConfig; per-message Labels are seeded by
// the Flow at pull time from a merge of the channel's static labels and the
// transport's per-message kv (e.g. GCP Pub/Sub Attributes, Redis request
// Metadata), with subscription labels winning on key collision so producers
// cannot override operator-pinned labels.
//
// After the initial pull-time merge, gates and the merge policy may freely
// mutate Labels in place; the last write wins. Gates are operator-chosen code,
// so the framework does not protect any subset of keys from gate writes —
// operators are responsible for the gates they load.
//
// The set of well-known label keys is policy-defined: upstream itself has no
// opinion on which keys exist, only that the map is the agreed transport.
type Labels map[string]string

// Get returns the value at key, or the empty string if the key is absent.
// Safe to call on a nil Labels.
func (l Labels) Get(key string) string {
	return l[key]
}

// Has reports whether the key is present (even with an empty value).
// Safe to call on a nil Labels.
func (l Labels) Has(key string) bool {
	_, ok := l[key]
	return ok
}

// Set assigns value to key. Panics if l is nil.
func (l Labels) Set(key, value string) {
	l[key] = value
}

// Merge writes every entry from src into l, overwriting on key collision.
// No-op if src is empty. Panics if l is nil and src is non-empty.
func (l Labels) Merge(src Labels) {
	for k, v := range src {
		l[k] = v
	}
}

// Clone returns a shallow copy of l. Returns nil if l is nil.
func (l Labels) Clone() Labels {
	if l == nil {
		return nil
	}
	out := make(Labels, len(l))
	for k, v := range l {
		out[k] = v
	}
	return out
}
