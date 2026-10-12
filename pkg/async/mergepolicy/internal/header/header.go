// Package header sets HTTP headers on the per-message header map the merge
// policies hand to the worker.
package header

import "strings"

// Set stores value under name, first dropping every key that names the same
// header in another casing. HTTP header names are case-insensitive and the
// worker applies the map with net/http's Header.Set, which canonicalizes the
// name, so two spellings of one header would collapse onto one wire header
// with whichever key Go's map iteration visited last. Dropping the variants
// makes the precedence the policies intend (caller over stamped defaults,
// later stamps over the caller) hold regardless of casing.
func Set(headers map[string]string, name, value string) {
	for k := range headers {
		if k != name && strings.EqualFold(k, name) {
			delete(headers, k)
		}
	}
	headers[name] = value
}
