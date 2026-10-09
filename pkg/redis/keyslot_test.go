package redis

import "testing"

func TestCRC16_MatchesRedisClusterChecksum(t *testing.T) {
	// CRC-16/XMODEM check value, and two slots quoted in the Redis Cluster
	// specification (CLUSTER KEYSLOT foo / bar).
	if got := crc16([]byte("123456789")); got != 0x31C3 {
		t.Fatalf("crc16(123456789) = %#x, want 0x31c3", got)
	}
	for key, want := range map[string]int{"foo": 12182, "bar": 5061, "{foo}:claimed": 12182, "x{foo}y": 12182} {
		if got := keySlot(key); got != want {
			t.Errorf("keySlot(%q) = %d, want %d", key, got, want)
		}
	}
}

func TestHashTag(t *testing.T) {
	cases := map[string]string{
		"plain":            "plain",
		"{tag}rest":        "tag",
		"pre{tag}":         "tag",
		"{}empty":          "{}empty",
		"{open":            "{open",
		"a{first}{second}": "first",
		"a}b{c}":           "c",
	}
	for key, want := range cases {
		if got := hashTag(key); got != want {
			t.Errorf("hashTag(%q) = %q, want %q", key, got, want)
		}
	}
}

func TestSlotKey_StandaloneKeepsNamesAndClusterTagsOnce(t *testing.T) {
	if got := slotKey("request-sortedset", false); got != "request-sortedset" {
		t.Fatalf("standalone slotKey = %q, want the key unchanged", got)
	}
	if got := slotKey("request-sortedset", true); got != "{request-sortedset}" {
		t.Fatalf("cluster slotKey = %q, want {request-sortedset}", got)
	}
	// A key that already carries a tag must not be wrapped again: the outer
	// braces would change the tag and move the derived keys to another slot.
	if got := slotKey("{pool-a}:requests", true); got != "{pool-a}:requests" {
		t.Fatalf("tagged slotKey = %q, want unchanged", got)
	}
}

func TestNewClaimKeys_ClusterLayoutSharesPendingSlot(t *testing.T) {
	for _, queue := range []string{"request-sortedset", "llm-d-async:requests:pool-a", "{pool-a}:requests", "a{b"} {
		standalone := newClaimKeys(queue, false)
		if standalone.claimed != queue+":claimed" || standalone.owners != queue+":claim-owners" || standalone.idx != queue+":claims-idx" {
			t.Errorf("standalone layout for %q changed: %+v", queue, standalone)
		}
		cluster := newClaimKeys(queue, true)
		if cluster.pending != queue {
			t.Errorf("cluster layout renamed the shared pending key for %q: %q", queue, cluster.pending)
		}
		for _, k := range []string{cluster.claimed, cluster.owners, cluster.idx} {
			if !sameHashSlot(k, queue) {
				t.Errorf("queue %q: private key %q is in slot %d, pending is in slot %d", queue, k, keySlot(k), keySlot(queue))
			}
		}
	}
}
