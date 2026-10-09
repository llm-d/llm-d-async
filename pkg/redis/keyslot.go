package redis

import "strings"

// Redis Cluster places a key in slot CRC16(key) mod 16384, where "key" is
// the hash tag (the text inside the first {...} with non-empty content) when
// the key has one. Multi-key scripts are legal only when every key shares a
// slot, so the private bookkeeping keys below are derived to share the slot
// of the shared key they belong to.

const hashSlots = 16384

// hashTag returns the slot-determining part of key: its hash tag when it has
// a non-empty one, otherwise the whole key.
func hashTag(key string) string {
	start := strings.IndexByte(key, '{')
	if start < 0 {
		return key
	}
	end := strings.IndexByte(key[start+1:], '}')
	if end <= 0 {
		return key
	}
	return key[start+1 : start+1+end]
}

// slotKey returns the form of a shared key to embed in the names of its
// private bookkeeping keys so they hash to the same slot. Outside cluster
// mode it is the key itself, preserving the names standalone deployments
// already hold. In cluster mode it is wrapped in a hash tag unless it already
// carries one, because "{p}" hashes exactly like the bare key "p"; the shared
// key itself keeps its name.
func slotKey(primary string, cluster bool) string {
	if !cluster || hashTag(primary) != primary {
		return primary
	}
	return "{" + primary + "}"
}

// keySlot returns the cluster hash slot of key.
func keySlot(key string) int {
	return int(crc16([]byte(hashTag(key)))) % hashSlots
}

// sameHashSlot reports whether one script may address both keys in cluster mode.
func sameHashSlot(a, b string) bool {
	return keySlot(a) == keySlot(b)
}

// crc16 is CRC-16/XMODEM (poly 0x1021, init 0), the checksum Redis Cluster
// uses for slot assignment.
func crc16(data []byte) uint16 {
	var crc uint16
	for _, b := range data {
		crc ^= uint16(b) << 8
		for range 8 {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}
