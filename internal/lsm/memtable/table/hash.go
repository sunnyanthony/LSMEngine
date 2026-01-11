package table

import "hash/fnv"

func hashKey(key []byte) uint64 {
	hasher := fnv.New64a()
	_, _ = hasher.Write(key)
	return hasher.Sum64()
}
