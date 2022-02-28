package shardmap

import (
	"github.com/goinbox/crypto"
	"github.com/goinbox/gomisc"

	"strconv"
	"sync"
	"testing"
)

var shardMap *ShardMap
var syncMap *sync.Map
var simpleMap map[string]int64

func init() {
	shardMap = New(32)
	syncMap = new(sync.Map)
	simpleMap = make(map[string]int64)
}

func TestSetGet(t *testing.T) {
	for i := 0; i < 10000; i++ {
		key := getIntMd5(i)
		shardMap.Set(key, i)

		v, ok := shardMap.Get(key)
		if !ok || v != i {
			t.Error(v, ok)
		}
	}
}

func TestWalkDel(t *testing.T) {
	shardMap.Walk(func(k string, v interface{}) {
		// t.log(k, v)

		shardMap.Del(k)

		_, ok := shardMap.Get(k)
		if ok {
			t.Error(v, ok)
		}
	})
}

func BenchmarkShardMapWrite(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ki := gomisc.RandByTime(nil)
			k := strconv.FormatInt(ki, 10)
			shardMap.Set(k, ki)
		}
	})
}

func getIntMd5(i int) string {
	return crypto.Md5String([]byte(strconv.Itoa(i)))
}

func BenchmarkSyncMapWrite(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ki := gomisc.RandByTime(nil)
			k := strconv.FormatInt(ki, 10)
			syncMap.Store(k, ki)
		}
	})
}

func BenchmarkSimpleMapWrite(b *testing.B) {
	lock := new(sync.Mutex)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ki := gomisc.RandByTime(nil)
			k := strconv.FormatInt(ki, 10)
			lock.Lock()
			simpleMap[k] = ki
			lock.Unlock()
		}
	})
}
