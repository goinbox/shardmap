// ref https://github.com/DeanThompson/syncmap/blob/master/syncmap.go

package shardmap

import (
	"sync"
)

const (
	DefShardCnt = 32

	BkdrSeed = 131 // 31 131 1313 13131 131313 etc...
)

type shardItem struct {
	sync.RWMutex

	data map[string]interface{}
}

type ShardMap struct {
	shardCnt uint8
	shards   []*shardItem
}

/**
* @param uint8, shardCnt must be pow of two
 */
func New(shardCnt uint8) *ShardMap {
	if !isPowOfTwo(shardCnt) {
		shardCnt = DefShardCnt
	}

	s := &ShardMap{
		shardCnt: shardCnt,
		shards:   make([]*shardItem, shardCnt),
	}

	for i, _ := range s.shards {
		s.shards[i] = &shardItem{
			data: make(map[string]interface{}),
		}
	}

	return s
}

func NewDefault() *ShardMap {
	return New(DefShardCnt)
}

func (s *ShardMap) Get(key string) (interface{}, bool) {
	si := s.locate(key)

	si.RLock()
	value, ok := si.data[key]
	si.RUnlock()

	return value, ok
}

func (s *ShardMap) Set(key string, value interface{}) {
	si := s.locate(key)

	si.Lock()
	si.data[key] = value
	si.Unlock()
}

func (s *ShardMap) Del(key string) {
	si := s.locate(key)

	si.Lock()
	delete(si.data, key)
	si.Unlock()
}

type kvItem struct {
	key   string
	value interface{}
}

func (s *ShardMap) Walk(wf func(k string, v interface{})) {
	for _, si := range s.shards {
		kvCh := make(chan *kvItem)

		go func() {
			si.RLock()

			for k, v := range si.data {
				si.RUnlock()
				kvCh <- &kvItem{
					key:   k,
					value: v,
				}
				si.RLock()
			}

			si.RUnlock()
			close(kvCh)
		}()

		for {
			kv, ok := <-kvCh
			if !ok {
				break
			}
			wf(kv.key, kv.value)
		}
	}
}

func (s *ShardMap) locate(key string) *shardItem {
	i := bkdrHash(key) & uint32(s.shardCnt-1)

	return s.shards[i]
}

func isPowOfTwo(x uint8) bool {
	return x != 0 && (x&(x-1) == 0)
}

// https://www.byvoid.com/blog/string-hash-compare/
func bkdrHash(str string) uint32 {
	var h uint32

	for _, c := range str {
		h = h*BkdrSeed + uint32(c)
	}

	return h
}
