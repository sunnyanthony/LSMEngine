package flow

import (
	"bytes"
	"context"
	"io"
	"sort"
	"sync"

	"lsmengine/internal/lsm/sstable/block"
	"lsmengine/internal/lsm/sstable/bloom"
	"lsmengine/internal/lsm/sstable/cache"
	"lsmengine/internal/lsm/sstable/config"
	"lsmengine/internal/lsm/sstable/format"
	"lsmengine/internal/lsm/sstable/index"
	"lsmengine/internal/lsm/sstable/storage"
	"lsmengine/pkg/lsm/errs"
)

// Pipeline centralizes index/filter lookup, data fetch, and prefetch so
// reader/writer stay small and act like controllers.
type Pipeline struct {
	source     storage.BlockSource
	nodes      *nodeRegistry
	indexer    *indexNode
	filterer   *filterNode
	dataNode   *dataNode
	decodeNode *decodeNode
	prefetcher *prefetchNode
	cache      *cache.BlockCache
	size       int64
	observer   FlowObserver
	policy     config.PolicySnapshot
}

func NewPipeline(source storage.BlockSource, cache *cache.BlockCache, indexCache *cache.IndexCache, filterCache *cache.FilterCache, entries []index.Entry, indexPartitioned bool, filter *bloom.Filter, filterIndex []index.Entry, filterPartitioned bool, size int64, policy config.PolicySnapshot) *Pipeline {
	nodes := newNodeRegistry()
	observer := FlowObserver(nopObserver{})
	// decode: locate the entry within a data block.
	decode := &decodeNode{observer: observer}
	// data: fetch data blocks (with cache) before decoding.
	data := newDataNode(source, cache, nodes, policy, size, decode, observer)
	// index: map user key -> data block descriptor (partitioned or top index).
	indexer := newIndexNode(entries, indexPartitioned, source, indexCache, nodes, data, observer)
	// filter: optional bloom filter gate before index lookup.
	filterer := newFilterNode(filter, filterIndex, filterPartitioned, source, filterCache, nodes, indexer, observer)
	var prefetcher *prefetchNode
	if policy.UsePrefetch {
		// prefetch: optional lookahead for range scans.
		prefetcher = newPrefetchNode(policy, source, cache, nodes)
	}
	p := &Pipeline{
		source:     source,
		nodes:      nodes,
		indexer:    indexer,
		filterer:   filterer,
		dataNode:   data,
		decodeNode: decode,
		prefetcher: prefetcher,
		cache:      cache,
		size:       size,
		observer:   observer,
		policy:     policy,
	}
	if p.prefetcher != nil && policy.PrefetchAsync {
		p.prefetcher.Start()
	}
	return p
}

func (p *Pipeline) WithObserver(obs FlowObserver) *Pipeline {
	if obs == nil {
		p.observer = nopObserver{}
		return p
	}
	p.observer = obs
	if p.decodeNode != nil {
		p.decodeNode.observer = obs
	}
	if p.dataNode != nil {
		p.dataNode.obs = obs
	}
	if p.indexer != nil {
		p.indexer.obs = obs
	}
	if p.filterer != nil {
		p.filterer.obs = obs
	}
	return p
}

func (p *Pipeline) NewRangePlan(start []byte) *IndexRangePlan {
	if p == nil || p.indexer == nil {
		return &IndexRangePlan{}
	}
	return p.indexer.NewRange(start)
}

func (p *Pipeline) NewPrefetchBudget() *PrefetchBudget {
	if p == nil {
		return nil
	}
	return NewPrefetchBudget(p.policy)
}

func (p *Pipeline) StopPrefetcher() {
	if p == nil || p.prefetcher == nil {
		return
	}
	p.prefetcher.Stop()
}

func (p *Pipeline) startNode() Node {
	if p == nil {
		return nil
	}
	if p.filterer != nil {
		return p.filterer
	}
	return p.indexer
}

// get runs the DAG for a single key and returns the decoded entry view.
func (p *Pipeline) Get(ctx context.Context, key []byte) (block.EntryView, bool, error) {
	if p == nil {
		return block.EntryView{}, false, nil
	}
	item := &FlowItem{Key: key}
	node := p.startNode()
	for node != nil {
		res := node.Process(ctx, item)
		if res.Err != nil {
			return block.EntryView{}, false, res.Err
		}
		if res.Done || item.Done {
			break
		}
		node = res.Next
	}
	if item.Found {
		return item.Entry, true, nil
	}
	return block.EntryView{}, false, nil
}

func (p *Pipeline) ReadBlockEntry(entry index.Entry) (*block.Block, error) {
	if err := p.dataNode.validateEntry(entry, errs.ErrSSTableBadBlock); err != nil {
		return nil, err
	}
	blk, _, err := p.dataNode.fetch(context.Background(), entry)
	return blk, err
}

func (p *Pipeline) Prefetch(entries []index.Entry, start int, budget *PrefetchBudget) {
	if p == nil || p.prefetcher == nil || !p.policy.UsePrefetch {
		return
	}
	p.prefetcher.Prefetch(entries, start, budget)
}

// --- DAG nodes ---

type dataNode struct {
	source storage.BlockSource
	cache  *cache.BlockCache
	nodes  *nodeRegistry
	policy config.PolicySnapshot
	size   int64
	next   Node
	obs    FlowObserver
}

func newDataNode(source storage.BlockSource, cache *cache.BlockCache, nodes *nodeRegistry, policy config.PolicySnapshot, size int64, next Node, obs FlowObserver) *dataNode {
	return &dataNode{
		source: source,
		cache:  cache,
		nodes:  nodes,
		policy: policy,
		size:   size,
		next:   next,
		obs:    obs,
	}
}

func (n *dataNode) Process(ctx context.Context, item *FlowItem) NodeResult {
	if err := n.validateEntry(item.Index, errs.ErrSSTableBadBlock); err != nil {
		return NodeResult{Err: err}
	}
	blk, cacheHit, err := n.fetch(ctx, item.Index)
	if err != nil {
		if n.obs != nil {
			n.obs.OnError(FlowEvent{Key: item.Key, Node: "data", Err: err}, "data", err)
		}
		return NodeResult{Err: err}
	}
	item.Block = blk
	if n.obs != nil {
		n.obs.OnNode(FlowEvent{
			Key:      item.Key,
			Node:     "data",
			CacheHit: cacheHit,
			Mmapped:  n.source != nil && n.source.Mmapped(),
		}, "data")
	}
	return NodeResult{Next: n.next}
}

func (n *dataNode) validateEntry(entry index.Entry, errBad error) error {
	if entry.Length == 0 {
		return errBad
	}
	if n.policy.ReadBlockMaxBytes > 0 && int(entry.Length) > n.policy.ReadBlockMaxBytes {
		return errBad
	}
	if n.size > 0 && int64(entry.Offset)+int64(entry.Length) > n.size {
		return errBad
	}
	return nil
}

func (n *dataNode) fetch(ctx context.Context, entry index.Entry) (*block.Block, bool, error) {
	desc := storage.BlockDescriptor{
		ID:     entry.Offset,
		Type:   format.BlockTypeData,
		Offset: entry.Offset,
		Length: entry.Length,
		Key:    entry.Key,
	}
	node := n.nodes.dataNode(desc, n.source, n.cache)
	return node.Fetch(ctx)
}

type decodeNode struct {
	observer FlowObserver
}

func (n *decodeNode) Process(_ context.Context, item *FlowItem) NodeResult {
	if item.Block == nil {
		return NodeResult{Done: true}
	}
	entry, ok, err := item.Block.FindView(item.Key)
	if err != nil {
		if n.observer != nil {
			n.observer.OnError(FlowEvent{Key: item.Key, Node: "decode", Err: err}, "decode", err)
		}
		return NodeResult{Err: err}
	}
	item.Entry = entry
	item.Found = ok
	item.Done = true
	if n != nil && n.observer != nil {
		n.observer.OnNode(FlowEvent{Key: item.Key, Node: "decode"}, "decode")
	}
	return NodeResult{Done: true}
}

// --- Nodes and registries ---

type dataBlockNode struct {
	desc   storage.BlockDescriptor
	source storage.BlockSource
	cache  *cache.BlockCache
}

func newDataBlockNode(desc storage.BlockDescriptor, source storage.BlockSource, cache *cache.BlockCache) *dataBlockNode {
	return &dataBlockNode{
		desc:   desc,
		source: source,
		cache:  cache,
	}
}

func (n *dataBlockNode) Fetch(ctx context.Context) (*block.Block, bool, error) {
	if n.cache != nil {
		if blk, ok := n.cache.Get(n.desc.Offset); ok {
			return blk, true, nil
		}
	}
	view, err := n.source.Read(ctx, n.desc, storage.ReadHint{Pin: true})
	if err != nil {
		if err == io.EOF {
			return nil, false, errs.ErrSSTableBadBlock
		}
		return nil, false, err
	}
	if view.Release != nil {
		defer view.Release()
	}
	payload, err := format.DecodeBlockPayload(view.Data, format.BlockTypeData, errs.ErrSSTableBadBlock)
	if err != nil {
		return nil, false, err
	}
	blk, err := block.Decode(payload)
	if err != nil {
		return nil, false, err
	}
	if n.cache != nil {
		n.cache.Add(n.desc.Offset, blk)
	}
	return blk, false, nil
}

type indexBlockNode struct {
	desc   storage.BlockDescriptor
	source storage.BlockSource
	cache  *cache.IndexCache
}

func newIndexBlockNode(desc storage.BlockDescriptor, source storage.BlockSource, cache *cache.IndexCache) *indexBlockNode {
	return &indexBlockNode{
		desc:   desc,
		source: source,
		cache:  cache,
	}
}

func (n *indexBlockNode) Fetch(ctx context.Context) ([]index.Entry, error) {
	if n.cache != nil {
		if entries, ok := n.cache.Get(n.desc.Offset); ok {
			return entries, nil
		}
	}
	view, err := n.source.Read(ctx, n.desc, storage.ReadHint{})
	if err != nil {
		if err == io.EOF {
			return nil, errs.ErrSSTableBadIndex
		}
		return nil, err
	}
	if view.Release != nil {
		defer view.Release()
	}
	payload, err := format.DecodeBlockPayload(view.Data, format.BlockTypeIndex, errs.ErrSSTableBadIndex)
	if err != nil {
		return nil, err
	}
	entries, err := index.Decode(payload)
	if err != nil {
		return nil, err
	}
	if n.cache != nil {
		n.cache.Add(n.desc.Offset, entries)
	}
	return entries, nil
}

type filterBlockNode struct {
	desc   storage.BlockDescriptor
	source storage.BlockSource
	cache  *cache.FilterCache
}

func newFilterBlockNode(desc storage.BlockDescriptor, source storage.BlockSource, cache *cache.FilterCache) *filterBlockNode {
	return &filterBlockNode{
		desc:   desc,
		source: source,
		cache:  cache,
	}
}

func (n *filterBlockNode) Fetch(ctx context.Context) (*bloom.Filter, error) {
	if n.cache != nil {
		if filter, ok := n.cache.Get(n.desc.Offset); ok {
			return filter, nil
		}
	}
	view, err := n.source.Read(ctx, n.desc, storage.ReadHint{})
	if err != nil {
		if err == io.EOF {
			return nil, errs.ErrSSTableBadMeta
		}
		return nil, err
	}
	if view.Release != nil {
		defer view.Release()
	}
	payload, err := format.DecodeBlockPayload(view.Data, format.BlockTypeFilter, errs.ErrSSTableBadMeta)
	if err != nil {
		return nil, err
	}
	filter := bloom.Decode(payload)
	if filter == nil {
		return nil, errs.ErrSSTableBadMeta
	}
	if n.cache != nil {
		n.cache.Add(n.desc.Offset, filter)
	}
	return filter, nil
}

type nodeRegistry struct {
	mu     sync.Mutex
	data   map[uint64]*dataBlockNode
	index  map[uint64]*indexBlockNode
	filter map[uint64]*filterBlockNode
}

func newNodeRegistry() *nodeRegistry {
	return &nodeRegistry{
		data:   make(map[uint64]*dataBlockNode),
		index:  make(map[uint64]*indexBlockNode),
		filter: make(map[uint64]*filterBlockNode),
	}
}

func (r *nodeRegistry) dataNode(desc storage.BlockDescriptor, source storage.BlockSource, cache *cache.BlockCache) *dataBlockNode {
	r.mu.Lock()
	defer r.mu.Unlock()
	if node := r.data[desc.Offset]; node != nil && node.desc.Length == desc.Length {
		return node
	}
	node := newDataBlockNode(desc, source, cache)
	r.data[desc.Offset] = node
	return node
}

func (r *nodeRegistry) indexNode(desc storage.BlockDescriptor, source storage.BlockSource, cache *cache.IndexCache) *indexBlockNode {
	r.mu.Lock()
	defer r.mu.Unlock()
	if node := r.index[desc.Offset]; node != nil && node.desc.Length == desc.Length {
		return node
	}
	node := newIndexBlockNode(desc, source, cache)
	r.index[desc.Offset] = node
	return node
}

func (r *nodeRegistry) filterNode(desc storage.BlockDescriptor, source storage.BlockSource, cache *cache.FilterCache) *filterBlockNode {
	r.mu.Lock()
	defer r.mu.Unlock()
	if node := r.filter[desc.Offset]; node != nil && node.desc.Length == desc.Length {
		return node
	}
	node := newFilterBlockNode(desc, source, cache)
	r.filter[desc.Offset] = node
	return node
}

// --- Index ---

type indexNode struct {
	top         []index.Entry
	partitioned bool
	source      storage.BlockSource
	cache       *cache.IndexCache
	nodes       *nodeRegistry
	next        Node
	obs         FlowObserver
}

func newIndexNode(top []index.Entry, partitioned bool, source storage.BlockSource, cache *cache.IndexCache, nodes *nodeRegistry, next Node, obs FlowObserver) *indexNode {
	return &indexNode{
		top:         top,
		partitioned: partitioned,
		source:      source,
		cache:       cache,
		nodes:       nodes,
		next:        next,
		obs:         obs,
	}
}

func (n *indexNode) Process(ctx context.Context, item *FlowItem) NodeResult {
	if n == nil {
		return NodeResult{Done: true}
	}
	entry, ok, err := n.Find(ctx, item.Key)
	if err != nil {
		return NodeResult{Err: err}
	}
	if !ok {
		item.Done = true
		return NodeResult{Done: true}
	}
	item.Index = entry
	if n.obs != nil {
		n.obs.OnNode(FlowEvent{Key: item.Key, Node: "index"}, "index")
	}
	return NodeResult{Next: n.next}
}

func (n *indexNode) Find(ctx context.Context, key []byte) (index.Entry, bool, error) {
	if n == nil {
		return index.Entry{}, false, nil
	}
	if !n.partitioned {
		idx := findBlock(n.top, key)
		if idx < 0 {
			return index.Entry{}, false, nil
		}
		return n.top[idx], true, nil
	}
	partIdx := findBlock(n.top, key)
	if partIdx < 0 {
		return index.Entry{}, false, nil
	}
	part, err := n.readPartition(ctx, n.top[partIdx])
	if err != nil {
		return index.Entry{}, false, err
	}
	idx := findBlock(part, key)
	if idx < 0 {
		return index.Entry{}, false, nil
	}
	return part[idx], true, nil
}

func (n *indexNode) NewRange(start []byte) *IndexRangePlan {
	if n == nil {
		return &IndexRangePlan{}
	}
	return &IndexRangePlan{
		node:  n,
		start: start,
	}
}

func (n *indexNode) readPartition(ctx context.Context, entry index.Entry) ([]index.Entry, error) {
	desc := storage.BlockDescriptor{
		ID:     entry.Offset,
		Type:   format.BlockTypeIndex,
		Offset: entry.Offset,
		Length: entry.Length,
		Key:    entry.Key,
	}
	node := n.nodes.indexNode(desc, n.source, n.cache)
	return node.Fetch(ctx)
}

type IndexRangePlan struct {
	node      *indexNode
	start     []byte
	topIndexI int
	done      bool
	started   bool
}

func (p *IndexRangePlan) Next(ctx context.Context) ([]index.Entry, error) {
	if p == nil || p.node == nil || p.done {
		return nil, io.EOF
	}
	if !p.started {
		p.started = true
		if p.node.partitioned && len(p.start) > 0 {
			idx := findBlock(p.node.top, p.start)
			if idx < 0 {
				idx = 0
			}
			p.topIndexI = idx
		}
	}
	if !p.node.partitioned {
		p.done = true
		if len(p.node.top) == 0 {
			return nil, io.EOF
		}
		return p.node.top, nil
	}
	for p.topIndexI < len(p.node.top) {
		entry := p.node.top[p.topIndexI]
		p.topIndexI++
		part, err := p.node.readPartition(ctx, entry)
		if err != nil {
			return nil, err
		}
		if len(part) == 0 {
			continue
		}
		return part, nil
	}
	p.done = true
	return nil, io.EOF
}

// --- Filter ---

type filterNode struct {
	filter      *bloom.Filter
	filterIndex []index.Entry
	partitioned bool
	source      storage.BlockSource
	cache       *cache.FilterCache
	nodes       *nodeRegistry
	next        Node
	obs         FlowObserver
}

func newFilterNode(filter *bloom.Filter, filterIndex []index.Entry, partitioned bool, source storage.BlockSource, cache *cache.FilterCache, nodes *nodeRegistry, next Node, obs FlowObserver) *filterNode {
	return &filterNode{
		filter:      filter,
		filterIndex: filterIndex,
		partitioned: partitioned,
		source:      source,
		cache:       cache,
		nodes:       nodes,
		next:        next,
		obs:         obs,
	}
}

func (n *filterNode) Process(ctx context.Context, item *FlowItem) NodeResult {
	ok, err := n.MayContain(ctx, item.Key)
	if err != nil {
		return NodeResult{Err: err}
	}
	if !ok {
		item.Done = true
		return NodeResult{Done: true}
	}
	if n.obs != nil {
		n.obs.OnNode(FlowEvent{Key: item.Key, Node: "filter"}, "filter")
	}
	return NodeResult{Next: n.next}
}

func (n *filterNode) MayContain(ctx context.Context, key []byte) (bool, error) {
	if n == nil {
		return true, nil
	}
	if !n.partitioned {
		if n.filter == nil {
			return true, nil
		}
		return n.filter.MayContain(key), nil
	}
	if len(n.filterIndex) == 0 {
		return true, nil
	}
	idx := findBlock(n.filterIndex, key)
	if idx < 0 {
		return true, nil
	}
	entry := n.filterIndex[idx]
	desc := storage.BlockDescriptor{
		ID:     entry.Offset,
		Type:   format.BlockTypeFilter,
		Offset: entry.Offset,
		Length: entry.Length,
		Key:    entry.Key,
	}
	node := n.nodes.filterNode(desc, n.source, n.cache)
	filter, err := node.Fetch(ctx)
	if err != nil {
		return true, err
	}
	return filter.MayContain(key), nil
}

// --- Prefetch ---

type prefetchNode struct {
	policy config.PolicySnapshot
	source storage.BlockSource
	cache  *cache.BlockCache
	nodes  *nodeRegistry

	mu sync.Mutex
	ch chan index.Entry
	wg sync.WaitGroup
}

func newPrefetchNode(policy config.PolicySnapshot, source storage.BlockSource, cache *cache.BlockCache, nodes *nodeRegistry) *prefetchNode {
	if cache == nil {
		return nil
	}
	return &prefetchNode{
		policy: policy,
		source: source,
		cache:  cache,
		nodes:  nodes,
	}
}

func (p *prefetchNode) Start() {
	if p == nil || !p.policy.PrefetchAsync {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ch != nil {
		return
	}
	p.ch = make(chan index.Entry, p.policy.PrefetchQueueDepth)
	workers := p.policy.PrefetchWorkers
	if workers <= 0 {
		workers = 1
	}
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go func(ch <-chan index.Entry) {
			defer p.wg.Done()
			for entry := range ch {
				p.prefetchEntry(entry)
			}
		}(p.ch)
	}
}

func (p *prefetchNode) Stop() {
	if p == nil {
		return
	}
	p.mu.Lock()
	ch := p.ch
	p.ch = nil
	p.mu.Unlock()
	if ch == nil {
		return
	}
	close(ch)
	p.wg.Wait()
}

func (p *prefetchNode) Prefetch(entries []index.Entry, start int, budget *PrefetchBudget) {
	if p == nil || p.cache == nil {
		return
	}
	targets := p.prefetchTargets(entries, start, budget)
	if len(targets) == 0 {
		return
	}
	if p.policy.PrefetchAsync {
		for _, entry := range targets {
			p.enqueue(entry)
		}
		return
	}
	for _, entry := range targets {
		p.prefetchEntry(entry)
	}
}

func (p *prefetchNode) enqueue(entry index.Entry) {
	p.mu.Lock()
	ch := p.ch
	p.mu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- entry:
	default:
	}
}

func (p *prefetchNode) prefetchEntry(entry index.Entry) {
	if entry.Length == 0 {
		return
	}
	if _, ok := p.cache.Get(entry.Offset); ok {
		return
	}
	desc := storage.BlockDescriptor{
		ID:     entry.Offset,
		Type:   format.BlockTypeData,
		Offset: entry.Offset,
		Length: entry.Length,
		Key:    entry.Key,
	}
	node := p.nodes.dataNode(desc, p.source, p.cache)
	_, _, _ = node.Fetch(context.Background())
}

func (p *prefetchNode) prefetchTargets(entries []index.Entry, start int, budget *PrefetchBudget) []index.Entry {
	if len(entries) == 0 {
		return nil
	}
	if budget == nil {
		return nil
	}
	out := make([]index.Entry, 0, budget.blocks)
	remainingBytes := budget.bytes
	for i := 1; ; i++ {
		idx := start + i
		if idx >= len(entries) {
			break
		}
		entry := entries[idx]
		if !p.prefetchEntryCandidate(entry) {
			continue
		}
		if budget.blocks > 0 && len(out) >= budget.blocks {
			break
		}
		if budget.bytes > 0 {
			if remainingBytes <= 0 && len(out) > 0 {
				break
			}
			if int(entry.Length) > remainingBytes && len(out) > 0 {
				break
			}
			remainingBytes -= int(entry.Length)
		}
		out = append(out, entry)
	}
	return out
}

func (p *prefetchNode) prefetchTargetsBudget(entries []index.Entry, start int, budget *PrefetchBudget) []index.Entry {
	if budget.bytes <= 0 && budget.blocks <= 0 {
		return nil
	}
	if budget.bytes > 0 {
		remaining := budget.bytes
		prefetched := 0
		var out []index.Entry
		for idx := start + 1; idx < len(entries); idx++ {
			entry := entries[idx]
			if !p.prefetchEntryCandidate(entry) {
				continue
			}
			if remaining <= 0 && prefetched > 0 {
				break
			}
			if int(entry.Length) > remaining && prefetched > 0 {
				break
			}
			out = append(out, entry)
			remaining -= int(entry.Length)
			prefetched++
		}
		budget.bytes = remaining
		return out
	}
	out := make([]index.Entry, 0, budget.blocks)
	for i := 1; i <= budget.blocks; i++ {
		idx := start + i
		if idx >= len(entries) {
			break
		}
		entry := entries[idx]
		if !p.prefetchEntryCandidate(entry) {
			continue
		}
		out = append(out, entry)
	}
	if budget.blocks > 0 {
		budget.blocks -= len(out)
		if budget.blocks < 0 {
			budget.blocks = 0
		}
	}
	return out
}

func (p *prefetchNode) prefetchEntryCandidate(entry index.Entry) bool {
	if entry.Length == 0 {
		return false
	}
	if p.policy.ReadBlockMaxBytes > 0 && int(entry.Length) > p.policy.ReadBlockMaxBytes {
		return false
	}
	if _, ok := p.cache.Get(entry.Offset); ok {
		return false
	}
	return true
}

func findBlock(entries []index.Entry, key []byte) int {
	if len(entries) == 0 {
		return -1
	}
	if bytes.Compare(key, entries[0].Key) < 0 {
		return -1
	}
	i := sort.Search(len(entries), func(i int) bool {
		return bytes.Compare(entries[i].Key, key) > 0
	})
	if i == 0 {
		return -1
	}
	idx := i - 1
	if bytes.Equal(entries[idx].Key, key) {
		for idx > 0 && bytes.Equal(entries[idx-1].Key, key) {
			idx--
		}
	}
	return idx
}
