# AOE Prototype

## Metadata

### Create a new memtable
```go
snapshot := CacheHolder.GetSnapshot()
segment := snapshot.GetUnClosedSegment()
if segment == nil {
    // No unclosed segment, create a new segment first
    ...
}
ctx := NewOperationContext(snapshot)
newblkop := NewCreateBlockOperation(&ctx)
blk, err := newblkop.CommitNewBlock()
if err != nil {
    return err
}
newblkop.Push()
err = newblkop.WaitDone()
if err != nil {
    return err
}
snapshot = CacheHolder.GetSnapshot()
blk, err = snapshot.GetUnClosedBlock()
if err != nil {
    return err
}

memtable := NewMemTable(blk)
memtable.Append(...)
...
```

### Create a new segment
```go
snapshot := CacheHolder.GetSnapshot()
ctx := NewOperationContext(snapshot)
newsegop := NewCreateSegmentOperation(&ctx)
seg, err := newsegop.CommitNewSegment()
if err != nil {
    return err
}
newsegop.Push()
err = newblkop.WaitDone()
if err != nil {
    return err
}
snapshot = CacheHolder.GetSnapshot()
```

### Mutable To Immutable
```go
// 1. Create a new memtable
// 2. Put the current memtable into flush queue
// 3. A worker apply update block op (? Maybe apply update block in main thread is better)
snapshot := CacheHolder.GetSnapshot()
ctx := NewOperationContext(snapshot)
updateblkop := NewUpdateBlockOperation(&ctx)
seg, err := updateblkop.CommitNewSegment()
if err != nil {
    return err
}
newsegop.Push()
err = newblkop.WaitDone()
if err != nil {
    return err
}
```

### Flush
```go
// 1. The flush worker take a memtable | segment from flush queue
// 2. Get the latest snapshot and check whether to flush or not. (maybe dropped already)
// 3. Serialize the data
// 4. Serialize the metadata into a tmp file
// 5. Create a flush operation and pass the tmp file in op context
// 6. The metadata execute worker check the consistency. If it is ok, rename the tmp file to correct name
// 7. Refresh the latest cache
snapshot := CacheHolder.GetSnapshot()
ctx := NewOperationContext(snapshot)
ctx.Block = memTable.Block // Here take flush memtable as example
flushop := NewFlushOperation(&ctx)
flushop.Push()
err = flushop.WaitDone()
if err != nil {
    return err
}
```
