package md2

import (
	"errors"
	"fmt"
	// log "github.com/sirupsen/logrus"
)

func NewTable(ids ...uint64) *Table {
	var id uint64
	if len(ids) == 0 {
		id = Meta.Sequence.GetTableID()
	} else {
		id = ids[0]
	}
	t := &Table{
		ID:         id,
		Partitions: make(map[uint64]*Partition),
		TimeStamp:  *NewTimeStamp(),
	}
	return t
}

func (t *Table) GetID() uint64 {
	return t.ID
}

func (t *Table) ReferencePartition(partition_id uint64) (p *Partition, err error) {
	t.RLock()
	defer t.RUnlock()
	p, ok := t.Partitions[partition_id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("specified partition %d not found in table %d", partition_id, t.ID))
	}
	return p, nil
}

func (t *Table) PartitionIDs(args ...int64) map[uint64]uint64 {
	var ts int64
	if len(args) == 0 {
		ts = NowMicro()
	} else {
		ts = args[0]
	}
	ids := make(map[uint64]uint64)
	t.RLock()
	defer t.RUnlock()
	for _, p := range t.Partitions {
		if !p.Select(ts) {
			continue
		}
		ids[p.GetID()] = p.GetID()
	}
	return ids
}

func (t *Table) CreatePartition() (p *Partition, err error) {
	p = NewPartition(t.ID, Meta.Sequence.GetPartitionID())
	return p, err
}

func (t *Table) String() string {
	s := fmt.Sprintf("Tbl(%d)", t.ID)
	s += "["
	for i, p := range t.Partitions {
		if i != 0 {
			s += "\n"
		}
		s += p.String()
	}
	if len(t.Partitions) > 0 {
		s += "\n"
	}
	s += "]"
	return s
}

func (t *Table) RegisterPartition(p *Partition) error {
	if t.ID != p.TableID {
		return errors.New(fmt.Sprintf("table id mismatch %d:%d", t.ID, p.TableID))
	}
	t.Lock()
	defer t.Unlock()

	err := p.Attach()
	if err != nil {
		return err
	}

	_, ok := t.Partitions[p.ID]
	if ok {
		return errors.New(fmt.Sprintf("Duplicate partition %d found in table %d", p.ID, t.ID))
	}
	t.Partitions[p.ID] = p
	return nil
}
