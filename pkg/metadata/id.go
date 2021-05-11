package metadata

import "fmt"

func (id *ID) IncIteration() {
	id.Iter += 1
}

func (id *ID) IncID() {
	id.ID += 1
	id.Iter = 0
}

func (id *ID) String() string {
	return fmt.Sprintf("ID(%d,%d)", id.ID, id.Iter)
}
