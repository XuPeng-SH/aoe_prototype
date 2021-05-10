package metadata

func (id *ID) IncIteration() {
	id.Iter += 1
}

func (id *ID) IncID() {
	id.ID += 1
	id.Iter = 0
}
