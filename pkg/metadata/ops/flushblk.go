package ops

type FlushBlockOperation struct {
	Operation
}

func (op *FlushBlockOperation) execute() error {
	return nil
}
