package it

type BaseIterator struct {
	Err       error
	Result    interface{}
	Executor  ExecutorT
	Resources IResources
}

var (
	_ Iterator = (*BaseIterator)(nil)
)

func (iter *BaseIterator) SetErr(err error) {
	iter.Err = err
}

func (iter *BaseIterator) GetErr() error {
	return iter.Err
}

func (iter *BaseIterator) SetResult(r interface{}) {
	iter.Result = r
}

func (iter *BaseIterator) GetResult() interface{} {
	return iter.Result
}

func (iter *BaseIterator) PreIter() error {
	return nil
}

func (iter *BaseIterator) Iter() {
	iter.Resources.IterResource(iter)
}

func (iter *BaseIterator) PostIter() error {
	return nil
}

func (iter *BaseIterator) Execute(res interface{}) error {
	if iter.Executor != nil {
		return iter.Executor(res, iter)
	}
	return nil
}
