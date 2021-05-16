package it

import ()

type ExecutorT func(interface{}, Iterator) error

type IResources interface {
	IterResource(Iterator)
}

type Iterator interface {
	PreIter() error
	Iter()
	PostIter() error
	GetResult() interface{}
	GetErr() error
	SetResult(interface{})
	SetErr(error)
	Execute(interface{}) error
}
