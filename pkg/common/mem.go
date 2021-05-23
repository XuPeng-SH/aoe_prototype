package common

type IAllocator interface {
	Malloc() (buf []byte, err error)
}
