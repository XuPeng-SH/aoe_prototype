package vector

import (
	"aoe/pkg/common"
	mock "aoe/pkg/mock/type"
	// log "github.com/sirupsen/logrus"
)

type Vector interface {
	Append(Vector, uint64) (n uint64, err error)
	GetData() []byte
	GetCount() uint64
	// GetAuxData() [][]byte
}

type BaseVector struct {
	Type   mock.ColType
	Data   []byte
	Offset int
}

type StdVector struct {
	BaseVector
}

type StrVector struct {
	BaseVector
	AuxData  [][]byte
	AuxAlloc common.IAllocator
}

func NewStdVector(t mock.ColType, dataBuf []byte) Vector {
	vec := &StdVector{
		BaseVector: BaseVector{
			Type: t,
			Data: dataBuf,
		},
	}
	return vec
}

func (v *StdVector) GetCount() uint64 {
	return uint64(v.Offset) / v.Type.Size()
}

func (v *StdVector) GetData() []byte {
	return v.Data
}

func (v *StdVector) Append(o Vector, offset uint64) (n uint64, err error) {
	buf := o.GetData()
	tsize := int(v.Type.Size())
	remaining := cap(v.Data) - v.Offset
	other_remaining := len(buf) - tsize*int(offset)
	to_write := other_remaining
	if other_remaining > remaining {
		to_write = remaining
	}
	start := int(offset) * tsize
	end := int(offset)*tsize + to_write
	v.Data = append(v.Data[v.Offset:], buf[start:end]...)
	v.Offset += to_write
	return uint64(to_write / tsize), nil
}

// func NewStrVector(t mock.ColType, dataBuf []byte, auxBuf []byte, alloc common.IAllocator) Vector {
// 	vec := &StrVector{
// 		BaseVector: BaseVector{
// 			Type: t,
// 			Data: dataBuf,
// 		},
// 		AuxData:  make([][]byte, 0),
// 		AuxAlloc: alloc,
// 	}
// 	vec.AuxData = append(vec.AuxData, auxBuf)
// 	return vec
// }

// func (v *StrVector) GetData() []byte {
// 	return v.Data
// }

// func (v *StrVector) Append(o Vector, offset uint64) (n uint64, err error) {
// 	buf := o.GetData()
// 	remaining := cap(v.Data) - len(v.Data)
// 	other_remaining := len(buf) - int(offset)
// 	to_write := other_remaining
// 	if other_remaining > remaining {
// 		to_write = remaining
// 	}
// 	return uint64(to_write), nil
// }
