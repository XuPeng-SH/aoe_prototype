package metadata

import (
// "errors"
)

func (st *State) Commit() error {
	if st.Type == PENDING {
		st.Type = COMMITTED
	} else if st.Type == DROPPENDING {
		st.Type = DROPCOMMITTED
	} else {
		// return errors.New("Cannot commit already committed resources")
	}

	return nil
}

func (st *State) String() string {
	switch st.Type {
	case PENDING:
		return "P"
	case DROPPENDING:
		return "DP"
	case COMMITTED:
		return "C"
	case DROPCOMMITTED:
		return "DC"
	}
	return "Invalid state"
}

func ToString(val interface{}) string {
	switch v := val.(type) {
	case DataState:
		switch v {
		case EMPTY:
			return "EMPTY"
		case PARTIAL:
			return "PARTIAL"
		case FULL:
			return "FULL"
		case SORTED:
			return "SORTED"
		}
	}
	panic("logic error")
}
