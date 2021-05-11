package metadata

import (
	"errors"
)

func (st *State) Commit() error {
	if st.Type == PENDING {
		st.Type = COMMITTED
	} else if st.Type == DROPPENDING {
		st.Type = DROPCOMMITTED
	} else {
		return errors.New("Cannot commit already committed resources")
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
