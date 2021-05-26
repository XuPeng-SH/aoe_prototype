package db

import (
	e "aoe/pkg/engine"
)

// type Reader interface {
// }

func Open(dirname string, opts *e.Options) (db *DB, err error) {
	db = &DB{
		Dir:  dirname,
		Opts: opts,
	}
	return db, err
}
