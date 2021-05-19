package engine

func Open(dirname string, opts *Options) (db *DB, err error) {
	db = &DB{
		Dir:  dirname,
		Opts: opts,
	}
	return db, err
}
