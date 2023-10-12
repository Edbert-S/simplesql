package simplesql

import (
	"database/sql"
	"fmt"
)

type DB interface {
	//methods
	Database()
	Query() sql.Rows
	CloseDB() error
}

type Database struct {
	name string
	db   *sql.DB
}

func (d Database) Connect() (*sql.DB, error) {
	return nil, fmt.Errorf("Can't use Abstract functions directly")
}

func (d *Database) Query(query string) (rows *sql.Rows, err error) {
	prepedQuery, err := d.db.Prepare(query)
	if err != nil {
		return nil, err
	}

	r, err := prepedQuery.Query()
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (d *Database) CloseDB() error {
	err := d.db.Close()
	if err != nil {
		return err
	}
	return nil
}
