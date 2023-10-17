package simplesql

import (
	"database/sql"
	"fmt"
)

type DB interface {
	//methods
	Query() sql.Rows
	CloseDB() error
	Ping() error
}

type Database struct {
	DB *sql.DB
}

func (d *Database) Connect() (*sql.DB, error) {
	return nil, fmt.Errorf("Can't use Abstract functions directly")
}

func (d *Database) Query(query string) (rows *sql.Rows, err error) {
	prepedQuery, err := d.DB.Prepare(query)
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
	err := d.DB.Close()
	if err != nil {
		return err
	}
	return nil
}

func (d *Database) Ping() error {
	return d.Ping()
}
