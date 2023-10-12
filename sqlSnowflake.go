package simplesql

import (
	"database/sql"
	"fmt"

	_ "github.com/snowflakedb/gosnowflake"
)

type Snowflake struct {
	Database
	SnowflakeAuth
}

type SnowflakeAuth struct {
	authType  string
	username  string
	password  string
	org       string
	connected bool
}

func (s Snowflake) new(db_name string, auth SnowflakeAuth) error {
	s.SnowflakeAuth = auth
	db, err := s.connect()
	if err != nil {
		return err
	}
	s.Database.new(db_name, db)
	return nil
}

func (s Snowflake) connect() (*sql.DB, error) {
	auth := s.SnowflakeAuth
	if auth.authType == "okta" {
		db, err := s.okta_connect()
		return db, err
	} else {
		return nil, fmt.Errorf("Invalid authType")
	}
}

func (s Snowflake) okta_connect() (*sql.DB, error) {
	a := s.SnowflakeAuth
	url := fmt.Sprintf("https://%v.okta.com@%v-%v/%v", a.username, a.org, a.org, s.name)
	db, err := sql.Open("snowflake", url)

	if err != nil {
		return nil, err
	}

	return db, nil
}

func (s Snowflake) CloseDB() error {
	err := s.CloseDB()
	return err
}

func (s Snowflake) Query(query string) (sql.Rows, error) {
	return s.Query(query)
}
