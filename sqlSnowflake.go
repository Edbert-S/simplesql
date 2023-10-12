package simplesql

import (
	"database/sql"
	"fmt"

	_ "github.com/snowflakedb/gosnowflake"
)

type snowflake struct {
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

func Snowflake(db_name string, auth SnowflakeAuth) (snowflake, error) {
	s := snowflake{}
	db, err := s.connect()
	s.Database.db = db
	s.Database.name = db_name
	if err != nil {
		return s, err
	}

	return s, nil
}

func (s snowflake) connect() (*sql.DB, error) {
	auth := s.SnowflakeAuth
	if auth.authType == "okta" {
		db, err := s.okta_connect()
		return db, err
	} else {
		return nil, fmt.Errorf("Invalid authType")
	}
}

func (s snowflake) okta_connect() (*sql.DB, error) {
	a := s.SnowflakeAuth
	url := fmt.Sprintf("https://%v.okta.com@%v-%v/%v", a.username, a.org, a.org, s.name)
	db, err := sql.Open("snowflake", url)

	if err != nil {
		return nil, err
	}

	return db, nil
}

func (s snowflake) CloseDB() error {
	err := s.CloseDB()
	return err
}

func (s snowflake) Query(query string) (sql.Rows, error) {
	return s.Query(query)
}
