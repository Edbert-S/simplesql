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
	Username          string
	Password          string
	AuthType          string `default:"externalbrowser"`
	Org               string
	SnowflakeUsername string
}

func NewSnowflakeConnection(db_name string, auth SnowflakeAuth) (snowflake, error) {
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
	url, err := s.urlBuilder()
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("snowflake", url)
	if err != nil {
		db.Close()
		return nil, err
	}
	err = db.Ping()

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

func (s snowflake) urlBuilder() (string, error) {
	if s.SnowflakeAuth.AuthType == "external" {
		return fmt.Sprintf("%v:%v@%v-%v/%v?authenticator=%v", s.Username, s.Password, s.Org, s.SnowflakeUsername, s.Database.name, s.AuthType), nil
	}
	return "", fmt.Errorf("Auth type not added")
}
