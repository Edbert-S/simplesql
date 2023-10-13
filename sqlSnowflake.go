package simplesql

import (
	"database/sql"
	"fmt"

	_ "github.com/snowflakedb/gosnowflake"
)

type snowflake struct {
	Database
	SnowflakeAuth
	Schema string
}

type SnowflakeAuth struct {
	Auth
	AuthType          string `default:"external"`
	Org               string
	SnowflakeUsername string
}

func Snowflake(db_name string, schema string, auth SnowflakeAuth) (snowflake, error) {
	s := snowflake{}
	s.Schema = schema
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
		return fmt.Sprintf("%v:%v@%v-%v/%v/%v?authenticator=externalbrowser", s.Auth.Username, s.Auth.Password, s.Org, s.SnowflakeUsername, s.Database.name, s.Schema), nil
	}
	return "", fmt.Errorf("Auth type not added")
}
