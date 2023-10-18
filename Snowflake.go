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
	AuthType          string
	Org               string
	SnowflakeUsername string
}

func NewSnowflakeConnection(auth SnowflakeAuth) (snowflake, error) {

	s := snowflake{
		SnowflakeAuth: auth,
	}

	db, err := s.connect()
	if err != nil {
		return s, err
	}
	s.Database.DB = db

	return s, nil
}

func (s snowflake) connect() (*sql.DB, error) {
	url := s.UrlBuilder()
	fmt.Print(url)
	db, err := sql.Open("snowflake", url)
	if err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func (s snowflake) CloseDB() error {
	err := s.Database.CloseDB()
	return err
}

func (s snowflake) Query(query string) (*sql.Rows, error) {
	return s.Database.Query(query)
}

func (s snowflake) UrlBuilder() string {
	return fmt.Sprintf("%v:%v@%v-%v?authenticator=%v",
		s.Username,
		s.Password,
		s.Org,
		s.SnowflakeUsername,
		s.AuthType,
	)
}
