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
	s := snowflake{}
	db, err := s.connect()
	s.Database.DB = db
	if err != nil {
		return s, err
	}

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
	err := s.CloseDB()
	return err
}

func (s snowflake) Query(query string) (sql.Rows, error) {
	return s.Query(query)
}

func (s snowflake) UrlBuilder() string {
	return fmt.Sprintf("%v:%v@%v-%v?authenticator=%v",
		s.Username,
		s.Password,
		s.Org,
		s.SnowflakeUsername,
		// s.Database.Name,
		s.AuthType,
	)
}
