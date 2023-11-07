package simplesql/Mysql

import (
	"database/sql"
	"fmt"
	
	"github.com/Edbert-S/simplesql"
)
 
type MySql struct {
	Database
	MysqlAuth
}

type MysqlAuth struct {
	Username string
	Password string
	Name string
	Port int
	Host string
	Protocol string
}

func NewMySqlConnection() {

}