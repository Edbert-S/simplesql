package simplesql/Mysql

import (
	"database/sql"
	"fmt"
	
	"github.com/Edbert-S/simplesql"
)
 
type Snowflake struct {
	Database
	MysqlAuth
}

type MysqlAuth struct {}

func NewMysql() {

}