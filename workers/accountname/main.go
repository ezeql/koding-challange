package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/ezeql/koding-challange/common"
	_ "github.com/lib/pq"
	"log"
	"time"
)

var (
	rabbitHost         = flag.String("rabbit-host", "127.0.0.1", "RabbitMQ host")
	rabbitPort         = flag.Int("rabbit-port", 5672, "RabbitMQ port")
	rabbitUser         = flag.String("rabbit-user", "guest", "RabbitMQ username")
	rabbitPassword     = flag.String("rabbit-password", "guest", "RabbitMQ password")
	postgreSQLHost     = flag.String("postgresql-host", "127.0.0.1", "PostgreSQL host")
	postgreSQLPort     = flag.Int("postgresql-port", 5432, "PostgreSQL port")
	postgreSQLUser     = flag.String("postgresql-user", "postgres", "PostgreSQL username")
	postgreSQLPassword = flag.String("postgresql-password", "", "PostgreSQL password")
	postgreSQLDB       = flag.String("postgresql-db", "", "PostgreSQL DB name")
	debugLevel         = flag.Bool("loglevel", false, "debug level (currently bool)")
)

type DB struct {
	*sql.DB
}

func main() {
	flag.Parse()

	db, err := openDB(*postgreSQLHost, *postgreSQLPort, *postgreSQLUser, *postgreSQLPassword, *postgreSQLDB)
	if err != nil {
		log.Fatalln("cannot open connection to DB:", err)
	}
	defer db.Close()

	if err := db.createTable(); err != nil {
		log.Fatalln("table:", err)
	}

	c, err := common.BuildRabbitMQConnector(*rabbitHost, *rabbitPort, *rabbitUser, *rabbitPassword)
	if err != nil {
		log.Fatalln("cannot connect to rabbitmq", err)
	}
	defer c.Close()

	err = c.Handle("account-name", func(b []byte) {
		d := common.MustUnmarshallFromJSON(b)
		if err := db.insertEntry(d.Username, *d.Time); err != nil {

		}
	})
	if err != nil {
		log.Fatalln("error connecting to Rabbit")
	}

	select {}
}

func openDB(host string, port int, user string, password string, dbName string) (*DB, error) {
	dbinfo := fmt.Sprintf("host=%s port=%v user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbName)

	conn, err := sql.Open("postgres", dbinfo)
	if err != nil {
		return nil, err
	}
	if err = conn.Ping(); err != nil {
		return nil, err
	}
	db := &DB{conn}
	return db, nil
}

func (db *DB) createTable() error {
	schema := `CREATE TABLE IF NOT EXISTS users_first_play (
				"username" 	varchar(30) PRIMARY KEY,
				"first_on" 	timestamp with time zone NOT NULL);`
	_, err := db.Exec(schema)
	return err
}

//ON CONFLICT (upsert) was introduced in pgsql 9.5
func (db *DB) insertEntry(username string, t time.Time) error {
	sql := `INSERT INTO users_first_play(username,first_on) 
			VALUES ( $1, $2 ) 
			ON CONFLICT(username) DO NOTHING;`
	_, err := db.Exec(sql, username, t)
	return err
}
