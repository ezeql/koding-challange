package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/ezeql/koding-challange/common"
	_ "github.com/lib/pq"
	"log"
	"net/http"
	"time"
)

var (
	rabbitHost         = flag.String("rabbit-host", "127.0.0.1", "RabbitMQ host")
	rabbitPort         = flag.Int("rabbit-port", 5672, "RabbitMQ port")
	rabbitUser         = flag.String("rabbit-user", "guest", "RabbitMQ username")
	rabbitPassword     = flag.String("rabbit-password", "guest", "RabbitMQ password")
	rabbitExchange     = flag.String("rabbit-exchange", "logs", "RabbitMQ exchange name")
	postgreSQLHost     = flag.String("postgresql-host", "127.0.0.1", "PostgreSQL host")
	postgreSQLPort     = flag.Int("postgresql-port", 5432, "PostgreSQL port")
	postgreSQLUser     = flag.String("postgresql-user", "postgres", "PostgreSQL username")
	postgreSQLPassword = flag.String("postgresql-password", "", "PostgreSQL password")
	postgreSQLDB       = flag.String("postgresql-db", "", "PostgreSQL DB name")
	debugMode          = flag.Bool("loglevel", false, "debug mode")
	metricsPort        = flag.Int("metrics-port", 33333, "expvar stats port")
)

type pgdb struct {
	*sql.DB
}

func main() {
	flag.Parse()
	common.DebugLevel = *debugMode
	common.Info("AccountName Worker")
	common.Info(`collects all the account names that sent metrics, with their 
				first occurrence datetime (UTC) into PostgreSQL.`)
	common.Info("connecting to PostgreSQL...")

	db, err := openDB(*postgreSQLHost, *postgreSQLPort, *postgreSQLUser, *postgreSQLPassword, *postgreSQLDB)
	if err != nil {
		log.Fatalln("cannot open connection to DB:", err)
	}
	defer db.Close()

	common.Info("Connected")

	if err := db.createTable(); err != nil {
		log.Fatalln("table:", err)
	}

	common.Info("connecting to RabbitMQ...")
	c, err := common.BuildRabbitMQConnector(*rabbitHost, *rabbitPort, *rabbitUser, *rabbitPassword, *rabbitExchange)
	if err != nil {
		log.Fatalln("cannot connect to rabbitmq", err)
	}
	common.Info("connected")
	defer c.Close()

	common.Info("Starting worker proccesor")
	err = c.Handle("account-name", func(b []byte) bool {
		d := common.MustUnmarshallFromJSON(b)
		if err := db.insertEntry(d.Username, d.Time); err != nil {
			log.Println("error inserting in pgsql", err)
			return false //requeue
		}
		return true
	})
	if err != nil {
		log.Fatalln("error connecting to Rabbit", err)
	}

	common.Info("Starting a metrics http server")

	bindTo := fmt.Sprintf(":%v", *metricsPort)
	http.ListenAndServe(bindTo, nil)
}

func openDB(host string, port int, user string, password string, dbName string) (*pgdb, error) {
	dbinfo := fmt.Sprintf("host=%s port=%v user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbName)

	conn, err := sql.Open("postgres", dbinfo)
	if err != nil {
		return nil, err
	}
	if err = conn.Ping(); err != nil {
		return nil, err
	}
	db := &pgdb{conn}
	return db, nil
}

func (db *pgdb) createTable() error {
	schema := `CREATE TABLE IF NOT EXISTS users_first_play (
				"username" 	varchar(30) PRIMARY KEY,
				"first_on" 	timestamp(0) with time zone NOT NULL);`
	_, err := db.Exec(schema)
	return err
}

//insertEntry will insert a metric or update to the earliest given time
func (db *pgdb) insertEntry(username string, t time.Time) error {
	sql := `INSERT into users_first_play as U (username,first_on)  
			VALUES ( $1, $2 ) ON CONFLICT(username) DO UPDATE 
			SET first_on = EXCLUDED.first_on 
			WHERE U.first_on > EXCLUDED.first_on;`

	//ON CONFLICT (upsert) was introduced in pgsql 9.5
	//http://www.postgresql.org/docs/9.5/static/sql-insert.html#SQL-ON-CONFLICT

	_, err := db.Exec(sql, username, t)
	return err
}
