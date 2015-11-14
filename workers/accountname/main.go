package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/ezeql/koding-test/common"
	_ "github.com/lib/pq"
	"log"
)

var (
	rabbitHost         = flag.String("rabbit-host", "localhost", "RabbitMQ host")
	rabbitPort         = flag.Int("rabbit-port", 5672, "RabbitMQ port")
	rabbitUser         = flag.String("rabbit-user", "guest", "RabbitMQ username")
	rabbitPassword     = flag.String("rabbit-password", "guest", "RabbitMQ password")
	postgreSQLHost     = flag.String("postgresql-host", "localhost", "PostgreSQL host")
	postgreSQLPort     = flag.Int("postgresql-port", 5432, "PostgreSQL port")
	postgreSQLUser     = flag.String("postgresql-user", "postgres", "PostgreSQL username")
	postgreSQLPassword = flag.String("postgresql-password", "", "PostgreSQL password")
	postgreSQLDB       = flag.String("postgresql-db", "", "PostgreSQL DB name")
)

func main() {
	flag.Parse()
	dbinfo := fmt.Sprintf("host=%v port=%v user=%v password=%v dbname=%v sslmode=disable",
		postgreSQLHost, postgreSQLPort, postgreSQLUser, postgreSQLPassword, postgreSQLDB)

	db, err := sql.Open("postgres", dbinfo)
	if err != nil {
		log.Fatalln("cannot open connection to DB:", err)
	}
	if err = db.Ping(); err != nil {
		log.Fatalln("cannot connect to DB:", err)
	}

	defer func() {
		if err := db.Close(); err != nil {
			log.Println("cannot close to postgres:", err)
		}
	}()

	schema := `CREATE TABLE IF NOT EXISTS "users_first_play" (
				"username" 	varchar(30) PRIMARY KEY,
				"first_on" 	date	 	NOT NULL);`
	if _, err = db.Exec(schema); err != nil {
		log.Fatalln("cannot create table:", err)
	}

	c, err := common.BuildRabbitMQConnector(*rabbitHost, *rabbitPort, *rabbitUser, *rabbitPassword)
	if err != nil {
		log.Fatalln("cannot connect to rabbitmq", err)
	}

	insert := `INSERT INTO users_first_play(username,first_on) 
				VALUES ( $1, $2 ) 
				ON CONFLICT(username) DO NOTHING;`

	c.Handle(func(b []byte) {
		m, err := common.FromJSONBytes(b)
		if err != nil {
			fmt.Print("invalid json")
		}
		if _, err = db.Exec(insert, m.Username); err != nil {
			log.Println("error executing statement:", err)
		}
	})

}
