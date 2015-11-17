package main

import (
	"flag"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
	"time"
)

var db *DB

func TestMain(m *testing.M) {
	flag.Parse()
	var err error
	db, err = openDB(*postgreSQLHost, *postgreSQLPort, *postgreSQLUser, *postgreSQLPassword, *postgreSQLDB)
	if err != nil {
		log.Fatalln("cannot connect to db")
	}
	defer db.Close()

	if err = db.createTable(); err != nil {
		log.Fatalln("cannot createTable")
	}

	if _, err = db.Exec("TRUNCATE users_first_play"); err != nil {
		log.Fatalln("cannot TRUNCATE")
	}

	os.Exit(m.Run())
}

func TestFirstEntry(t *testing.T) {
	user := "JohnDoe"
	now := time.Now().Round(time.Second)
	later := now.Add(time.Minute * 5).UTC()

	err := db.insertEntry(user, now)
	assert.Nil(t, err)

	err = db.insertEntry(user, later)
	assert.Nil(t, err)

	r := db.QueryRow("SELECT first_on from users_first_play where username = $1", user)

	var first time.Time
	r.Scan(&first)

	assert.True(t, now.Equal(first))
}
