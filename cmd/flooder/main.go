package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"time"
)

func main() {
	log.Println("Flooding with metrics... =D")

	client := &http.Client{}
	for range time.Tick(time.Millisecond) {
		m := map[string]interface{}{
			"username": "kodingbot",
			"count":    1,
			"metric":   "kite_call",
		}
		json, err := json.Marshal(m)
		if err != nil {
			log.Fatalln(err)
		}
		contentReader := bytes.NewReader(json)

		req, _ := http.NewRequest("POST", "http://127.0.0.1:8080/metric", contentReader)
		req.Header.Set("Content-Type", "application/json")
		_, err = client.Do(req)
		if err != nil {
			log.Fatalln(err)
		}

	}
}
