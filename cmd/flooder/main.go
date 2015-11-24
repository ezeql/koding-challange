package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"
)

var (
	url = flag.String("url", "http://127.0.0.1:8080", "metric endpoint url")
)

func main() {
	log.Println("Flooding with metrics... =D")

	client := &http.Client{}
	dest := fmt.Sprintf("%s/metrics", *url)

	m := map[string]interface{}{
		"username": "kodingbot",
		"count":    1,
		"metric":   "kite_call",
	}

	for range time.Tick(500 * time.Millisecond) {

		json, _ := json.Marshal(m)

		contentReader := bytes.NewReader(json)

		req, _ := http.NewRequest("POST", dest, contentReader)
		req.Header.Set("Content-Type", "application/json")
		_, err := client.Do(req)
		if err != nil {
			log.Fatalln(err)
		}

	}
}
