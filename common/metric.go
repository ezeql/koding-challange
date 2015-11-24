package common

import (
	"encoding/json"
	"time"
)

//MetricData
type MetricEntry struct {
	Username string    `json:"username"`
	Count    int64     `json:"count"`
	Metric   string    `json:"metric"`
	Time     time.Time `json:"time"`
}

//MustUnmarshallFromJSON builds a MetricData from JSON buffer
func MustUnmarshallFromJSON(b []byte) MetricEntry {
	m := MetricEntry{}
	if e := json.Unmarshal(b, &m); e != nil {
		Log("invalid json", e)
	}
	return m
}
