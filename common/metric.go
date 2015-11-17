package common

import (
	"encoding/json"
	"time"
)

//MetricData
type MetricEntry struct {
	Username string
	Count    int64
	Metric   string
	Time     *time.Time
}

//MustUnmarshallFromJSON builds a MetricData from JSON buffer
func MustUnmarshallFromJSON(b []byte) MetricEntry {
	m := MetricEntry{}
	if e := json.Unmarshal(b, &m); e != nil {
		panic(e)
	}
	if m.Time == nil {
		t := time.Now().UTC()
		m.Time = &t
	}
	return m
}
