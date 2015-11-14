package common

import (
	"encoding/json"
	"time"
)

//MetricData
type MetricData struct {
	Username string
	Count    int64
	Metric   string
	Time     *time.Time
}

//FromJSONBytes builds a MetricData from JSON buffer
func FromJSONBytes(b []byte) (MetricData, error) {
	m := MetricData{}
	e := json.Unmarshal(b, &m)
	if m.Time == nil {
		t := time.Now().UTC()
		m.Time = &t
	}
	return m, e
}
