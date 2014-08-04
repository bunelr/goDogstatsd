package aggregator

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"
)

const MAX_STORED_HIST_SAMPLE int = 1000

type metric interface {
	sample(value []byte, sample_rate float64) error
	flush(timestamp int64, interval int64) formattedMetric
}

type Context struct {
	Metric_name string
	Tag_string  string
}

type formattedMetric struct {
	Metric      string           `json:"metric"`
	Points      [][2]interface{} `json:"points"`
	Tags        []string         `json:"tags"`
	Host        string           `json:"host"`
	Metric_type string           `json:"type"`
	Interval    int64            `json:"interval"`
}

type gauge struct {
	name  string
	tags  []string
	value float64
}

func newGauge(name string, tags []string) *gauge {
	gauge := new(gauge)
	gauge.name = name
	gauge.tags = tags
	return gauge
}

func (m *gauge) sample(value []byte, sample_rate float64) error {
	parsed_value, err := strconv.ParseFloat(string(value), 64)
	if err != nil {
		return fmt.Errorf("Value %s can't be parsed as float: %s", string(value), err)
	} else {
		m.value = parsed_value
		return nil
	}
}

func (m gauge) flush(timestamp int64, interval int64) formattedMetric {
	points := make([][2]interface{}, 1)
	points[0][0] = timestamp
	points[0][1] = m.value
	return formattedMetric{m.name,
		points,
		m.tags,
		"myhost",
		"gauge",
		interval}
}

type histogram struct {
	name    string
	tags    []string
	count   int64
	samples []float64
}

func newHistogram(name string, tags []string) *histogram {
	histogram := new(histogram)
	histogram.name = name
	histogram.tags = tags
	histogram.count = 0
	histogram.samples = make([]float64, 0, MAX_STORED_HIST_SAMPLE)
	return histogram
}

func (m *histogram) sample(value []byte, sample_rate float64) error {
	float_value, err := strconv.ParseFloat(string(value), 64)
	if err != nil {
		return fmt.Errorf("Can't parse value %s as float: %s", string(value), err)
	} else {
		m.count += int64(1 / sample_rate)
		if len(m.samples) < MAX_STORED_HIST_SAMPLE {
			m.samples[len(m.samples)] = float_value
		}
		return nil
	}
}

func (m histogram) flush(timestamp int64, interval int64) formattedMetric {
	return formattedMetric{}
}

type counter struct {
	name  string
	tags  []string
	count int64
}

func newCounter(name string, tags []string) *counter {
	counter := new(counter)
	counter.name = name
	counter.tags = tags
	return counter
}

func (m *counter) sample(value []byte, sample_rate float64) error {
	int_value, err := strconv.ParseFloat(string(value), 64)
	if err != nil {
		return fmt.Errorf("Can't parse value %s as float: %s", string(value), err)
	} else {
		m.count += int64(int_value / sample_rate)
		return nil
	}
}

func (m counter) flush(timestamp int64, interval int64) formattedMetric {
	points := make([][2]interface{}, 1)
	points[0][0] = timestamp
	points[0][1] = m.count
	return formattedMetric{m.name,
		points,
		m.tags,
		"myhost",
		"counter",
		interval}
}

type set struct {
	name   string
	tags   []string
	values map[string]bool
}

func newSet(name string, tags []string) *set {
	s := new(set)
	s.name = name
	s.tags = tags
	s.values = make(map[string]bool)
	return s
}

func (m *set) sample(value []byte, sample_rate float64) error {
	m.values[string(value)] = true
	return nil
}

func (m set) flush(timestamp int64, interval int64) formattedMetric {
	points := make([][2]interface{}, 1)
	points[0][0] = timestamp
	points[0][1] = len(m.values)
	return formattedMetric{m.name,
		points,
		m.tags,
		"myhost",
		"gauge",
		interval}
}

type Aggregator struct {
	metrics_by_context map[Context]metric
}

func NewAggregator() Aggregator {
	m := make(map[Context]metric)
	return Aggregator{m}
}

func (a Aggregator) Sample_metric(c Context, name string, tags []string, metric_type string, raw_value []byte, sample_rate float64) {
	var sampled_metric metric
	sampled_metric, exists := a.metrics_by_context[c]
	if !exists {
		metricPtr, err := a.CreateMetric(name, tags, metric_type, c)
		sampled_metric = *metricPtr
		if err != nil {
			log.Println(err)
			return
		}
	}
	err := sampled_metric.sample(raw_value, sample_rate)
	if !exists && err == nil {
		a.metrics_by_context[c] = sampled_metric
	}
}

func (a Aggregator) Flush() ([]byte, error) {
	var payload map[string][]formattedMetric
	payload = make(map[string][]formattedMetric)

	to_flush := make([]formattedMetric, 0)
	timestamp := time.Now().Unix()
	var interval int64 = 15
	for _, metric := range a.metrics_by_context {
		to_flush = append(to_flush, metric.flush(timestamp, interval))
	}
	payload["series"] = to_flush
	serialized, err := json.Marshal(payload)
	a.metrics_by_context = make(map[Context]metric)
	if err != nil {
		return nil, fmt.Errorf("Could not serialize payload to JSON: %s", err)
	} else {
		return serialized, nil
	}
}

func (a Aggregator) CreateMetric(name string, tags []string, metric_type string, context Context) (*metric, error) {
	var new_metric metric

	switch metric_type {
	case "g":
		new_metric = newGauge(name, tags)
	case "h", "ms":
		new_metric = newHistogram(name, tags)
	case "c":
		new_metric = newCounter(name, tags)
	case "s":
		new_metric = newSet(name, tags)
	default:
		return nil, errors.New("Unsupported metric type")
	}

	return &new_metric, nil
}
