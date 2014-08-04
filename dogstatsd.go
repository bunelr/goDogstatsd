package main

import (
	// Project
	"github.com/bunelr/goDogstatsd/aggregator"
	"github.com/bunelr/goDogstatsd/config"

	// stdlib
	"bytes"
	"flag"
	"log"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	MAX_UDP_PACKET_SIZE int = 8 * 1024
)

var (
	// Go does not allow const array
	NEWLINE_SEP  = []byte("\n")
	EVENT_PREFIX = []byte("_e")
	SEMI_COLON   = []byte(":")
	PIPE_SEP     = []byte("|")
	AROBASE      = []byte("@")
	POUND_SIGN   = []byte("#")
	COMA         = []byte(",")
)

var intake_pipeline = make(chan []byte)
var bucket_aggregator = aggregator.NewAggregator()

func run_udp_server(config config.Config) {
	binding := "localhost:" + config.Listening_port
	addr, error := net.ResolveUDPAddr("udp", binding)
	if error != nil {
		log.Println("Can't resolve localhost, trying with 127.0.0.1")
		binding = "127.0.0.1:" + config.Listening_port
		addr, error = net.ResolveUDPAddr("udp", binding)
		if error != nil {
			log.Fatalln("Can't resolve the address for the udp server:", error)
		}
	}

	conn, error := net.ListenUDP("udp", addr)
	if error != nil {
		log.Fatalln("Can't listen on connection %s", error)
	}

	message := make([]byte, MAX_UDP_PACKET_SIZE)

	for {
		bytes_read, _, err := conn.ReadFrom(message)
		if err != nil {
			log.Println("Could not read the message, Error: ", err)
			continue
		}
		intake_pipeline <- (message[:bytes_read])
	}

	conn.Close()

}

func run_packet_handler() {
	for {
		packets := <-intake_pipeline
		for _, packet := range bytes.Split(packets, NEWLINE_SEP) {
			if bytes.HasPrefix(packet, EVENT_PREFIX) {
				log.Println("UDP events are not yet implemented")
			} else {
				parse_metric_packet(packet)
			}
		}
	}
}

func parse_metric_packet(packet []byte) {
	//Add error handling to these functions
	name_and_data := bytes.SplitN(packet, SEMI_COLON, 2)

	name := string(name_and_data[0])
	data := bytes.Split(name_and_data[1], PIPE_SEP)
	if len(data) < 2 {
		log.Printf("Wrongly formatted packet: %s", string(packet))
	}

	raw_value := data[0]
	metric_type := string(data[1])

	var sample_rate float64 = 1
	var byte_tags [][]byte
	var tags []string
	var err error

	for _, datum := range data[2:] {
		// maybe replace by exact byte value and check first element
		if bytes.HasPrefix(datum, AROBASE) {
			sample_rate, err = strconv.ParseFloat(string(datum[1:]), 64)
			if err != nil {
				log.Println("Warning: Couldn't decode the sample rate: ", err)
				sample_rate = 1
			}
		} else if bytes.HasPrefix(datum, POUND_SIGN) {
			byte_tags = bytes.Split(datum[1:], COMA)
		}
	}

	//TODO: Maybe create the tags array and set his size at the beginning
	for _, tag := range byte_tags {
		tags = append(tags, string(tag))
	}

	sort.Strings(tags)
	context := aggregator.Context{name, strings.Join(tags, ",")}

	bucket_aggregator.Sample_metric(context, name, tags, metric_type, raw_value, sample_rate)
}

func run_flusher(config config.Config) {

	var req *http.Request
	var res *http.Response

	client := &http.Client{}
	api_key := config.Api_key
	url := config.Datadog_host + "/api/v1/series/?api_key=" + api_key
	wait_period := time.Duration(config.Flush_interval) * time.Second

	headers := http.Header{}
	headers.Set("Content-type", "application/json")
	headers.Set("api_key", api_key)

	for {
		time.Sleep(wait_period)
		log.Println("Starting Flush")

		payload, err := bucket_aggregator.Flush()
		if err != nil {
			log.Println(err)
			continue
		}
		body := bytes.NewBuffer(payload)
		req, err = http.NewRequest("POST", url, body)
		if err != nil {
			log.Println("Error preparing the request:", err)
			continue
		}
		req.Header = headers
		res, err = client.Do(req)
		if err != nil {
			log.Printf("Error flushing to Datadog: %s, Message: %s", err, res)
		} else {
			log.Println("Flush completed")
		}
	}
}

func main() {
	config_file := flag.String("config", "", "Path to the configuration file")
	flag.Parse()
	config := config.Get_config(*config_file)

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	log.Println("Starting the udp server")
	go run_udp_server(config)

	log.Println("Starting the packet parser")
	go run_packet_handler()

	log.Println("Starting the flusher")
	run_flusher(config)

}
