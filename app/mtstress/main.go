package main

import (
    "os"
    "os/signal"
    "io"
    "io/ioutil"
    "syscall"
    "flag"
    "time"
    "log"
    "fmt"
    "bytes"
	"net/http"
	//"net/http/httputil"
    "compress/gzip"
)

type HttpClient struct {
    client           *http.Client
}

type HttpConfig struct {
    URLs             []string
    Headers          map[string]string
    ContentEncoding  string
}

var (
    url         = flag.String("remoteWrite.url", "http://127.0.0.1:8480/insert/0/influx/write", "Remote write compatible storage")
    threads     = flag.Int("threads", 200, "Number of threads") 
    interval    = flag.Int("interval", 5, "How often a new metrics packet will be sent") 
    packetSize  = flag.Int("packetSize", 1000, "The number of metrics sent in the packet") 
)

func newHttpClient() *HttpClient {
    client := &HttpClient{ 
        client: &http.Client{
            Transport: &http.Transport{
                MaxIdleConnsPerHost: 10,
                IdleConnTimeout:     90 * time.Second,
                DisableCompression:  false,
            },
            Timeout: 30 * time.Second,
        },
    }
    return client
}

func getMetrics() []byte {
    lines := ""
    for i := 0; i < *packetSize; i++ {
        lines = lines + fmt.Sprintf("test_metric,location=us-midwest,line=%v temperature=2\r\n", i)
    }
    return []byte(lines)
}

func main() {

	// Command-line flag parsing
	flag.Parse()

	// Program completion signal processing
    c := make(chan os.Signal, 2)
    signal.Notify(c, os.Interrupt, syscall.SIGTERM)
    go func() {
        <-c
        log.Print("[info] mtstress stopped")
        os.Exit(0)
    }()

    for i := 0; i < *threads; i++ {
        go func() {

            clnt := newHttpClient()

            for i := 0; i < 300; i++ {
                time.Sleep(time.Duration(*interval) * time.Second)

                var buf bytes.Buffer

                data := getMetrics()
                
                writer := gzip.NewWriter(&buf)
                if _, err := writer.Write(data); err != nil {
                    log.Printf("[error] %v", err)
                    continue
                }
                if err := writer.Close(); err != nil {
                    log.Printf("[error] %v", err)
                    continue
                }

                req, err := http.NewRequest("POST", *url, &buf)
                if err != nil {
                    log.Printf("[error] %s - %v", *url, err)
                    continue
                }

                req.Header.Set("Content-Encoding", "gzip")

                resp, err := clnt.client.Do(req)
                if err != nil {
                    log.Printf("[error] %s - %v", *url, err)
                    continue
                }
                io.Copy(ioutil.Discard, resp.Body)
                defer resp.Body.Close()

                if resp.StatusCode >= 400 {
                    log.Printf("[error] when writing to [%s] received status code: %d", *url, resp.StatusCode)
                    continue
                }
            }
        }()
    }

	// Influxdb
	// /insert/<accountID>/influx/write
	// /insert/<accountID>/influx/api/v2/write

    log.Print("[info] mtstress started")

	// Daemon mode
    for {
        time.Sleep(300 * time.Second)
    }
}