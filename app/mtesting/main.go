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
    "math/rand"
    "sync/atomic"
    "compress/gzip"
    "encoding/json"
    "github.com/gorilla/websocket"
    //"github.com/prometheus/client_golang/prometheus"
    //"github.com/prometheus/client_golang/prometheus/promhttp"
)

type HttpClient struct {
    client           *http.Client
}

type HttpConfig struct {
    URLs             []string
    Headers          map[string]string
    ContentEncoding  string
}

type Resp struct {
    Status           string                    `json:"status"`
    Error            string                    `json:"error,omitempty"`
    Warnings         []string                  `json:"warnings,omitempty"`
    Data             Data                      `json:"data"`
}

type Data struct {
    RequestTotal     uint64                    `json:"requestTotal"`
    RequestSuccess   uint64                    `json:"requestSuccess"`
    RequestErrors    uint64                    `json:"requestErrors"`
    PacketSize       int                       `json:"packetSize"`
    RequestSpeed     float64                   `json:"requestSpeed"`
    Threads          int                       `json:"threads"`
    Seconds          int64                     `json:"seconds"`
}

type Start struct {
    WriteUrl         string                    `json:"writeUrl"`
    Threads          int                       `json:"threads"`
    Interval         int                       `json:"interval"`
    PacketSize       int                       `json:"packetSize"`
}

var (
    lsAddress   = flag.String("web.listen-address", ":8065", "listen address")

    upgrader    = websocket.Upgrader{
        ReadBufferSize:  1024,
        WriteBufferSize: 1024,
        CheckOrigin:     func(r *http.Request) bool { return true },
    }

    run = make(chan int, 1)
    thr = make(chan int, 100000)
    sdt = time.Now().UTC().Unix()
    stats = &Data{}
)

func encodeResp(resp *Resp) []byte {
    jsn, err := json.Marshal(resp)
    if err != nil {
        return encodeResp(&Resp{Status:"error", Error:err.Error()})
    }
    return jsn
}

func httpStart(w http.ResponseWriter, r *http.Request) {

    var start Start

    body, err := ioutil.ReadAll(r.Body)
    if err != nil {
        log.Printf("[error] %v - %s", err, r.URL.Path)
        w.WriteHeader(400)
        w.Write(encodeResp(&Resp{Status:"error", Error:err.Error()}))
        return
    }

    if err := json.Unmarshal(body, &start); err != nil {
        log.Printf("[error] %v - %s", err, r.URL.Path)
        w.WriteHeader(400)
        w.Write(encodeResp(&Resp{Status:"error", Error:err.Error()}))
        return
    }

    stats = &Data{
        RequestTotal: 0,
        RequestSuccess: 0,
        RequestErrors: 0,
        PacketSize: start.PacketSize,
    }

    run <- 0
    sdt = time.Now().UTC().Unix()

    for t := 0; t < start.Threads; t++ {

        if len(run) == 0 {
            break;
        }

        go func() {

            thr <- 1

            time.Sleep(time.Duration(rand.Intn(10000)) * time.Millisecond)

            clnt := newHttpClient()

            for i := 0; i < 300; i++ {

                if len(run) == 0 {
                    break;
                }

                atomic.AddUint64(&stats.RequestTotal, 1)

                var buf bytes.Buffer

                data := getMetrics(start.PacketSize)
                
                writer := gzip.NewWriter(&buf)
                if _, err := writer.Write(data); err != nil {
                    atomic.AddUint64(&stats.RequestErrors, 1)
                    log.Printf("[error] %v", err)
                    time.Sleep(time.Duration(start.Interval) * time.Second)
                    continue
                }
                if err := writer.Close(); err != nil {
                    atomic.AddUint64(&stats.RequestErrors, 1)
                    log.Printf("[error] %v", err)
                    time.Sleep(time.Duration(start.Interval) * time.Second)
                    continue
                }

                req, err := http.NewRequest("POST", start.WriteUrl, &buf)
                if err != nil {
                    atomic.AddUint64(&stats.RequestErrors, 1)
                    log.Printf("[error] %s - %v", start.WriteUrl, err)
                    time.Sleep(time.Duration(start.Interval) * time.Second)
                    continue
                }

                req.Header.Set("Content-Encoding", "gzip")

                resp, err := clnt.client.Do(req)
                if err != nil {
                    //requestErrors.With(prometheus.Labels{}).Inc()
                    atomic.AddUint64(&stats.RequestErrors, 1)
                    log.Printf("[error] %s - %v", start.WriteUrl, err)
                    time.Sleep(time.Duration(start.Interval) * time.Second)
                    continue
                }
                io.Copy(ioutil.Discard, resp.Body)
                defer resp.Body.Close()

                if resp.StatusCode >= 400 {
                    atomic.AddUint64(&stats.RequestErrors, 1)
                    log.Printf("[error] when writing to [%s] received status code: %d", start.WriteUrl, resp.StatusCode)
                    time.Sleep(time.Duration(start.Interval) * time.Second)
                    continue
                }

                atomic.AddUint64(&stats.RequestSuccess, 1)

                time.Sleep(time.Duration(start.Interval) * time.Second)
            }

            <- thr

            clnt.client.CloseIdleConnections()
        }()
    }
    
    w.WriteHeader(204)
    return
}

func httpStop(w http.ResponseWriter, r *http.Request) {

    if len(run) > 0 {
        <- run
    }

    for {
        if len(thr) == 0 {
            break;
        } 
        time.Sleep(100 * time.Millisecond)
    }
    
    w.WriteHeader(204)
    return
}

func wsEndpoint(w http.ResponseWriter, r *http.Request) {
    ws, err := upgrader.Upgrade(w, r, nil)
    if err != nil {
        log.Printf("[error] %v", err)
        w.WriteHeader(500)
        return
    }
    defer ws.Close()

    for {

        tnow := time.Now().UTC().Unix() - sdt
        rspd := float64(0)
        if tnow > 0 {
            rspd = float64(stats.RequestSuccess / uint64(tnow))
        }

        jsn, err := json.Marshal(
            &Resp{
                Status:"success", 
                Data: Data{
                    RequestTotal: stats.RequestTotal,
                    RequestSuccess: stats.RequestSuccess,
                    RequestErrors: stats.RequestErrors,
                    PacketSize: stats.PacketSize,
                    RequestSpeed: rspd,
                    Threads: len(thr),
                    Seconds: tnow,
                },
            },
        )
        if err != nil {
            log.Printf("[error] %v", err)
            w.WriteHeader(500)
            return
        }

        if err := ws.WriteMessage(websocket.TextMessage, []byte(jsn)); err != nil {
            log.Printf("[error] %v", err)
            w.WriteHeader(500)
            return
        }

        time.Sleep(100 * time.Millisecond)
    }
}

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

func getMetrics(size int) []byte {
    lines := ""
    for i := 0; i < size; i++ {
        lines = lines + fmt.Sprintf("test_metric,location=us-midwest,line=%v temperature=2\r\n", i)
    }
    return []byte(lines)
}

func main() {

    // Creating monitoring
    //prometheus.MustRegister(requestErrors)

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

	// Influxdb
	// /insert/<accountID>/influx/write
	// /insert/<accountID>/influx/api/v2/write

    log.Print("[info] mtstress started")

    //http.Handle("/metrics", promhttp.Handler())
    http.HandleFunc("/api/v1/start", httpStart)
    http.HandleFunc("/api/v1/stop", httpStop)
    http.HandleFunc("/api/v1/ws", wsEndpoint)

    log.Fatal(http.ListenAndServe(*lsAddress, nil))
}