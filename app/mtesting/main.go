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
    "net"
    "bytes"
    //"strconv"
    "strings"
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


// ########################################################
type NetstatData struct {
    Data           []SockTable            `json:"data"`
}

// SockTable type represents each line of the /cmd/[tcp|udp]
type SockTable struct {
    Id             string                 `json:"id,omitempty"`
    Timestamp      int64                  `json:"timestamp"`
    LocalAddr      SockAddr               `json:"localAddr"`
    RemoteAddr     SockAddr               `json:"remoteAddr"`
    Relation       Relation               `json:"relation"`
    Options        Options                `json:"options"`
}

// SockAddr represents
type SockAddr struct {
    IP             net.IP                 `json:"ip"`
    Name           string                 `json:"name"`
    Port           uint16                 `json:"-"`
}

type Relation struct {
    Mode           string                 `json:"mode"`
    Port           uint16                 `json:"port"`
    Command        string                 `json:"command,omitempty"`
    Result         int                    `json:"result"`
    Response       float64                `json:"response"`
    Trace          int                    `json:"trace"`
}

type Options struct {
    Service        string                 `json:"service,omitempty"`
    Status         string                 `json:"status,omitempty"`
    Command        string                 `json:"command,omitempty"`
    Timeout        float64                `json:"timeout"`
    MaxRespTime    float64                `json:"maxRespTime"`
    AccountID      uint32                 `json:"accountID"`
}

var (
    lsAddress = flag.String("web.listen-address", "0.0.0.0:8065", "listen address")

    upgrader  = websocket.Upgrader{
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

func startTestInflux(start Start) {
    run <- 0
    sdt = time.Now().UTC().Unix()

    atomic.StoreUint64(&stats.RequestTotal, 0)
    atomic.StoreUint64(&stats.RequestErrors, 0)
    atomic.StoreUint64(&stats.RequestSuccess, 0)

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

                data := getMetrics(stats.PacketSize)
                
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

                log.Printf("[debug] writing to [%s] received status code: %d", start.WriteUrl, resp.StatusCode)

                atomic.AddUint64(&stats.RequestSuccess, 1)

                time.Sleep(time.Duration(start.Interval) * time.Second)
            }

            <- thr

            clnt.client.CloseIdleConnections()
        }()
    }
}

func startTestNetmap(start Start) {
    //run <- 0
    //sdt = time.Now().UTC().Unix()

    for t := 0; t < start.Threads; t++ {

        //if len(run) == 0 {
        //    break;
        //}

        go func(t int) {

            //thr <- 1

            time.Sleep(time.Duration(rand.Intn(10000)) * time.Millisecond)

            clnt := newHttpClient()

            for l := 0; l < 50; l++ { //Повторений раз через интервал

                var records NetstatData

                for r := 0; r < start.PacketSize; r++ {
                    record := SockTable{
                        LocalAddr:     SockAddr{
                            IP:            net.IPv4(10,20,20,1),
                            Name:          fmt.Sprintf("host-%d", t),
                        },
                        RemoteAddr:    SockAddr{
                            IP:            net.IPv4(10,20,20,2),
                            Name:          fmt.Sprintf("host2-%d", r),
                        },
                        Relation:      Relation{
                            Mode:          "tcp",
                            Port:          22,
                        },
                        Options:       Options{},
                    }
                    records.Data = append(records.Data, record)
                }

                //if len(run) == 0 {
                //    break;
                //}

                //atomic.AddUint64(&stats.RequestTotal, 1)

                data, err := json.Marshal(records)
                if err != nil {
                    log.Printf("[error] %v", err)
                    continue
                }
                

                //var buf bytes.Buffer

                //data := getMetrics(stats.PacketSize)
                
                //writer := gzip.NewWriter(&buf)
                //if _, err := writer.Write(data); err != nil {
                //    atomic.AddUint64(&stats.RequestErrors, 1)
                //    log.Printf("[error] %v", err)
                //    time.Sleep(time.Duration(start.Interval) * time.Second)
                //    continue
                //}
                //if err := writer.Close(); err != nil {
                //    atomic.AddUint64(&stats.RequestErrors, 1)
                //    log.Printf("[error] %v", err)
                //    time.Sleep(time.Duration(start.Interval) * time.Second)
                //    continue
                //}

                log.Printf("[test] %v", t)

                req, err := http.NewRequest("POST", start.WriteUrl, bytes.NewReader(data))
                if err != nil {
                    //atomic.AddUint64(&stats.RequestErrors, 1)
                    log.Printf("[error] %s - %v", start.WriteUrl, err)
                    time.Sleep(time.Duration(start.Interval) * time.Second)
                    continue
                }

                

                //req.Header.Set("Content-Encoding", "gzip")

                resp, err := clnt.client.Do(req)
                if err != nil {
                    //requestErrors.With(prometheus.Labels{}).Inc()
                    //atomic.AddUint64(&stats.RequestErrors, 1)
                    log.Printf("[error] %s - %v", start.WriteUrl, err)
                    time.Sleep(time.Duration(start.Interval) * time.Second)
                    continue
                }
                io.Copy(ioutil.Discard, resp.Body)
                defer resp.Body.Close()

                

                if resp.StatusCode >= 400 {
                    //atomic.AddUint64(&stats.RequestErrors, 1)
                    log.Printf("[error] when writing to [%s] received status code: %d", start.WriteUrl, resp.StatusCode)
                    time.Sleep(time.Duration(start.Interval) * time.Second)
                    continue
                }

                log.Printf("[debug] writing to [%s] received status code: %d", start.WriteUrl, resp.StatusCode)

                //atomic.AddUint64(&stats.RequestSuccess, 1)

                time.Sleep(time.Duration(start.Interval) * time.Second)
            }

            //<- thr

            clnt.client.CloseIdleConnections()
        }(t)
    }
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

    startTestInflux(start)
    
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

func getStatus() ([]byte, error) {
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
    
    return jsn, err
    
}

func httpStatus(w http.ResponseWriter, r *http.Request) {
    jsn, err := getStatus()

    if err != nil {
        log.Printf("[error] %v", err)
        w.WriteHeader(500)
        return
    }

    w.WriteHeader(200)
    w.Write(jsn)
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

        jsn, err := getStatus()

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

    host := fmt.Sprintf("hostname-%d.example.com", rand.Intn(100))
    lines := []string{
        fmt.Sprintf("cpu_usage,host=%s,cpu=cpu-total active=%v", host, rand.Intn(25)*4),
        fmt.Sprintf("mem_used,host=%s percent=%v", host, rand.Intn(25)*4),
        fmt.Sprintf("swap_used,host=%s percent=%v", host, rand.Intn(25)*4),
        fmt.Sprintf("disk_used,host=%s,path=/tmp percent=%v", host, rand.Intn(25)*4),
        fmt.Sprintf("disk_used,host=%s,path=/usr percent=%v", host, rand.Intn(25)*4),
        fmt.Sprintf("procstat_lookup,host=%s,application=test,instance=1 running=%v", host, rand.Intn(1)),
        fmt.Sprintf("filestat,host=%s,path=/test/test/test exists=%v", host, rand.Intn(1)),
    }
    //for i := 0; i < size; i++ {
    //    lines = lines + fmt.Sprintf("cpu_usage,host=%s,cpu=cpu-total active=%v\r\n", host, rand.Intn(25)*4)
    //}

    return []byte(strings.Join(lines, "\r\n"))
}

func main() {

    //Influx - http://localhost:8480/insert/0/influx/write
    //Netmap - http://localhost:8084/api/v1/netmap/records

    // Command-line flag parsing
    Type           := flag.String("type", "", "(influx|netmap)")
    WriteUrl       := flag.String("remoteWrite.url", "", "RemoteWriteUrl")
    Threads        := flag.Int("threads", 0, "Threads")
    PacketSize     := flag.Int("packet.size", 500, "PacketSize")
    Interval       := flag.Int("interval", 10, "Interval")
    flag.Parse()

    if *Type == "influx" {
        start := Start{
            WriteUrl:   *WriteUrl,
            Threads:    *Threads,
            PacketSize: *PacketSize,
            Interval:   *Interval,
        }

        startTestInflux(start)
    }

    if *Type == "netmap" {
        start := Start{
            WriteUrl:   *WriteUrl,
            Threads:    *Threads,
            PacketSize: *PacketSize,
            Interval:   *Interval,
        }

        startTestNetmap(start)
    }

    // Creating monitoring
    //prometheus.MustRegister(requestErrors)

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

    http.HandleFunc("/api/v1/start", httpStart)
    http.HandleFunc("/api/v1/stop", httpStop)
    http.HandleFunc("/api/v1/status", httpStatus)
    http.HandleFunc("/ws", wsEndpoint)

    log.Fatal(http.ListenAndServe(*lsAddress, nil))
}