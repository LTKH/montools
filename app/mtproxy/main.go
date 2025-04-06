package main

import (
    "io"
    "io/ioutil"
    "strconv"
    "net"
    //"net/rpc"
    "net/url"
    "net/http"
    "time"
    "log"
    //"fmt"
    "os"
    "os/signal"
    "syscall"
    "flag"
    "sync"
    //"math"
    "strings"
    "bytes"
    "crypto/sha1"
    "encoding/hex"
    "compress/gzip"

    "github.com/ltkh/montools/internal/config/mtproxy"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
    requestTotal = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "mtproxy_http_requests_total",
            Help: "",
        },
        []string{"listen_addr","user","code"},
    )
    sizeBytesTotal = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "mtproxy_http_size_bytes_total",
            Help: "",
        },
        []string{"listen_addr","url_path","object"},
    )
    sizeBytesDropped = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "mtproxy_http_size_bytes_dropped",
            Help: "",
        },
        []string{"listen_addr","url_path","object"},
    )
    sizeBytesBucket = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "mtproxy_http_size_bytes_avg",
            Help: "",
        },
        []string{"listen_addr","url_path","object"},
    )
    healthCheckFailed = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "mtproxy_health_check_failed",
            Help: "",
        },
        []string{"target_url"},
    )
    upstreamRequests = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "mtproxy_upstream_requests_count",
            Help: "",
        },
        []string{"target_url"},
    )
)

type API struct {
    Upstream     *config.Upstream
    //Peers        *config.Peers
    //Limits       *Limits
    Objects      *Objects
    Client       *http.Client
    Debug        bool
}

type Objects struct {
    sync.RWMutex
    items        map[string]*Object
    limit        float64
}

type Object struct {
    Timestamp    int64
    Arr          []float64
    Avg          float64
    UrlPath      string
}

func sumArray(numbers []float64) float64 {
    result := float64(0)
    for i := 0; i < len(numbers); i++ {
        result += numbers[i]
    }
    return result
}

func getObject(r *http.Request, header string) string {
    IPAddress := r.Header.Get(header)
    if IPAddress == "" {
        IPAddress = r.Header.Get("X-Real-Ip")
    } 
    if IPAddress == "" {
        IPAddress = r.Header.Get("X-Forwarded-For")
    } 
    if IPAddress == "" {
        remAddr := strings.Split(r.RemoteAddr, ":")
        if len(remAddr) > 0 { IPAddress = remAddr[0] }
    } 
    if IPAddress == "" { 
        IPAddress = "unknown" 
    }
    return IPAddress
}

func (o *Objects) Get(key string) *Object {
    o.RLock()
    defer o.RUnlock()

    item, ok := o.items[key]
    if ok {
        return item
    }
    
    return &Object{}
}

func (o *Objects) Set(key, path string, size float64) *Object {
    o.Lock()
    defer o.Unlock()

    for _, k := range []string{key, "all"}{
        item, ok := o.items[k]
        if ok {
            o.items[k] = &Object{Timestamp: time.Now().Unix(), Arr: append(item.Arr, size), Avg: item.Avg, UrlPath: path}
        } else {
            o.items[k] = &Object{Timestamp: time.Now().Unix(), Arr: []float64{size}, Avg: float64(0), UrlPath: path}
        }
    }

    return o.items[key]
}

func (o *Objects) Update(key string, updt time.Duration) *Object {
    o.Lock()
    defer o.Unlock()

    item, ok := o.items[key]
    if ok {
        avg := sumArray(item.Arr)/float64(updt.Seconds())
        object := &Object{Timestamp: item.Timestamp, Arr: []float64{0}, Avg: avg, UrlPath: item.UrlPath}
        if avg == 0 && item.Timestamp + 600 < time.Now().Unix() {
            delete(o.items, key)
        } else {
            o.items[key] = object
        }
        return object
    }

    return &Object{}
}

func (o *Objects) Items() []string {
    o.RLock()
    defer o.RUnlock()

    items := []string{}
    for key, _ := range o.items {
        items = append(items, key)
    }
    
    return items
}

func NewAPI(c *config.HttpClient, u *config.Upstream, d bool) (*API, error) {
    if c.Timeout == 0 {
        c.Timeout = 5 * time.Second
    }
    if c.HttpTransport.MaxIdleConnsPerHost == 0 {
        c.HttpTransport.MaxIdleConnsPerHost = 3000
    }
    if c.HttpTransport.DialContext.Timeout == 0 {
        c.HttpTransport.DialContext.Timeout = 5 * time.Second
    }
    if c.HttpTransport.DialContext.KeepAlive == 0 {
        c.HttpTransport.DialContext.KeepAlive = 60 * time.Second
    }
    if c.HttpTransport.ExpectContinueTimeout == 0 {
        c.HttpTransport.ExpectContinueTimeout = 1 * time.Second
    }
    if c.HttpTransport.TLSHandshakeTimeout == 0 {
        c.HttpTransport.TLSHandshakeTimeout = 5 * time.Second
    }
    if c.HttpTransport.ResponseHeaderTimeout == 0 {
        c.HttpTransport.ResponseHeaderTimeout = 10 * time.Second
    }

    api := &API{ 
        Upstream: u,
        Objects: &Objects{items: make(map[string]*Object)},
        Client: &http.Client{
            Timeout: c.Timeout,
            Transport: &http.Transport{
                MaxIdleConnsPerHost: c.HttpTransport.MaxIdleConnsPerHost,
                DialContext: (&net.Dialer{
                    Timeout:   c.HttpTransport.DialContext.Timeout,
                    KeepAlive: c.HttpTransport.DialContext.KeepAlive,
                }).DialContext,
                ExpectContinueTimeout: c.HttpTransport.ExpectContinueTimeout,
                TLSHandshakeTimeout:   c.HttpTransport.TLSHandshakeTimeout,
                ResponseHeaderTimeout: c.HttpTransport.ResponseHeaderTimeout,
            },
        },
        Debug: d,
    }

    go func(api *API){
        if api.Upstream.UpdateStat == 0 {
            api.Upstream.UpdateStat = 60 * time.Second
        }

        for {
            items := api.Objects.Items()
            for _, key := range items {
                item := api.Objects.Update(key, api.Upstream.UpdateStat)
                sizeBytesBucket.With(prometheus.Labels{"listen_addr": api.Upstream.ListenAddr, "url_path": item.UrlPath, "object": key}).Set(item.Avg)
            }
            time.Sleep(api.Upstream.UpdateStat)
        }
    }(api)
    
    return api, nil
}

func getStringHash(text string) string {
    h := sha1.New()
    io.WriteString(h, text)
    return hex.EncodeToString(h.Sum(nil))
}

func getPrefixURL(prefix []*config.URLPrefix) *config.URLPrefix {
    var urlPrefix *config.URLPrefix

    if len(prefix) == 0 {
        return urlPrefix
    }
    
    requests := 1000000

    for _, up := range prefix {
        if len(up.Health) != 0 {
            continue
        }

        if len(up.Requests) < requests {
            requests = len(up.Requests)
            urlPrefix = up
        }
    }

    return urlPrefix
}

func readData(r *http.Request) (map[string][]string, []byte, error) {
    var reader io.ReadCloser
    var err error

    // Check that the server actual sent compressed data
    if r.Header.Get("Content-Encoding") == "gzip" {
        reader, err = gzip.NewReader(r.Body)
        if err != nil {
            return r.Header, nil, err
        }
        delete(r.Header, "Content-Encoding")
        delete(r.Header, "Content-Length")
        defer reader.Close()
    } else {
        reader = r.Body
    }
    defer r.Body.Close()

    data, err := ioutil.ReadAll(reader)
    if err != nil {
        return r.Header, nil, err
    }

    return r.Header, data, nil
}

func (api *API) NewRequest(r *http.Request, url string, data io.Reader) ([]byte, map[string][]string, int, error) {
    req, err := http.NewRequest(r.Method, url, data)
    if err != nil {
        return nil, nil, 400, err
    }

    req.Header = r.Header
    req.URL.RawQuery = r.URL.RawQuery

    resp, err := api.Client.Do(req)
    if err != nil {
        return nil, nil, 503, err
    }
    defer resp.Body.Close()

    body, err := io.ReadAll(resp.Body)
    if err != nil {
        return nil, nil, 400, err
    }

    return body, resp.Header, resp.StatusCode, nil
}

func (api *API) HealthCheck(w http.ResponseWriter, r *http.Request){
    w.Header().Set("Content-Type", "text/plain; charset=utf-8")

    for _, urlMap := range api.Upstream.URLMap {
        for _, urlPrefix := range urlMap.URLPrefix {
            if len(urlPrefix.Health) == 0 {
                w.WriteHeader(200)
                w.Write([]byte("OK"))
                return
            }
        }
    }

    w.WriteHeader(503)
    return
}

func (api *API) ReverseProxy(w http.ResponseWriter, r *http.Request) {
    username, password, auth := r.BasicAuth()

    // Get request body
    header, data, err := readData(r)
    if err != nil {
        log.Printf("[error] %v - %s", err, r.URL.Path)
        requestTotal.With(prometheus.Labels{"listen_addr": api.Upstream.ListenAddr, "user": username, "code": "400"}).Inc()
        w.WriteHeader(400)
        return
    }
    r.Header = header

    // Get a limit for an object
    key := getObject(r, api.Upstream.ObjectHeader)
    size := float64(len(data))
    
    // Checking the size limit
    if api.Upstream.SizeLimit > 0 {
        all := api.Objects.Get("all")
        if all.Avg > api.Upstream.SizeLimit {
            item := api.Objects.Get(key)
            count := float64(len(api.Objects.items) - 1)
            if count > 0 && item.Avg > api.Upstream.SizeLimit / count {
                sizeBytesDropped.With(prometheus.Labels{"listen_addr": api.Upstream.ListenAddr, "url_path": r.URL.Path, "object": key}).Add(size)
                requestTotal.With(prometheus.Labels{"listen_addr": api.Upstream.ListenAddr, "user": username, "code": "413"}).Inc()
                if api.Upstream.ErrorCode != 0 {
                    w.WriteHeader(api.Upstream.ErrorCode)
                    return
                }
                w.WriteHeader(413)
                return
            }
        }
    }

    api.Objects.Set(key, r.URL.Path, size)
    sizeBytesTotal.With(prometheus.Labels{"listen_addr": api.Upstream.ListenAddr, "url_path": r.URL.Path, "object": "all"}).Add(size)

    // Path matching check
    for _, mapPath := range api.Upstream.MapPaths {

        if mapPath.RE.Match([]byte(r.URL.Path)) {

            if len(api.Upstream.URLMap[mapPath.Index].Users) > 0 {
                if !auth {
                    w.WriteHeader(401)
                    requestTotal.With(prometheus.Labels{"listen_addr": api.Upstream.ListenAddr, "user": username, "code": "401"}).Inc()
                    return
                }
                mPassword, ok := api.Upstream.URLMap[mapPath.Index].MapUsers[username]
                if !ok {
                    requestTotal.With(prometheus.Labels{"listen_addr": api.Upstream.ListenAddr, "user": username, "code": "403"}).Inc()
                    w.WriteHeader(403)
                    return
                }
                if getStringHash(password) != mPassword {
                    requestTotal.With(prometheus.Labels{"listen_addr": api.Upstream.ListenAddr, "user": username, "code": "403"}).Inc()
                    w.WriteHeader(403)
                    return
                }
            }

            if urlPrefix := getPrefixURL(api.Upstream.URLMap[mapPath.Index].URLPrefix); urlPrefix != nil {

                if api.Upstream.URLMap[mapPath.Index].RequestsLimit > 0 && len(urlPrefix.Requests) > api.Upstream.URLMap[mapPath.Index].RequestsLimit {
                    requestTotal.With(prometheus.Labels{"listen_addr": api.Upstream.ListenAddr, "user": username, "code": strconv.Itoa(429)}).Inc()
                    
                    if api.Upstream.URLMap[mapPath.Index].ErrorCode != 0 {
                        w.WriteHeader(api.Upstream.URLMap[mapPath.Index].ErrorCode)
                        return
                    }
                    w.WriteHeader(429)
                    return
                }

                if len(urlPrefix.Requests) < 1000000 {
                    urlPrefix.Requests <- 1
                }

                body, header, code, err := api.NewRequest(r, urlPrefix.URL+r.URL.Path, bytes.NewReader(data))
                if err != nil {
                    if len(urlPrefix.Requests) > 0 {
                        <- urlPrefix.Requests
                    }

                    log.Printf("[error] %v - %s", err, r.URL.Path)
                    requestTotal.With(prometheus.Labels{"listen_addr": api.Upstream.ListenAddr, "user": username, "code": strconv.Itoa(code)}).Inc()
                    
                    if api.Upstream.URLMap[mapPath.Index].ErrorCode != 0 {
                        w.WriteHeader(api.Upstream.URLMap[mapPath.Index].ErrorCode)
                        return
                    }
                    w.WriteHeader(code)
                    return
                }

                if len(urlPrefix.Requests) > 0 {
                    <- urlPrefix.Requests
                }

                requestTotal.With(prometheus.Labels{"listen_addr": api.Upstream.ListenAddr, "user": username, "code": strconv.Itoa(code)}).Inc()
                if code >= 400 && code < 500 {
                    log.Printf("[warn] %v", string(body))
                }

                // Set header values
                for key, val := range header {
                    for k, v := range val {
                        if k == 0 { 
                            w.Header().Set(key, v) 
                        } else { 
                            w.Header().Add(key, v) 
                        }
                    }
                }
                w.WriteHeader(code)
                w.Write(body)
                return
            }

            requestTotal.With(prometheus.Labels{"listen_addr": api.Upstream.ListenAddr, "user": username, "code": "502"}).Inc()
            w.WriteHeader(502)
            return
        }
    }

    requestTotal.With(prometheus.Labels{"listen_addr": api.Upstream.ListenAddr, "user": username, "code": "404"}).Inc()
    w.WriteHeader(404)
}

func main() {

    // Command-line flag parsing
    lsAddress      := flag.String("listen.client-address", "127.0.0.1:8426", "listen address")
    cfFile         := flag.String("config.file", "config/mtproxy.yml", "config file")
    encryptPass    := flag.String("encrypt", "", "encrypt password")
    debug          := flag.Bool("debug", false, "debug mode")
    flag.Parse()

    if *encryptPass != "" {
        log.Printf("[pass] %s", getStringHash(*encryptPass))
        return
    }

    // Loading configuration file
    cfg, err := config.NewConfig(*cfFile)
    if err != nil {
        log.Fatalf("[error] %v", err)
    }

    for _, stream := range cfg.Upstreams {
        // Creating api
        api, err := NewAPI(cfg.HttpClient, stream, *debug)
        if err != nil {
            log.Fatalf("[error] %v", err)
        }

        mux := http.NewServeMux()
        mux.HandleFunc("/health", api.HealthCheck)
        mux.HandleFunc("/", api.ReverseProxy)

        for _, urlMap := range stream.URLMap {
            for _, urlPrefix := range urlMap.URLPrefix {
                if urlMap.HealthCheck != "" {
                    go func(urlPrefix *config.URLPrefix){
                        for{
                            r := &http.Request{
                                Method: "GET",
                                URL: &url.URL{RawQuery: ""},
                            }
                            _, _, code, err := api.NewRequest(r, urlPrefix.URL+urlMap.HealthCheck, nil)
                            if err != nil || code >= 300 {
                                if len(urlPrefix.Health) < 5 {
                                    urlPrefix.Health <- 1
                                }
                                log.Printf("[warn] \"GET %v\" %v", urlPrefix.URL+urlMap.HealthCheck, code)
                            } else {
                                if len(urlPrefix.Health) > 0 {
                                    <- urlPrefix.Health
                                }
                            }
                            healthCheckFailed.With(prometheus.Labels{"target_url": urlPrefix.URL+urlMap.HealthCheck}).Set(float64(len(urlPrefix.Health)))
                            upstreamRequests.With(prometheus.Labels{"target_url": urlPrefix.URL}).Set(float64(len(urlPrefix.Requests)))
                            time.Sleep(1 * time.Second)
                        }
                    }(urlPrefix)
                }
            }
        }

        go func(stream *config.Upstream) {
            log.Printf("[info] upstream address: %v", stream.ListenAddr)
            if stream.CertFile != "" && stream.CertKey != "" {
                if err := http.ListenAndServeTLS(stream.ListenAddr, stream.CertFile, stream.CertKey, mux); err != nil {
                    log.Fatalf("[error] %v", err)
                }
            } else {
                if err := http.ListenAndServe(stream.ListenAddr, mux); err != nil {
                    log.Fatalf("[error] %v", err)
                }
            }
        }(stream)
    }

    go func(){
        prometheus.MustRegister(requestTotal)
        prometheus.MustRegister(sizeBytesTotal)
        prometheus.MustRegister(sizeBytesDropped)
        prometheus.MustRegister(sizeBytesBucket)
        prometheus.MustRegister(healthCheckFailed)
        prometheus.MustRegister(upstreamRequests)

        mux := http.NewServeMux()
        mux.Handle("/metrics", promhttp.Handler())
        if err := http.ListenAndServe(*lsAddress, mux); err != nil {
            log.Fatalf("[error] %v", err)
        }
    }()

    log.Print("[info] mtproxy started")

    // Program signal processing
    c := make(chan os.Signal, 1)
    signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
    for {
        <-c
        log.Print("[info] mtproxy stopped")
        os.Exit(0)
    }
}