package main

import (
    "io"
    //"fmt"
    "net"
    "net/url"
    "net/rpc"
    "net/http"
    "net/http/httputil"
    "log"
    "crypto/sha1"
    "time"
    "strconv"
    "sync"
    "bytes"
    //"io/ioutil"
    "compress/gzip"
    "encoding/hex"
    "encoding/json"
    "github.com/ltkh/montools/internal/monitor"
    "github.com/prometheus/client_golang/prometheus"
)

var (
    connections = make(map[string]chan int)
)

type API struct {
    Upstream     *Upstream
    Peers        *Peers
    Limits       *Limits
    Debug        bool
}

type RPC struct {
    Debug        bool
}

type Peers struct {
    sync.RWMutex
    items        map[string]*rpc.Client
}

type Limits struct {
    sync.RWMutex
    items        map[string]float64
}

type responseWriter struct {
    responseWriter http.ResponseWriter
    StatusCode     int
}

func extendResponseWriter(w http.ResponseWriter) *responseWriter {
    return &responseWriter{w, 0}
}

func (w *responseWriter) Write(b []byte) (int, error) {
    return w.responseWriter.Write(b)
}

func (w *responseWriter) Header() http.Header {
    return w.responseWriter.Header()
}

func (w *responseWriter) WriteHeader(statusCode int) {
    // receive status code from this method
    w.StatusCode = statusCode
    w.responseWriter.WriteHeader(statusCode)

    return
}

func (w *responseWriter) Done() {
    // if the `w.WriteHeader` wasn't called, set status code to 200 OK
    if w.StatusCode == 0 {
        w.StatusCode = http.StatusOK
    }

    return
}

func NewAPI(upstream *Upstream, peers []string, debug bool) (*API, error) {
    api := &API{ 
        Upstream: upstream,
        Peers: &Peers{items: make(map[string]*rpc.Client)},
        Limits: &Limits{items: make(map[string]float64)},
        Debug: debug,
    }
    for _, id := range peers {
        connections[id] = make(chan int, 1)
    }
    return api, nil
}

func (api *API) ApiPeers(peers []string) {
    for _, id := range peers {

        conn, err := net.DialTimeout("tcp", id, 2 * time.Second)
        if err == nil {
            if _, ok := api.Peers.items[id]; !ok {
                api.Peers.Lock()
                api.Peers.items[id] = rpc.NewClient(conn)
                api.Peers.Unlock()
                log.Printf("[info] successful connection: %v", id)
                continue
            }
            if len(connections[id]) > 0 {
                <- connections[id]
                api.Peers.Lock()
                api.Peers.items[id] = rpc.NewClient(conn)
                api.Peers.Unlock()
                log.Printf("[info] connection restored: %v", id)
                continue
            }
        } else {
            log.Printf("[error] %v", err)
        }
        
    }
}

func NewRPC() (*RPC, error) {
    return &RPC{ }, nil
}

func rewriteBody(resp *http.Response) (err error) {
    //username, _, _ := resp.Request.BasicAuth()
    //upstream := resp.Header.Get("X-Upstream-Header")
    //monitor.ProxyTotal.With(prometheus.Labels{"target_url": resp.Request.URL.Path, "user": username, "code": strconv.Itoa(resp.StatusCode)}).Inc()
    //monitor.RequestTotal.With(prometheus.Labels{"listen_addr": upstream, "user": username, "code": strconv.Itoa(resp.StatusCode)}).Inc()

    return nil
}

func getReverseProxy(URLPrefix string) *httputil.ReverseProxy {
    return &httputil.ReverseProxy{
        Director: func(r *http.Request) {
            target, _ := url.Parse(URLPrefix)
            target.Path = URLPrefix+r.URL.Path
            target.RawQuery = r.URL.RawQuery
            r.URL = target
        },
        Transport: func() *http.Transport {
            tr := http.DefaultTransport.(*http.Transport).Clone()
            tr.DisableCompression = true
            tr.ForceAttemptHTTP2 = false
            tr.MaxIdleConnsPerHost = 100
            if tr.MaxIdleConns != 0 && tr.MaxIdleConns < tr.MaxIdleConnsPerHost {
                tr.MaxIdleConns = tr.MaxIdleConnsPerHost
            }
            return tr
        }(),
        FlushInterval: time.Second,
        ErrorLog: nil,
        //ModifyResponse: rewriteBody,
    }
}

func getStringHash(text string) string {
    h := sha1.New()
    io.WriteString(h, text)
    return hex.EncodeToString(h.Sum(nil))
}

func getPrefixURL(prefix []*URLPrefix) *URLPrefix {
    var urlPrefix *URLPrefix

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

func getLenBody(r *http.Request) (float64, error) {
    var reader io.ReadCloser
    var err error

    // Check that the server actual sent compressed data
    switch r.Header.Get("Content-Encoding") {
        case "gzip":
            reader, err = gzip.NewReader(r.Body)
            if err != nil {
                return float64(r.ContentLength), err
            }
            defer reader.Close()
        default:
            reader = r.Body
    }
    defer r.Body.Close()

    var respBody bytes.Buffer
    _, err = io.Copy(&respBody, reader)
    if err != nil {
        return float64(r.ContentLength), err
    }

    return float64(respBody.Len()), nil
}

func (api *API) reverseProxy(w http.ResponseWriter, r *http.Request){
    username, password, auth := r.BasicAuth()

    // Get a limit for an object
    object := r.Header.Get(api.Upstream.ObjectHeader)
    size, err := getLenBody(r)
    if err != nil {
        log.Printf("[error] %v - %s", err, r.URL.Path)
    }

    api.Limits.Lock()
    defer api.Limits.Unlock()

    if val, ok := api.Limits.items[object]; ok {
        size = (size + val)/ 2
    }
    api.Limits.items[object] = size
    monitor.SizeBytesBucket.With(prometheus.Labels{"listen_addr": api.Upstream.ListenAddr, "object": object}).Set(size)

    // Check size limit
    if api.Upstream.SizeLimit > 0 {
        if size > api.Upstream.SizeLimit / float64(len(api.Limits.items)) {
            log.Printf("[error] payload too large (%v) - %v", object, r.URL.Path)
            monitor.RequestTotal.With(prometheus.Labels{"listen_addr": api.Upstream.ListenAddr, "user": username, "code": "413"}).Inc()
            w.WriteHeader(413)
            return
        }
    }

    for _, mapPath := range api.Upstream.MapPaths {
        if mapPath.RE.Match([]byte(r.URL.Path)) {
        
            if api.Debug {
                log.Printf("[debug] proxy match found Path - %v", r.URL.Path)
            }

            if len(api.Upstream.URLMap[mapPath.index].Users) > 0 {
                if !auth {
                    w.WriteHeader(401)
                    monitor.RequestTotal.With(prometheus.Labels{"listen_addr": api.Upstream.ListenAddr, "user": username, "code": "401"}).Inc()
                    return
                }
                mPassword, ok := api.Upstream.URLMap[mapPath.index].MapUsers[username]
                if !ok {
                    monitor.RequestTotal.With(prometheus.Labels{"listen_addr": api.Upstream.ListenAddr, "user": username, "code": "403"}).Inc()
                    w.WriteHeader(403)
                    return
                }
                if getStringHash(password) != mPassword {
                    monitor.RequestTotal.With(prometheus.Labels{"listen_addr": api.Upstream.ListenAddr, "user": username, "code": "403"}).Inc()
                    w.WriteHeader(403)
                    return
                }
            }

            if urlPrefix := getPrefixURL(api.Upstream.URLMap[mapPath.index].URLPrefix); urlPrefix != nil {
                
                if api.Debug {
                    log.Printf("[debug] proxy match found URL - %v", urlPrefix.URL)
                }

                if len(urlPrefix.Requests) < 1000000 {
                    urlPrefix.Requests <- 1
                }
                
                ew := extendResponseWriter(w)
                getReverseProxy(urlPrefix.URL).ServeHTTP(ew, r)
                ew.Done()
                
                if len(urlPrefix.Requests) > 0 {
                    <- urlPrefix.Requests
                }
                
                monitor.RequestTotal.With(prometheus.Labels{"listen_addr": api.Upstream.ListenAddr, "user": username, "code": strconv.Itoa(ew.StatusCode)}).Inc()
                return
            }

            monitor.RequestTotal.With(prometheus.Labels{"listen_addr": api.Upstream.ListenAddr, "user": username, "code": "502"}).Inc()
            w.WriteHeader(502)
            return
        }
    }

    monitor.RequestTotal.With(prometheus.Labels{"listen_addr": api.Upstream.ListenAddr, "user": username, "code": "404"}).Inc()
    w.WriteHeader(404)
    return
}

func (api *API) healthCheck(w http.ResponseWriter, r *http.Request){
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

    w.WriteHeader(502)
    return
}

func (api *API) getLimits(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")

    api.Peers.RLock()
    defer api.Peers.RUnlock()

    var wg sync.WaitGroup

    for id, client := range api.Peers.items {

        wg.Add(1)

        go func(id string, client *rpc.Client, wg *sync.WaitGroup) {
            defer wg.Done()

            var items map[string]float64
            err := client.Call("RPC.GetLimits", api.Upstream.ListenAddr, &items)
            if err != nil {
                log.Printf("[error] %v - %s%s", err, id, r.URL.Path)
                if len(connections[id]) < 1 {
                    connections[id] <- 1
                }
                return
            }

            api.Limits.Lock()
            defer api.Limits.Unlock()

            for object, value := range items{
                api.Limits.items[object] = value
            }

        }(id, client, &wg)
        
    }

    wg.Wait()

    api.Limits.RLock()
    defer api.Limits.RUnlock()

    data, err := json.Marshal(api.Limits.items)
    if err != nil {
        log.Printf("[error] %v - %s", err, r.URL.Path)
        return
    }

    w.WriteHeader(200)
    w.Write(data)
    return
}

func (rpc *RPC) GetLimits(address string, items *map[string]float64) error {
    test, _ := monitor.SizeBytesBucket.GetMetricWith(prometheus.Labels{"listen_addr": address, "object": ""})
    //for _, data := range test {
        log.Printf("[debug] %v", test)
    //}

    //var it map[string]float64
    //it["test2"] = 34.56

    (*items)["test2"] = 34.67
    
    return nil
}
