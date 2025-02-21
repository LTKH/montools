package main

import (
    "log"
    //"fmt"
    "flag"
    //"time"
    "net/http"
    "os"
    "os/signal"
    "syscall"
    "github.com/gorilla/mux"
    "github.com/ltkh/montools/internal/config/mtprom"
    "github.com/ltkh/montools/internal/api/logql"
)

func main() {
    // Command-line flag parsing
    cfFile := flag.String("config.file", "config/mtprom.yml", "config file")
    flag.Parse()

    // Loading configuration file
    cfg, err := config.New(*cfFile)
    if err != nil {
        log.Fatalf("[error] %v", err)
    }

    for _, upstream := range cfg.Upstreams {
        if upstream.Type != "logql" {
            continue
        }

        api, err := logql.New(upstream)
        if err != nil {
            log.Fatalf("[error] %v", err)
        }

        rtr := mux.NewRouter()
        rtr.HandleFunc("/loki/api/v1/labels", api.ApiLabels)
        rtr.HandleFunc("/loki/api/v1/label/{name:[^/]+}/values", api.ApiLabelValues)
        rtr.HandleFunc("/loki/api/v1/query", api.ApiQuery)
        rtr.HandleFunc("/loki/api/v1/query_range", api.ApiQueryRange)
        rtr.HandleFunc("/loki/api/v1/series", api.ApiSeries)
        rtr.HandleFunc("/loki/api/v1/metadata", api.ApiMetadata)
        rtr.HandleFunc("/loki/api/v1/index/stats", api.ApiIndexStats)
        http.Handle("/", rtr)

        go func(){
            log.Printf("[info] upstream address: %v", upstream.ListenAddr)
            err = http.ListenAndServe(upstream.ListenAddr, nil)
            if err != nil {
                log.Fatalf("[error] %v", err)
            }
        }()
    }

    log.Print("[info] mtlogql started")

    // Program signal processing
    c := make(chan os.Signal, 1)
    signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
    for {
        <-c
        log.Print("[info] mtlogql stopped")
        os.Exit(0)
    }

}