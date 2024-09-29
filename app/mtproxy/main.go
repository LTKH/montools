package main

import (
    "net"
    "net/rpc"
    "net/http"
    "time"
    "log"
    //"fmt"
    "os"
    "os/signal"
    "syscall"
    "flag"
    //"strings"
    "github.com/ltkh/montools/internal/monitor"
    "gopkg.in/natefinch/lumberjack.v2"
)

func main() {

    // Command-line flag parsing
    lsAddress      := flag.String("listen.client-address", "127.0.0.1:8426", "listen address")
    prAddress      := flag.String("listen.peer-address", "127.0.0.1:8427", "listen peer address")
    //initClucter    := flag.String("initial-cluster", "", "initial cluster nodes")
    cfFile         := flag.String("config.file", "config/mtproxy.yml", "config file")
    lgFile         := flag.String("log.file", "", "log file")
    logMaxSize     := flag.Int("log.max-size", 1, "log max size") 
    logMaxBackups  := flag.Int("log.max-backups", 3, "log max backups")
    logMaxAge      := flag.Int("log.max-age", 10, "log max age")
    logCompress    := flag.Bool("log.compress", true, "log compress")
    encryptPass    := flag.String("encrypt", "", "encrypt password")
    debug          := flag.Bool("debug", false, "debug mode")
    flag.Parse()

    if *encryptPass != "" {
        log.Printf("[pass] %s", getStringHash(*encryptPass))
        return
    }

    // Logging settings
    if *lgFile != "" {
        log.SetOutput(&lumberjack.Logger{
            Filename:   *lgFile,
            MaxSize:    *logMaxSize,    // megabytes after which new file is created
            MaxBackups: *logMaxBackups, // number of backups
            MaxAge:     *logMaxAge,     // days
            Compress:   *logCompress,   // using gzip
        })
    }

    // Initial cluster nodes
    peers := []string{}
    /*
    if *initClucter != "" {
        peers = strings.Split(*initClucter, ",")
    }
    if len(peers) == 0 && os.Getenv("MTPROXY_INITIAL_CLUSTER") != "" {
        peers = strings.Split(os.Getenv("MTPROXY_INITIAL_CLUSTER"), ",")
    }
    if len(peers) == 0 && *prAddress != "" {
        peers = append(peers, *prAddress)
    }
    */

    // Program signal processing
    c := make(chan os.Signal, 1)
    signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
    go func(){
        for {
            <-c
            log.Print("[info] mtproxy stopped")
            os.Exit(0)
        }
    }()

    // Loading configuration file
    cfg, err := configNew(*cfFile)
    if err != nil {
        log.Fatalf("[error] %v", err)
    }

    // Creating RPC
    rpcV1, err := NewRPC()
    if err != nil {
        log.Fatalf("[error] %v", err)
    }

    // TCP Listen
    go func(){
        inbound, err := net.Listen("tcp", *prAddress)
        if err != nil {
            log.Fatalf("[error] %v", err)
        }
        rpc.Register(rpcV1)
        rpc.Accept(inbound)
    }()

    //opening monitoring port
    monitor.New(*lsAddress)

    for _, u := range cfg.Upstreams { 
        // Creating api
        apiV1, err := NewAPI(u, peers, *debug)
        if err != nil {
            log.Fatalf("[error] %v", err)
        }

        mux := http.NewServeMux()
        mux.HandleFunc("/health", apiV1.healthCheck)
        //mux.HandleFunc("/limits", apiV1.getLimits)
        mux.HandleFunc("/", apiV1.reverseProxy)

        go func(u *Upstream) {
            log.Printf("[info] upstream address: %v", u.ListenAddr)
            if u.CertFile != "" && u.CertKey != "" {
                if err := http.ListenAndServeTLS(u.ListenAddr, u.CertFile, u.CertKey, mux); err != nil {
                    log.Fatalf("[error] %v", err)
                }
            } else {
                if err := http.ListenAndServe(u.ListenAddr, mux); err != nil {
                    log.Fatalf("[error] %v", err)
                }
            }
        }(u)

        go func(peers []string) {
            for {
                apiV1.ApiPeers(peers)
                time.Sleep(10 * time.Second)
            }
        }(peers)
    }

    log.Print("[info] mtproxy started")

    // Daemon mode
    for {

        time.Sleep(60 * time.Second)

    }

}