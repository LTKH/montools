package main

import (
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
    lsAddress      := flag.String("httpListenAddr", "127.0.0.1:8426", "listen address")
    cfFile         := flag.String("configFile", "config/mtproxy.yml", "config file")
    lgFile         := flag.String("loggerFile", "", "log file")
    logMaxSize     := flag.Int("loggerMaxSize", 1, "log max size") 
    logMaxBackups  := flag.Int("loggerMaxBackups", 3, "log max backups")
    logMaxAge      := flag.Int("loggerMaxAge", 10, "log max age")
    logCompress    := flag.Bool("loggerCompress", true, "log compress")
    encryptPass    := flag.String("encrypt", "", "encrypt password")
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

    //opening monitoring port
    monitor.New(*lsAddress)

    for _, u := range cfg.Upstreams { 
        // Creating api
        proxy, err := apiNew(u)
        if err != nil {
            log.Fatalf("[error] %v", err)
        }

        mux := http.NewServeMux()
        mux.HandleFunc("/", proxy.reverseProxy)
        go func(u *Upstream) {
            log.Printf("[info] %v", u.ListenAddr)
            err := http.ListenAndServe(u.ListenAddr, mux)
            if err != nil {
                log.Fatalf("[error] %v", err)
            }
        }(u)
    }

    log.Print("[info] mtproxy started")

    // Daemon mode
    for {

        time.Sleep(60 * time.Second)

    }

}