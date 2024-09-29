package monitor

import (
    "net/http"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
    RequestTotal = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name:      "mtproxy_http_request_total",
            Help:      "",
        },
        []string{"listen_addr","user","code"},
    )
    SizeBytesBucket = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name:      "mtproxy_http_size_bytes_avg",
            Help:      "",
        },
        []string{"listen_addr","object"},
    )
    ProxyTotal = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name:      "mtproxy_http_proxy_total",
            Help:      "",
        },
        []string{"target_url","user","code"},
    )
    HealthCheckFailed = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name:      "mtproxy_health_check_failed",
            Help:      "",
        },
        []string{"target_url"},
    )
)

func New(listen string){
    prometheus.MustRegister(RequestTotal)
    prometheus.MustRegister(SizeBytesBucket)
    prometheus.MustRegister(ProxyTotal)
    prometheus.MustRegister(HealthCheckFailed)

    http.Handle("/metrics", promhttp.Handler())
    go http.ListenAndServe(listen, nil)
}