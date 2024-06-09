package monitor

import (
    "net/http"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
    RequestTotal = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: "mtproxy_http_request",
            Name:      "total",
            Help:      "",
        },
        []string{"listen_addr","user"},
    )
    ProxyTotal = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: "mtproxy_http_proxy",
            Name:      "total",
            Help:      "",
        },
        []string{"target_url","user","code"},
    )
    HealthCheckFailed = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Namespace: "mtproxy_health_check",
            Name:      "failed",
            Help:      "",
        },
        []string{"target_url"},
    )
)

func New(listen string){
    prometheus.MustRegister(RequestTotal)
    prometheus.MustRegister(ProxyTotal)
    prometheus.MustRegister(HealthCheckFailed)

    http.Handle("/metrics", promhttp.Handler())
    go http.ListenAndServe(listen, nil)
}