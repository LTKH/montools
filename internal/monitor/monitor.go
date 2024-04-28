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
        []string{"listen_addr"},
    )
    ProxyTotal = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: "mtproxy_http_proxy",
            Name:      "total",
            Help:      "",
        },
        []string{"url_prefix"},
    )
    HealthCheckFailed = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Namespace: "mtproxy_health_check",
            Name:      "failed",
            Help:      "",
        },
        []string{"url_prefix"},
    )
)

func New(listen string){
    prometheus.MustRegister(RequestTotal)
	prometheus.MustRegister(ProxyTotal)
    prometheus.MustRegister(HealthCheckFailed)

    http.Handle("/metrics", promhttp.Handler())
    go http.ListenAndServe(listen, nil)
}