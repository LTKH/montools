package config

import (
    //"io"
    "fmt"
    //"path"
    //"errors"
    "regexp"
    //"strings"
    //"strconv"
    //"net/url"
    "io/ioutil"
    "gopkg.in/yaml.v2"
    "time"
    //"net/http"
    //"log"
    //"github.com/ltkh/montools/internal/monitor"
    //"github.com/prometheus/client_golang/prometheus"
)

type Config struct {
    HttpClient             *HttpClient             `yaml:"http_client"`
    Upstreams              []*Upstream             `yaml:"upstreams"`
}

type HttpClient struct {
    Timeout                time.Duration           `yaml:"timeout"`
    HttpTransport          *HttpTransport          `yaml:"transport"`
}

type HttpTransport struct {
    DialContext            *DialContext            `yaml:"dial_context"`
    MaxIdleConnsPerHost    int                     `yaml:"max_idle_conns_per_host"`
    ExpectContinueTimeout  time.Duration           `yaml:"expect_continue_timeout"`
    TLSHandshakeTimeout    time.Duration           `yaml:"tls_handshake_timeout"`
    ResponseHeaderTimeout  time.Duration           `yaml:"response_header_timeout"`
}

type DialContext struct {
    Timeout                time.Duration           `yaml:"timeout"`
    KeepAlive              time.Duration           `yaml:"keep_alive"`
}

type Upstream struct {
    ListenAddr             string                  `yaml:"listen_addr"`
    ObjectHeader           string                  `yaml:"object_header"`
    SizeLimit              float64                 `yaml:"size_limit"`
    UpdateStat             time.Duration           `yaml:"update_stat"`
    ErrorCode              int                     `yaml:"error_code"`
    CertFile               string                  `yaml:"cert_file"`
    CertKey                string                  `yaml:"cert_key"`
    URLMap                 []*URLMap               `yaml:"url_map"`
    MapPaths               []SrcPath               `yaml:"-"`
}

// URLMap is a mapping from source paths to target urls.
type URLMap struct {
    SrcPaths               []string                `yaml:"src_paths"`
    URLPrefix              []*URLPrefix            `yaml:"url_prefix"`
    Users                  []*UserInfo             `yaml:"users"`
    MapUsers               map[string]string       `yaml:"-"`
    HealthCheck            string                  `yaml:"health_check"`
    ErrorCode              int                     `yaml:"error_code"`
    RequestsLimit          int                     `yaml:"requests_limit"`
}

// URLPrefix represents passed `url_prefix`
type URLPrefix struct {
    //Check                  bool
    Requests              chan int
    Health                chan int
    URL                   string
}

// SrcPath represents an src path
type SrcPath struct {
    sOriginal             string
    RE                    *regexp.Regexp
    Index                 int
}

// UserInfo is user information
type UserInfo struct {
    Username              string                  `yaml:"username"`
    Password              string                  `yaml:"password"`
}

// UnmarshalYAML unmarshals up from yaml.
func (up *URLPrefix) UnmarshalYAML(f func(interface{}) error) error {
    var s string
    if err := f(&s); err != nil {
        return err
    }
    //up.Check = true
    up.URL = s
    up.Requests = make(chan int, 1000000)
    up.Health = make(chan int, 5)
    return nil
}

func NewConfig(filename string) (*Config, error) {

    cfg := &Config{}

    content, err := ioutil.ReadFile(filename)
    if err != nil {
       return cfg, err
    }

    if err := yaml.UnmarshalStrict(content, cfg); err != nil {
        return cfg, err
    }

    for _, stream := range cfg.Upstreams {
        if stream.ObjectHeader == "" { 
            stream.ObjectHeader = "X-Custom-Object"
        }
        for i, urlMap := range stream.URLMap {
            for _, srcPaths := range urlMap.SrcPaths {
                var mp SrcPath
                mp.sOriginal = srcPaths
                mp.Index = i

                re, err := regexp.Compile("^(?:" + srcPaths + ")$")
                if err != nil {
                    return cfg, fmt.Errorf("cannot build regexp from %q: %w", srcPaths, err)
                }
                mp.RE = re

                stream.MapPaths = append(stream.MapPaths, mp)
            }
            mu := make(map[string]string)
            for _, user := range urlMap.Users {
                mu[user.Username] = user.Password
            }
            urlMap.MapUsers = mu
        }
    }
    
    return cfg, nil
}
