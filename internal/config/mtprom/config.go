package config

import (
    //"io"
    //"fmt"
    //"path"
    //"errors"
    //"regexp"
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
    Upstreams              []*Upstream             `yaml:"upstreams"`
}

type Upstream struct {
    ListenAddr             string                  `yaml:"listen_addr"`
    Source                 *Source                 `yaml:"source"`
    Debug                  bool                    `yaml:"debug"`
    Type                   string                  `yaml:"type"`
}

type Source struct {
    Type                   string                  `yaml:"type"`
    Addr                   []string                `yaml:"addr"`
    Database               string                  `yaml:"database"`
    Username               string                  `yaml:"username"`
    Password               string                  `yaml:"password"`
    DialTimeout            time.Duration           `yaml:"dial_timeout"`
    MaxExecutionTime       int                     `yaml:"max_execution_time"`
    DBaseNames             []string                `yaml:"dbase_names"`
    TableNames             []string                `yaml:"table_names"`
    Tables                 []*Table                `yaml:"tables"`
}

type Table struct {
    Name                   string                  `yaml:"name"`
    LabelTypes             []string                `yaml:"label_types"`
    LabelNames             []string                `yaml:"label_names"`
    ValueTypes             []string                `yaml:"value_types"`
    ValueNames             []string                `yaml:"value_names"`
    TimesTypes             []string                `yaml:"times_types"`
    TimesNames             []string                `yaml:"times_names"`
}

type Result struct {
    Metric                 map[string]string       `json:"metric"`
    Value                  []interface{}           `json:"value,omitempty"`
    Values                 []interface{}           `json:"values,omitempty"`
}

func New(filename string) (*Config, error) {

    cfg := &Config{}

    content, err := ioutil.ReadFile(filename)
    if err != nil {
       return cfg, err
    }

    if err := yaml.UnmarshalStrict(content, cfg); err != nil {
        return cfg, err
    }
    
    return cfg, nil
}
