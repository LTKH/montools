package config

import (
    //"fmt"
    //"path"
    "time"
    //"errors"
    //"regexp"
    //"strings"
    //"strconv"
    //"net/url"
    "io/ioutil"
    "gopkg.in/yaml.v2"
)

type MTSources struct {
    Upstreams        []*Upstream             `yaml:"upstreams"`
}

type Upstream struct {
    ListenAddr       string                  `yaml:"listen_addr"`
	Type             string                  `yaml:"type"`
    Source           *Source                 `yaml:"source"`
}

type Source struct {
	Type             string                  `yaml:"type"`
	Addr             []string                `yaml:"addr"`
	Database         string                  `yaml:"database"`
	Username         string                  `yaml:"username"`
	Password         string                  `yaml:"password"`
	DialTimeout      time.Duration           `yaml:"dial_timeout"`
	MaxExecutionTime int                     `yaml:"max_execution_time"`
    Fields           map[string]string       `yaml:"fields"`
}

func NewMTSources(filename string) (*MTSources, error) {

    cfg := &MTSources{}

    content, err := ioutil.ReadFile(filename)
    if err != nil {
       return cfg, err
    }

    if err := yaml.UnmarshalStrict(content, cfg); err != nil {
        return cfg, err
    }
    
    return cfg, nil
}