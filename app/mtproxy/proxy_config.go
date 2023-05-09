package main

import (
    "fmt"
    //"path"
    //"errors"
    "regexp"
    //"strings"
    //"strconv"
    //"net/url"
    "io/ioutil"
    "gopkg.in/yaml.v2"
)

type Config struct {
    Upstreams        []*Upstream             `yaml:"upstreams"`
}

type Upstream struct {
    ListenAddr       string                  `yaml:"listen_addr"`
    URLMap           []*URLMap               `yaml:"url_map"`
}

// URLMap is a mapping from source paths to target urls.
type URLMap struct {
    SrcPaths         []*SrcPath              `yaml:"src_paths"`
    URLPrefix        []*URLPrefix            `yaml:"url_prefix"`
    Users            []*UserInfo             `yaml:"users"`
    MapUsers         map[string]string       `yaml:"-"`
}

// URLPrefix represents passed `url_prefix`
type URLPrefix struct {
    Check            bool
    Requests         chan int
    URL              string
}

// SrcPath represents an src path
type SrcPath struct {
    sOriginal        string
    RE               *regexp.Regexp
}

// UserInfo is user information
type UserInfo struct {
    Username         string                  `yaml:"username"`
    Password         string                  `yaml:"password"`
}

// UnmarshalYAML unmarshals up from yaml.
func (up *URLPrefix) UnmarshalYAML(f func(interface{}) error) error {
    var s string
    if err := f(&s); err != nil {
        return err
    }
    up.Check = true
    up.URL = s
    return nil
}

// UnmarshalYAML implements yaml.Unmarshaler
func (sp *SrcPath) UnmarshalYAML(f func(interface{}) error) error {
    var s string
    if err := f(&s); err != nil {
        return err
    }
    sAnchored := "^(?:" + s + ")$"
    re, err := regexp.Compile(sAnchored)
    if err != nil {
        return fmt.Errorf("cannot build regexp from %q: %w", s, err)
    }
    sp.sOriginal = s
    sp.RE = re
    return nil
}

func configNew(filename string) (*Config, error) {

    cfg := &Config{}

    content, err := ioutil.ReadFile(filename)
    if err != nil {
       return cfg, err
    }

    if err := yaml.UnmarshalStrict(content, cfg); err != nil {
        return cfg, err
    }

    for _, stream := range cfg.Upstreams {
        for _, urlMap := range stream.URLMap {
            mu := make(map[string]string)
            for _, user := range urlMap.Users {
                mu[user.Username] = user.Password
            }
            urlMap.MapUsers = mu
            for _, urlPrefix := range urlMap.URLPrefix {
                urlPrefix.Requests = make(chan int, 1000000)
            }
        }
    }
    
    return cfg, nil
}
