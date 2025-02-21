package db

import (
    "time"
    "errors"
    "github.com/ltkh/montools/internal/config/mtprom"
    "github.com/ltkh/montools/internal/db/oracle"
    "github.com/ltkh/montools/internal/db/clickhouse"
    //"github.com/ltkh/montools/internal/db/sqlite3"
    //"github.com/ltkh/montools/internal/db/redis"
)

type Client interface {
    Close() error
    
    Labels(start, end time.Time) ([]string, error)
    LabelValues(name string, start, end time.Time) ([]string, error)
    Series(match string, start, end time.Time) ([]map[string]string, error)
    Query(query string, time time.Time, timeout time.Duration, limit int) (config.ResultType, error)
    QueryRange(query string, start, end time.Time, step time.Duration, limit int) (config.ResultType, error)
    LokiQuery(query string, time time.Time, timeout time.Duration, limit int) (config.ResultType, error)
    LokiQueryRange(query string, start, end time.Time, step time.Duration, limit int) (config.ResultType, error)
}

func NewClient(config *config.Source, debug bool) (Client, error) {
    switch config.Type {
        case "oracle":
            return oracle.NewClient(config, debug)
        case "clickhouse":
            return clickhouse.NewClient(config, debug)
    }
    return nil, errors.New("invalid client")
}