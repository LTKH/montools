package db

import (
    "time"
    "errors"
    "github.com/ltkh/montools/internal/config/mtprom"
    "github.com/ltkh/montools/internal/db/clickhouse"
    //"github.com/ltkh/montools/internal/db/sqlite3"
    //"github.com/ltkh/montools/internal/db/redis"
)

type Client interface {
    Close() error
    
    Labels(start, end time.Time) ([]string, error)
    LabelValues(name string, start, end time.Time) ([]string, error)
    Series(match string, start, end time.Time) ([]map[string]string, error)
    Query(query string, limit int, time time.Time, timeout time.Duration) ([]config.Result, error)
    QueryRange(query string, limit int, start, end time.Time, step time.Duration) ([]config.Result, error)

}

func NewClient(config *config.Source, debug bool) (Client, error) {
    switch config.Type {
        case "clickhouse":
            return clickhouse.NewClient(config, debug)
    }
    return nil, errors.New("invalid client")
}