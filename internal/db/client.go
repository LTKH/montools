package db

import (
    "errors"
    "github.com/ltkh/montools/internal/config"
    "github.com/ltkh/montools/internal/db/chouse"
    //"github.com/ltkh/montools/internal/db/sqlite3"
    //"github.com/ltkh/montools/internal/db/redis"
)

type Client interface {
    //CreateTables() error
    Close() error

    ShowTables() ([]string, error)

    //SaveStatus(records []config.SockTable) error
    //SaveNetstat(records []config.SockTable) error

    //LoadRecords(args config.RecArgs) ([]config.SockTable, error)
    //SaveRecords(records []config.SockTable) error
    //DelRecords(ids []string) error

    //LoadExceptions(args config.ExpArgs) ([]config.Exception, error)
    //SaveExceptions(records []config.Exception) error
    //DelExceptions(ids []string) error
    
    //Healthy() error
    //LoadUser(login string) (cache.User, error)
    //SaveUser(user cache.User) error
    //LoadUsers() ([]cache.User, error)
    //LoadAlerts() ([]cache.Alert, error)
    //SaveAlerts(alerts map[string]cache.Alert) error
    //AddAlert(alert cache.Alert) error
    //UpdAlert(alert cache.Alert) error
    //DeleteOldAlerts() (int64, error)
}

func NewClient(config *config.Source) (Client, error) {
    switch config.Type {
        case "clickhouse":
            return chouse.NewClient(config)
    }
    return nil, errors.New("invalid client")
}